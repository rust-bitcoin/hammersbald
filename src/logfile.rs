//
// Copyright 2018 Tamas Blummer
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
//!
//! # The log file
//! A synchronous append writer of a log file. It maintains its own set of page offsets
//! loged, so only the first pre-image will be stored within a batch. The batch should reset
//! this set.
//!

use bcdb::{DBFile, RW, PageIterator, PageFile};
use page::{Page, PAGE_SIZE};
use error::BCSError;
use types::Offset;

use std::io::{Read,Write,Seek,SeekFrom};
use std::sync::{Arc, Mutex};
use std::collections::HashSet;

/// The buffer pool
pub struct LogFile {
    rw: Mutex<Box<RW>>,
    appended: HashSet<Offset>
}

impl LogFile {
    pub fn new(rw: Box<RW>) -> LogFile {
        LogFile { rw: Mutex::new(rw), appended: HashSet::new() }
    }

    pub fn init (&mut self) -> Result<(), BCSError> {
        Ok(())
    }

    /// append a page if not yet logged in this batch. Returns false if the page was logged before.
    pub fn append_page (&mut self, page: Arc<Page>) -> Result<bool, BCSError> {
        if !self.appended.contains(&page.offset) {
            self.appended.insert(page.offset);
            self.rw.lock().unwrap().write(&page.finish())?;
            return Ok(true);
        }
        Ok(false)
    }

    /// empties the set of logged pages
    pub fn reset (&mut self) {
        self.appended.clear();
    }

    pub fn page_iter (&self) -> PageIterator {
        PageIterator::new(self, 0)
    }
}

impl DBFile for LogFile {
    fn flush(&mut self) -> Result<(), BCSError> {
        Ok(self.rw.lock().unwrap().flush()?)
    }

    fn sync(&mut self) -> Result<(), BCSError> {
        let rw = self.rw.lock().unwrap();
        rw.sync()
    }

    fn truncate(&mut self, offset: Offset) -> Result<(), BCSError> {
        let mut rw = self.rw.lock().unwrap();
        rw.truncate(offset.as_usize())
    }

    fn len(&mut self) -> Result<Offset, BCSError> {
        self.flush()?;
        let mut rw = self.rw.lock().unwrap();
        Offset::new(rw.len()?)
    }
}

impl PageFile for LogFile {
    fn read_page (&self, offset: Offset) -> Result<Arc<Page>, BCSError> {
        let mut buffer = [0u8; PAGE_SIZE];
        let mut rw = self.rw.lock().unwrap();
        rw.seek(SeekFrom::Start(offset.as_usize() as u64))?;
        rw.read(&mut buffer)?;
        let page = Arc::new(Page::from_buf(buffer));
        Ok(page)
    }
}