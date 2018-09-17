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

use bcdb::{PageIterator, PageFile};
use page::Page;
use error::BCSError;
use types::Offset;

use std::sync::Mutex;
use std::collections::HashSet;

/// The buffer pool
pub struct LogFile {
    rw: Mutex<Box<PageFile>>,
    appended: HashSet<Offset>,
    pub tbl_len: u64
}

impl LogFile {
    pub fn new(rw: Box<PageFile>) -> LogFile {
        LogFile { rw: Mutex::new(rw), appended: HashSet::new(), tbl_len: 0 }
    }

    pub fn init (&mut self) -> Result<(), BCSError> {
        Ok(())
    }

    pub fn has_page(&self, offset: Offset) -> bool {
        self.appended.contains(&offset)
    }

    /// append a page if not yet logged in this batch. Returns false if the page was logged before.
    pub fn append_page (&mut self, page: Page) -> Result<bool, BCSError> {
        if self.appended.insert(page.offset) {
            self.rw.lock().unwrap().append_page(page)?;
            return Ok(true);
        }
        Ok(false)
    }

    /// empties the set of logged pages
    pub fn clear_cache(&mut self) {
        self.appended.clear();
    }

    pub fn page_iter (&self) -> PageIterator {
        PageIterator::new(self, 0)
    }
}

impl PageFile for LogFile {
    fn flush(&mut self) -> Result<(), BCSError> {
        Ok(self.rw.lock().unwrap().flush()?)
    }

    fn len(&mut self) -> Result<u64, BCSError> {
        let mut rw = self.rw.lock().unwrap();
        rw.len()
    }

    fn truncate(&mut self, len: u64) -> Result<(), BCSError> {
        let mut rw = self.rw.lock().unwrap();
        rw.truncate(len)
    }

    fn sync(&self) -> Result<(), BCSError> {
        let rw = self.rw.lock().unwrap();
        rw.sync()
    }

    fn read_page (&self, offset: Offset) -> Result<Page, BCSError> {
        let rw = self.rw.lock().unwrap();
        rw.read_page(offset)
    }

    fn append_page(&mut self, page: Page) -> Result<(), BCSError> {
        let mut rw = self.rw.lock().unwrap();
        rw.append_page(page)
    }

    fn write_page(&mut self, _: Page) -> Result<(), BCSError> {
        unimplemented!()
    }
}