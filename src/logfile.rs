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
    rw: Box<PageFile>,
    appended: HashSet<Offset>,
    pub tbl_len: u64
}

impl LogFile {
    pub fn new(rw: Box<PageFile>) -> LogFile {
        LogFile { rw, appended: HashSet::new(), tbl_len: 0 }
    }

    pub fn init (&mut self) -> Result<(), BCSError> {
        Ok(())
    }

    pub fn has_page(&self, offset: Offset) -> bool {
        self.appended.contains(&offset)
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
        Ok(self.rw.flush()?)
    }

    fn len(&self) -> Result<u64, BCSError> {
        self.rw.len()
    }

    fn truncate(&mut self, len: u64) -> Result<(), BCSError> {
        self.rw.truncate(len)
    }

    fn sync(&self) -> Result<(), BCSError> {
        self.rw.sync()
    }

    fn read_page (&self, offset: Offset) -> Result<Page, BCSError> {
        self.rw.read_page(offset)
    }

    fn append_page(&mut self, page: Page) -> Result<(), BCSError> {
        if self.appended.insert(page.offset) {
            self.rw.append_page(page)?;
        }
        Ok(())
    }

    fn write_page(&mut self, _: Page) -> Result<(), BCSError> {
        unimplemented!()
    }
}