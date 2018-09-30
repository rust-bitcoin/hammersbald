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

use page::{Page, PageFile, PAGE_SIZE};
use error::BCDBError;
use types::Offset;

use std::collections::HashSet;
use std::sync::Arc;

/// The buffer pool
pub struct LogFile {
    rw: Box<PageFile>,
    logged: HashSet<Offset>,
    pub tbl_len: u64
}

impl LogFile {
    pub fn new(rw: Box<PageFile>) -> LogFile {
        LogFile { rw, logged: HashSet::new(), tbl_len: 0 }
    }

    pub fn init (&mut self) -> Result<(), BCDBError> {
        Ok(())
    }

    pub fn is_logged (&self, offset: Offset) -> bool {
        self.logged.contains(&offset)
    }

    /// empties the set of logged pages
    pub fn clear_cache(&mut self) {
        self.logged.clear();
    }

    pub fn page_iter (&self) -> LogPageIterator {
        LogPageIterator::new(self, 0)
    }
}

impl PageFile for LogFile {
    fn flush(&mut self) -> Result<(), BCDBError> {
        Ok(self.rw.flush()?)
    }

    fn len(&self) -> Result<u64, BCDBError> {
        self.rw.len()
    }

    fn truncate(&mut self, len: u64) -> Result<(), BCDBError> {
        self.rw.truncate(len)
    }

    fn sync(&self) -> Result<(), BCDBError> {
        self.rw.sync()
    }

    fn read_page (&self, offset: Offset) -> Result<Option<Page>, BCDBError> {
        self.rw.read_page(offset)
    }

    fn append_page(&mut self, page: Arc<Page>) -> Result<(), BCDBError> {
        self.logged.insert(page.offset);
        self.rw.append_page(page)
    }

    fn write_page(&mut self, _: Offset, _: Arc<Page>) -> Result<(), BCDBError> {
        unimplemented!()
    }
}

/// iterate through pages of a paged file
pub struct LogPageIterator<'file> {
    /// the current page of the iterator
    pub pagenumber: u64,
    file: &'file LogFile
}

/// page iterator
impl<'file> LogPageIterator<'file> {
    /// create a new iterator starting at given page
    pub fn new (file: &'file LogFile, pagenumber: u64) -> LogPageIterator {
        LogPageIterator{pagenumber, file}
    }
}

impl<'file> Iterator for LogPageIterator<'file> {
    type Item = Page;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pagenumber < (1 << 47) / PAGE_SIZE as u64 {
            let offset = Offset::from((self.pagenumber)* PAGE_SIZE as u64);
            if let Ok(Some(page)) = self.file.read_page(offset) {
                self.pagenumber += 1;
                return Some(page);
            }
        }
        None
    }
}