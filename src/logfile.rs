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
//! The writer of the log file.
//!

use page::Page;
use pagedfile::{PagedFile, PagedFileIterator};
use error::BCDBError;
use offset::Offset;

use std::collections::HashSet;

pub struct LogFile {
    file: Box<PagedFile>,
    logged: HashSet<Offset>,
    source_len: u64
}

impl LogFile {
    pub fn new(rw: Box<PagedFile>) -> LogFile {
        LogFile { file: rw, logged: HashSet::new(), source_len:0 }
    }

    pub fn init (&mut self, data_len: u64, table_len: u64, link_len: u64) -> Result<(), BCDBError> {
        self.truncate(0)?;
        let mut first = Page::new();
        first.write(0, &[0xBC, 0x00]).unwrap();
        first.write_offset(2, Offset::from(data_len)).unwrap();
        first.write_offset(8, Offset::from(table_len)).unwrap();
        first.write_offset(14, Offset::from(link_len)).unwrap();

        self.append_page(first)?;
        self.flush()?;
        Ok(())
    }

    pub fn page_iter (&self) -> PagedFileIterator {
        PagedFileIterator::new(self, Offset::from(0))
    }

    pub fn log_page(&mut self, offset: Offset, source: &PagedFile) -> Result<(), BCDBError>{
        if offset.as_u64() < self.source_len && self.logged.insert(offset) {
            if let Some(page) = source.read_page(offset)? {
                self.append_page(page)?;
            }
        }
        Ok(())
    }

    pub fn reset(&mut self, len: u64) {
        self.source_len = len;
        self.logged.clear();
    }
}

impl PagedFile for LogFile {
    fn flush(&mut self) -> Result<(), BCDBError> {
        Ok(self.file.flush()?)
    }

    fn len(&self) -> Result<u64, BCDBError> {
        self.file.len()
    }

    fn truncate(&mut self, len: u64) -> Result<(), BCDBError> {
        self.file.truncate(len)
    }

    fn sync(&self) -> Result<(), BCDBError> {
        self.file.sync()
    }

    fn read_page (&self, offset: Offset) -> Result<Option<Page>, BCDBError> {
        self.file.read_page(offset)
    }

    fn append_page(&mut self, page: Page) -> Result<(), BCDBError> {
        self.file.append_page(page)
    }

    fn write_page(&mut self, _: Offset, _: Page) -> Result<u64, BCDBError> {
        unimplemented!()
    }

    fn shutdown (&mut self) {}
}
