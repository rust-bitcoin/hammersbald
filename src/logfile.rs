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
//! A synchronous append writer of a log file.
//!

use page::{Page, TablePage, PageFile, PAGE_SIZE};
use tablefile::TableFile;
use error::BCDBError;
use offset::Offset;

/// The buffer pool
pub struct LogFile {
    rw: Box<PageFile>
}

impl LogFile {
    pub fn new(rw: Box<PageFile>) -> LogFile {
        LogFile { rw }
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

    pub fn page_iter (&self) -> LogPageIterator {
        LogPageIterator::new(self, 0)
    }

    pub fn append_table_page(&mut self, page: TablePage) -> Result<u64, BCDBError> {
        self.append_page(page.page)
    }

    pub fn log_pages (&mut self, offsets: Vec<Offset>, table_file: &TableFile) -> Result<(), BCDBError> {
        for offset in offsets {
            if let Some(page) = table_file.read_page(offset)? {
                self.append_page(page)?;
            } else {
                return Err(BCDBError::Corrupted(format!("can not find pre-image to log {}", offset)));
            }
        }
        self.flush()?;
        self.sync()
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

    fn append_page(&mut self, page: Page) -> Result<u64, BCDBError> {
        self.rw.append_page(page)
    }

    fn write_page(&mut self, _: Offset, _: Page) -> Result<u64, BCDBError> {
        unimplemented!()
    }

    fn shutdown (&mut self) {}
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