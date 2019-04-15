//
// Copyright 2018-2019 Tamas Blummer
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
use error::HammersbaldError;
use pref::PRef;

use std::collections::HashSet;

pub struct LogFile {
    file: Box<PagedFile>,
    logged: HashSet<PRef>,
    source_len: u64
}

impl LogFile {
    pub fn new(rw: Box<PagedFile>) -> LogFile {
        LogFile { file: rw, logged: HashSet::new(), source_len:0 }
    }

    pub fn init (&mut self, data_len: u64, table_len: u64, link_len: u64) -> Result<(), HammersbaldError> {
        self.truncate(0)?;
        let mut first = Page::new(PRef::from(0));
        first.write_pref(0, PRef::from(data_len));
        first.write_pref(6, PRef::from(table_len));
        first.write_pref(12, PRef::from(link_len));

        self.append_pages(&vec!(first))?;
        self.flush()?;
        Ok(())
    }

    pub fn page_iter (&self) -> PagedFileIterator {
        PagedFileIterator::new(self, PRef::from(0))
    }

    pub fn log_page(&mut self, pref: PRef, source: &PagedFile) -> Result<(), HammersbaldError>{
        if pref.as_u64() < self.source_len && self.logged.insert(pref) {
            if let Some(page) = source.read_page(pref)? {
                self.append_pages(&vec!(page))?;
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
    fn read_pages (&self, pref: PRef, n: usize) -> Result<Vec<Page>, HammersbaldError> {
        self.file.read_pages(pref, n)
    }

    fn len(&self) -> Result<u64, HammersbaldError> {
        self.file.len()
    }

    fn truncate(&mut self, len: u64) -> Result<(), HammersbaldError> {
        self.file.truncate(len)
    }

    fn sync(&self) -> Result<(), HammersbaldError> {
        self.file.sync()
    }

    fn shutdown (&mut self) {}

    fn append_pages(&mut self, pages: &Vec<Page>) -> Result<(), HammersbaldError> {
        self.file.append_pages(pages)
    }

    fn update_page(&mut self, _: Page) -> Result<u64, HammersbaldError> {
        unimplemented!()
    }

    fn flush(&mut self) -> Result<(), HammersbaldError> {
        Ok(self.file.flush()?)
    }
}
