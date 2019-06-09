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
//! # a file that is read and wrote by pages
//!

use page::{Page, PAGE_SIZE, PAGE_PAYLOAD_SIZE};
use error::HammersbaldError;
use pref::PRef;

use std::cmp::{max,min};

/// a paged file
pub trait PagedFile : Send + Sync {
    /// read a page starting at pref
    fn read_page (&self, pref: PRef) -> Result<Option<Page>, HammersbaldError>;
    /// read n page starting at pref
    fn read_pages (&self, pref: PRef, n: usize) -> Result<Vec<Page>, HammersbaldError>;
    /// length of the storage
    fn len (&self) -> Result<u64, HammersbaldError>;
    /// truncate storage
    fn truncate(&mut self, new_len: u64) -> Result<(), HammersbaldError>;
    /// tell OS to flush buffers to disk
    fn sync (&self) -> Result<(), HammersbaldError>;
    /// shutdown async write
    fn shutdown (&mut self);
    /// append pages
    fn append_pages (&mut self, pages: &Vec<Page>) -> Result<(), HammersbaldError>;
    /// write a page at its position
    fn update_page (&mut self, page: Page) -> Result<u64, HammersbaldError>;
    /// flush buffered writes
    fn flush(&mut self) -> Result<(), HammersbaldError>;
}

pub trait PagedFileRead {
    /// read a slice from a paged file
    fn read(&self, pos: PRef, buf: &mut [u8]) -> Result<PRef, HammersbaldError>;
}

pub trait PagedFileWrite {
    /// write a slice to a paged file
    fn append(&mut self, buf: &[u8]) -> Result<PRef, HammersbaldError>;
}

/// a reader for a paged file
pub struct PagedFileAppender {
    file: Box<PagedFile>,
    pos: PRef,
    page: Option<Page>,
    lep: PRef
}

impl PagedFileAppender {
    /// create a reader that starts at a position
    pub fn new (file: Box<PagedFile>, pos: PRef, lep: PRef) -> PagedFileAppender {
        PagedFileAppender {file, pos, page: None, lep}
    }

    pub fn position (&self) -> PRef {
        self.pos
    }

    pub fn lep (&self) -> PRef {
        self.lep
    }

    pub fn advance (&mut self) {
        self.lep = self.pos;
    }

    pub fn append(&mut self, buf: &[u8]) -> Result<PRef, HammersbaldError> {
        let mut pages = Vec::with_capacity(buf.len()/PAGE_SIZE+1);
        let mut wrote = 0;
        while wrote < buf.len() {
            if self.page.is_none () {
                self.page = Some(Page::new(self.lep));
            }
            if let Some(ref mut page) = self.page {
                let space = min(PAGE_PAYLOAD_SIZE - self.pos.in_page_pos(), buf.len() - wrote);
                page.write(self.pos.in_page_pos(), &buf[wrote..wrote + space]);
                wrote += space;
                self.pos += space as u64;
                if self.pos.in_page_pos() == PAGE_PAYLOAD_SIZE {
                    page.write_pref(PAGE_PAYLOAD_SIZE, self.lep);
                    pages.push(page.clone());
                    self.pos += (PAGE_SIZE - PAGE_PAYLOAD_SIZE) as u64;
                }
            }
            if self.pos.in_page_pos() == 0 {
                self.page = None;
            }
        }
        self.file.append_pages(&pages)?;
        Ok(self.pos)
    }

    pub fn read(&self, mut pos: PRef, buf: &mut [u8]) -> Result<PRef, HammersbaldError> {
        let np = buf.len() / PAGE_SIZE + if buf.len() % PAGE_SIZE != 0 {1} else {0};
        let pages = self.read_pages(pos, np)?;
        let mut pi = pages.iter();
        let mut read = 0;
        while read < buf.len() {
            if let Some(page) = pi.next() {
                let have = min(PAGE_PAYLOAD_SIZE - pos.in_page_pos(), buf.len() - read);
                page.read(pos.in_page_pos(), &mut buf[read .. read + have]);
                read += have;
                pos += have as u64;
                if pos.in_page_pos() == PAGE_PAYLOAD_SIZE {
                    pos += (PAGE_SIZE - PAGE_PAYLOAD_SIZE) as u64;
                }
            }
            else {
                break;
            }
        }
        Ok(pos)
    }
}

impl PagedFile for PagedFileAppender {
    fn read_page(&self, pref: PRef) -> Result<Option<Page>, HammersbaldError> {
        let result = self.read_pages(pref, 1)?;
        if let Some (page) = result.first() {
            Ok(Some(page.clone()))
        }
        else {
            Ok(None)
        }
    }

    fn read_pages(&self, pref: PRef, n: usize) -> Result<Vec<Page>, HammersbaldError> {
        let end = pref + (n * PAGE_SIZE) as u64;
        if let Some(ref page) = self.page {
            let mut result = Vec::new();
            if pref < self.pos.this_page() {
                let np = ((min(self.pos.this_page().as_u64(), end.as_u64()) - pref.as_u64()) / PAGE_SIZE as u64) as usize;
                result.extend(self.file.read_pages(pref, np)?);
                if result.len() < np {
                    return Ok(result);
                }
            }
            if end > self.pos.this_page() {
                if pref <= self.pos.this_page() {
                    result.push(page.clone());
                }
                if end > self.pos.this_page() + PAGE_SIZE as u64 {
                    let start = max(pref, self.pos.this_page() + PAGE_SIZE as u64);
                    let np = ((end.as_u64() - start.as_u64()) / PAGE_SIZE as u64) as usize;
                    result.extend(self.file.read_pages(start, np)?);
                }
            }
            return Ok(result);
        }
        return self.file.read_pages(pref, n)
    }

    fn len(&self) -> Result<u64, HammersbaldError> {
        self.file.len()
    }

    fn truncate(&mut self, new_len: u64) -> Result<(), HammersbaldError> {
        if new_len >= PAGE_SIZE as u64 {
            if let Some(last_page) = self.file.read_pages(PRef::from(new_len - PAGE_SIZE as u64), 1)?.first() {
                self.lep = last_page.read_pref(PAGE_PAYLOAD_SIZE);
            }
            else {
                return Err(HammersbaldError::Corrupted("where is the last page?".to_string()));
            }
        }
        else {
            self.lep = PRef::invalid();
        }
        self.pos = PRef::from(new_len);
        self.file.truncate(new_len)
    }

    fn sync(&self) -> Result<(), HammersbaldError> {
        self.file.sync()
    }

    fn shutdown(&mut self) {
        self.file.shutdown()
    }

    fn append_pages(&mut self, pages: &Vec<Page>) -> Result<(), HammersbaldError> {
        self.file.append_pages(pages)
    }

    fn update_page(&mut self, _: Page) -> Result<u64, HammersbaldError> {
        unimplemented!()
    }

    fn flush(&mut self) -> Result<(), HammersbaldError> {
        if let Some(ref mut page) = self.page {
            if self.pos.in_page_pos() > 0 {
                page.write_pref(PAGE_PAYLOAD_SIZE, self.lep);
                self.file.append_pages(&vec!(page.clone()))?;
                self.pos += PAGE_SIZE as u64 - self.pos.in_page_pos() as u64;
            }
        }
        Ok(self.file.flush()?)
    }
}

/// iterate through pages of a paged file
pub struct PagedFileIterator<'file> {
    // the current page of the iterator
    pagenumber: u64,
    // the iterated file
    file: &'file PagedFile
}

/// page iterator
impl<'file> PagedFileIterator<'file> {
    /// create a new iterator starting at given page
    pub fn new (file: &'file PagedFile, pref: PRef) -> PagedFileIterator {
        PagedFileIterator {pagenumber: pref.page_number(), file}
    }
}

impl<'file> Iterator for PagedFileIterator<'file> {
    type Item = Page;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pagenumber <= (1 << 35) / PAGE_SIZE as u64 {
            let pref = PRef::from((self.pagenumber)* PAGE_SIZE as u64);
            if let Ok(pages) = self.file.read_pages(pref, 1) {
                if let Some(page) = pages.first() {
                    self.pagenumber += 1;
                    return Some(page.clone());
                }
            }
        }
        None
    }
}
