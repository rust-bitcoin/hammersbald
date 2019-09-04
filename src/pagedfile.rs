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

use page::{Page, PAGE_SIZE};
use error::Error;
use pref::PRef;

use std::cmp::min;
use std::io::{self, ErrorKind};

/// a paged file
pub trait PagedFile : Send + Sync {
    /// read a page at pref
    fn read_page (&self, pref: PRef) -> Result<Option<Page>, Error>;
    /// length of the storage
    fn len (&self) -> Result<u64, Error>;
    /// truncate storage
    fn truncate(&mut self, new_len: u64) -> Result<(), Error>;
    /// tell OS to flush buffers to disk
    fn sync (&self) -> Result<(), Error>;
    /// shutdown async write
    fn shutdown (&mut self);
    /// append pages
    fn append_page(&mut self, page: Page) -> Result<(), Error>;
    /// write a page at its position
    fn update_page (&mut self, page: Page) -> Result<u64, Error>;
    /// flush buffered writes
    fn flush(&mut self) -> Result<(), Error>;
}

pub trait PagedFileRead {
    /// read a slice from a paged file
    fn read(&self, pos: PRef, buf: &mut [u8]) -> Result<PRef, Error>;
}

pub trait PagedFileWrite {
    /// write a slice to a paged file
    fn append(&mut self, buf: &[u8]) -> Result<PRef, Error>;
}

/// a reader for a paged file
pub struct PagedFileAppender {
    file: Box<dyn PagedFile>,
    pos: PRef,
    page: Option<Page>
}

impl PagedFileAppender {
    /// create a reader that starts at a position
    pub fn new (file: Box<dyn PagedFile>, pos: PRef) -> PagedFileAppender {
        PagedFileAppender {file, pos, page: None}
    }

    pub fn position (&self) -> PRef {
        self.pos
    }

    pub fn append(&mut self, buf: &[u8]) -> Result<PRef, Error> {
        let mut wrote = 0;
        while wrote < buf.len() {
            if self.page.is_none () {
                self.page = Some(Page::new());
            }
            if let Some(ref mut page) = self.page {
                let space = min(PAGE_SIZE - self.pos.in_page_pos(), buf.len() - wrote);
                page.write(self.pos.in_page_pos(), &buf[wrote..wrote + space]);
                wrote += space;
                if self.pos.in_page_pos() + space == PAGE_SIZE {
                    self.file.append_page(page.clone())?;
                }
                self.pos += space as u64;
            }
            if self.pos.in_page_pos() == 0 {
                self.page = None;
            }
        }
        Ok(self.pos)
    }

    pub fn read(&self, mut pos: PRef, buf: &mut [u8], len: usize) -> Result<PRef, Error> {
        let mut read = 0;
        while read < len {
            if let Some(ref page) = self.read_page (pos.this_page())? {
                let have = min(PAGE_SIZE - pos.in_page_pos(), len - read);
                page.read(pos.in_page_pos(), &mut buf[read .. read + have]);
                read += have;
                pos += have as u64;
            }
            else {
                return Err(Error::IO(io::Error::from(ErrorKind::UnexpectedEof)));
            }
        }
        Ok(pos)
    }
}

impl PagedFile for PagedFileAppender {
    fn read_page(&self, pref: PRef) -> Result<Option<Page>, Error> {
        if let Some(ref page) = self.page {
            if pref.this_page() == self.pos.this_page() {
                return Ok(Some(page.clone()));
            }
        }
        self.file.read_page(pref)
    }

    fn len(&self) -> Result<u64, Error> {
        self.file.len()
    }

    fn truncate(&mut self, new_len: u64) -> Result<(), Error> {
        self.pos = PRef::from(new_len);
        self.file.truncate(new_len)
    }

    fn sync(&self) -> Result<(), Error> {
        self.file.sync()
    }

    fn shutdown(&mut self) {
        self.file.shutdown()
    }

    fn append_page(&mut self, page: Page) -> Result<(), Error> {
        self.file.append_page(page)
    }

    fn update_page(&mut self, _: Page) -> Result<u64, Error> {
        unimplemented!()
    }

    fn flush(&mut self) -> Result<(), Error> {
        if let Some(ref mut page) = self.page {
            if self.pos.in_page_pos() > 0 {
                self.file.append_page(page.clone())?;
                self.pos += PAGE_SIZE as u64 - self.pos.in_page_pos() as u64;
            }
        }
        self.page = None;
        Ok(self.file.flush()?)
    }
}

/// iterate through pages of a paged file
pub struct PagedFileIterator<'file> {
    // the current page of the iterator
    pagenumber: u64,
    // the iterated file
    file: &'file dyn PagedFile
}

/// page iterator
impl<'file> PagedFileIterator<'file> {
    /// create a new iterator starting at given page
    pub fn new (file: &'file dyn PagedFile, pref: PRef) -> PagedFileIterator {
        PagedFileIterator {pagenumber: pref.page_number(), file}
    }
}

impl<'file> Iterator for PagedFileIterator<'file> {
    type Item = Page;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pagenumber <= (1 << 35) / PAGE_SIZE as u64 {
            let pref = PRef::from((self.pagenumber)* PAGE_SIZE as u64);
            if let Ok(Some(page)) = self.file.read_page(pref) {
                self.pagenumber += 1;
                return Some(page);
            }
        }
        None
    }
}
