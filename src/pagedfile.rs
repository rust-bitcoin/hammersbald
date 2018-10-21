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
//! # a file that is read and wrote by pages
//!

use page::{Page, PAGE_SIZE, PAGE_PAYLOAD_SIZE};
use error::BCDBError;
use pref::PRef;
use format::Envelope;

use std::cmp::min;
use std::io::Read;
use std::io::Error;

/// a paged file
pub trait PagedFile {
    /// read a page at pref
    fn read_page (&self, pref: PRef) -> Result<Option<Page>, BCDBError>;
    /// length of the storage
    fn len (&self) -> Result<u64, BCDBError>;
    /// truncate storage
    fn truncate(&mut self, new_len: u64) -> Result<(), BCDBError>;
    /// tell OS to flush buffers to disk
    fn sync (&self) -> Result<(), BCDBError>;
    /// shutdown async write
    fn shutdown (&mut self);
    /// append a page
    fn append_page (&mut self, page: Page) -> Result<(), BCDBError>;
    /// write a page at its position
    fn update_page (&mut self, page: Page) -> Result<u64, BCDBError>;
    /// flush buffered writes
    fn flush(&mut self) -> Result<(), BCDBError>;
}

pub trait PagedFileRead {
    /// read a slice from a paged file
    fn read(&self, pos: PRef, buf: &mut [u8]) -> Result<PRef, BCDBError>;
}

pub trait PagedFileWrite {
    /// write a slice to a paged file
    fn append(&mut self, buf: &[u8]) -> Result<PRef, BCDBError>;
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

    pub fn advance (&mut self) -> PRef {
        let lep = self.lep;
        self.lep = self.pos;
        lep
    }

    pub fn read_envelope(&self, pref: PRef) -> Result<Envelope, BCDBError> {
        Envelope::deseralize(&mut AppenderReader{appender: self, pos: pref})
    }

    pub fn append(&mut self, buf: &[u8]) -> Result<PRef, BCDBError> {
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
                    self.file.append_page(page.clone())?;
                    self.pos += (PAGE_SIZE - PAGE_PAYLOAD_SIZE) as u64;
                }
            }
            if self.pos.in_page_pos() == 0 {
                self.page = None;
            }
        }
        Ok(self.pos)
    }

    pub fn read(&self, mut pos: PRef, buf: &mut [u8]) -> Result<PRef, BCDBError> {
        let mut read = 0;
        while read < buf.len() {
            if let Some(ref page) = self.read_page(pos.this_page())? {
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

struct AppenderReader<'a> {
    pub appender: &'a PagedFileAppender,
    pub pos: PRef
}

impl<'a> Read for AppenderReader<'a> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, Error> {
        self.pos = self.appender.read(self.pos, buf)?;
        Ok(buf.len())
    }
}

impl PagedFile for PagedFileAppender {
    fn read_page(&self, pref: PRef) -> Result<Option<Page>, BCDBError> {
        if let Some(ref page) = self.page {
            if self.pos.this_page() == pref {
                return Ok(Some(page.clone()))
            }
        }
        return self.file.read_page(pref)
    }

    fn len(&self) -> Result<u64, BCDBError> {
        self.file.len()
    }

    fn truncate(&mut self, new_len: u64) -> Result<(), BCDBError> {
        if new_len >= PAGE_SIZE as u64 {
            if let Some(last_page) = self.file.read_page(PRef::from(new_len - PAGE_SIZE as u64))? {
                self.lep = last_page.read_offset(PAGE_PAYLOAD_SIZE);
            }
            else {
                return Err(BCDBError::Corrupted("where is the last page?".to_string()));
            }
        }
        else {
            self.lep = PRef::from(0);
        }
        self.file.truncate(new_len)
    }

    fn sync(&self) -> Result<(), BCDBError> {
        self.file.sync()
    }

    fn shutdown(&mut self) {
        self.file.shutdown()
    }

    fn append_page(&mut self, page: Page) -> Result<(), BCDBError> {
        if let Some (ref page) = self.page {
            if self.pos.in_page_pos() > 0 {
                self.file.append_page(page.clone())?;
                self.pos += PAGE_SIZE as u64 - self.pos.in_page_pos() as u64;
            }
        }
        self.pos += PAGE_SIZE as u64;
        self.file.append_page(page)
    }

    fn update_page(&mut self, _: Page) -> Result<u64, BCDBError> {
        unimplemented!()
    }

    fn flush(&mut self) -> Result<(), BCDBError> {
        if let Some(ref page) = self.page {
            if self.pos.in_page_pos() > 0 {
                self.file.append_page(page.clone())?;
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
            if let Ok(Some(page)) = self.file.read_page(pref) {
                self.pagenumber += 1;
                return Some(page);
            }
        }
        None
    }
}
