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
//! # a page in the blockchain store
//!
//! The page is the unit of read and write.
//!
//!

use error::BCDBError;
use offset::Offset;

use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};

use std::io::{Read, Cursor};
use std::io;
use std::cmp::min;

pub const PAGE_SIZE: usize = 4096;

/// A page of the persistent files
#[derive(Clone)]
pub struct Page {
    pub payload: [u8; PAGE_SIZE]
}

impl Page {
    /// create a new empty page to be appended at given offset
    pub fn new () -> Page {
        Page {payload: [0u8; PAGE_SIZE]}
    }

    /// create a Page from read buffer
    pub fn from_buf (payload: [u8; PAGE_SIZE]) -> Page {
        Page {payload}
    }

    /// append some data
    /// will return Error::DoesNotFit if data does not fit into the page
    pub fn write (&mut self, pos: usize, data: & [u8]) -> Result<(), BCDBError> {
        if pos + data.len() > PAGE_SIZE {
            return Err (BCDBError::DoesNotFit);
        }
        self.payload [pos .. pos + data.len()].copy_from_slice(&data[..]);
        Ok(())
    }

    /// write an offset
    pub fn write_offset (&mut self, pos: usize, offset: Offset) -> Result<(), BCDBError> {
        if pos + 6 > PAGE_SIZE {
            return Err (BCDBError::DoesNotFit);
        }
        self.payload[pos .. pos + 6].copy_from_slice(offset.to_vec().as_slice());
        Ok(())
    }

    /// read some data
    /// will return Error::DoesNotFit if data does not fit into the page
    pub fn read (&self, pos: usize, data: &mut [u8]) -> Result<(), BCDBError> {
        if pos + data.len() > PAGE_SIZE {
            return Err (BCDBError::DoesNotFit);
        }
        let len = data.len();
        data[..].copy_from_slice(&self.payload [pos .. pos + len]);
        Ok(())
    }

    /// read a stored offset
    pub fn read_offset(&self, pos: usize) -> Result<Offset, BCDBError> {
        let mut buf = [0u8;6];
        self.read(pos, &mut buf)?;
        Ok(Offset::from(&buf[..]))
    }

    pub fn read_u64(&self, pos: usize) -> Result<u64, BCDBError> {
        let mut buf = [0u8;8];
        self.read(pos, &mut buf)?;
        Ok(Cursor::new(buf).read_u64::<BigEndian>()?)
    }

    pub fn write_u64(&mut self, pos: usize, n: u64) -> Result<(), BCDBError> {
        let mut bytes = Vec::new();
        bytes.write_u64::<BigEndian>(n)?;
        self.write(pos, &bytes.as_slice())
    }

    /// finish a page after appends to write out
    pub fn finish (&self) -> [u8; PAGE_SIZE] {
        self.payload
    }
}

/// a read-write-seak-able storage with added methods
/// synchronized in its implementation
pub trait PageFile : Send + Sync {
    /// flush buffered writes
    fn flush(&mut self) -> Result<(), BCDBError>;
    /// length of the storage
    fn len (&self) -> Result<u64, BCDBError>;
    /// truncate storage
    fn truncate(&mut self, new_len: u64) -> Result<(), BCDBError>;
    /// tell OS to flush buffers to disk
    fn sync (&self) -> Result<(), BCDBError>;
    /// read a page at given offset
    fn read_page (&self, offset: Offset) -> Result<Option<Page>, BCDBError>;
    /// append a page (ignore offset in the Page)
    fn append_page (&mut self, page: Page) -> Result<u64, BCDBError>;
    /// write a page at its position as specified in page.offset
    fn write_page (&mut self, offset: Offset, page: Page) -> Result<u64, BCDBError>;
    /// shutdown async processing
    fn shutdown (&mut self);
}

/// iterate through pages of a paged file
pub struct PageIterator<'file> {
    // the current page of the iterator
    pagenumber: u64,
    // the current page
    page: Option<Page>,
    // position on current page
    pos: usize,
    // the iterated file
    file: &'file PageFile
}

/// page iterator
impl<'file> PageIterator<'file> {
    /// create a new iterator starting at given page
    pub fn new (file: &'file PageFile, offset: Offset) -> PageIterator {
        PageIterator{pagenumber: offset.page_number(), page: None, pos: offset.in_page_pos(), file}
    }
}

impl<'file> Iterator for PageIterator<'file> {
    type Item = Page;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pagenumber <= (1 << 35) / PAGE_SIZE as u64 {
            let offset = Offset::from((self.pagenumber)* PAGE_SIZE as u64);
            if let Ok(Some(page)) = self.file.read_page(offset) {
                self.pagenumber += 1;
                return Some(page);
            }
        }
        None
    }
}

impl<'file> Read for PageIterator<'file> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, io::Error> {
        let mut read = 0;
        loop {
            if self.page.is_none() {
                self.page = self.file.read_page(Offset::from(self.pagenumber))?;
            }
            if let Some(ref page) = self.page {
                let have = min(PAGE_SIZE - self.pos, buf.len() - read);
                buf[read..read + have].copy_from_slice(&page.payload[self.pos..self.pos + have]);
                self.pos += have;
                read += have;
            }
            else {
                return Ok(read)
            }
            if read == buf.len() {
                break;
            } else {
                self.page = None;
                self.pagenumber += 1;
                self.pos = 0;
            }
        }
        Ok(buf.len())
    }
}
