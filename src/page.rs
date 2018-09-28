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
//! <pre>
//! +------------------------------------+
//! |    | payload                       |
//! +----+-------------------------------+
//! |u48 | block offset                  |
//! +----+-------------------------------+
//! </pre>
//!

use error::BCDBError;
use types::Offset;

use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};

use std::sync::Arc;
use std::io::Cursor;

pub const PAGE_SIZE: usize = 4096;
pub const PAYLOAD_MAX: usize = 4090;

/// A page of the persistent files
#[derive(Clone)]
pub struct Page {
    pub payload: [u8; PAYLOAD_MAX],
    pub offset: Offset
}

impl Page {
    /// create a new empty page to be appended at given offset
    pub fn new (offset: Offset) -> Page {
        Page {payload: [0u8; PAYLOAD_MAX], offset}
    }

    /// create a Page from read buffer
    pub fn from_buf (buf: [u8; PAGE_SIZE as usize]) -> Page {
        let mut payload = [0u8; PAYLOAD_MAX];
        payload.copy_from_slice(&buf[0..PAYLOAD_MAX]);
        Page {payload, offset: Offset::from_slice(&buf[PAYLOAD_MAX .. PAYLOAD_MAX + 6]).unwrap() }
    }

    /// append some data
    /// will return Error::DoesNotFit if data does not fit into the page
    pub fn write (&mut self, pos: usize, data: & [u8]) -> Result<(), BCDBError> {
        if pos + data.len() > PAYLOAD_MAX {
            return Err (BCDBError::DoesNotFit);
        }
        self.payload [pos .. pos + data.len()].copy_from_slice(&data[..]);
        Ok(())
    }

    /// write an offset
    pub fn write_offset (&mut self, pos: usize, offset: Offset) -> Result<(), BCDBError> {
        if pos + 6 > PAYLOAD_MAX {
            return Err (BCDBError::DoesNotFit);
        }
        offset.serialize(&mut self.payload[pos .. pos + 6]);
        Ok(())
    }

    /// read some data
    /// will return Error::DoesNotFit if data does not fit into the page
    pub fn read (&self, pos: usize, data: &mut [u8]) -> Result<(), BCDBError> {
        if pos + data.len() > PAYLOAD_MAX {
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
        Offset::from_slice(&buf)
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
        let mut page = [0u8; PAGE_SIZE];
        page[0 .. PAYLOAD_MAX].copy_from_slice (&self.payload[..]);
        self.offset.serialize(&mut page[PAYLOAD_MAX ..]);
        page
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
    fn read_page (&self, offset: Offset) -> Result<Page, BCDBError>;
    /// append a page (ignore offset in the Page)
    fn append_page (&mut self, page: Page) -> Result<(), BCDBError>;
    /// write a page at its position as specified in page.offset
    fn write_page (&mut self, page: Page) -> Result<(), BCDBError>;
    /// write a batch of pages in parallel (if possible)
    fn write_batch (&mut self, writes: Vec<Arc<Page>>) -> Result<(), BCDBError>;
}

/// iterate through pages of a paged file
pub struct PageIterator<'file> {
    /// the current page of the iterator
    pub pagenumber: u64,
    file: &'file PageFile
}

/// page iterator
impl<'file> PageIterator<'file> {
    /// create a new iterator starting at given page
    pub fn new (file: &'file PageFile, pagenumber: u64) -> PageIterator {
        PageIterator{pagenumber, file}
    }
}

impl<'file> Iterator for PageIterator<'file> {
    type Item = Page;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pagenumber < (1 << 47) / PAGE_SIZE as u64 {
            let offset = Offset::new((self.pagenumber)* PAGE_SIZE as u64).unwrap();
            if let Ok(page) = self.file.read_page(offset) {
                self.pagenumber += 1;
                return Some(page);
            }
        }
        None
    }
}


#[cfg(test)]
mod test {
    extern crate hex;

    use super::*;
    #[test]
    fn form_test () {
        let mut page = Page::new(Offset::new(4711).unwrap());
        let payload: &[u8] = "hello world".as_bytes();
        page.write(0,payload).unwrap();
        let result = page.finish();

        let mut check = [0u8; PAGE_SIZE];
        check[0 .. payload.len()].copy_from_slice(payload);
        check[PAGE_SIZE -1] = (4711 % 256) as u8;
        check[PAGE_SIZE -2] = (4711 / 256) as u8;
        assert_eq!(hex::encode(&result[..]), hex::encode(&check[..]));

        let page2 = Page::from_buf(check);
        assert_eq!(page.offset, page2.offset);
        assert_eq!(hex::encode(&page.payload[..]), hex::encode(&page2.payload[..]));
    }
}
