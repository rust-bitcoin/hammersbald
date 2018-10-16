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
//! # The table file
//! Specific implementation details to hash table file
//!

use page::{Page, PageFile, PAGE_SIZE};
use error::BCDBError;
use offset::Offset;

pub const FIRST_PAGE_HEAD:usize = 28;
pub const BUCKETS_FIRST_PAGE:usize = 677;
pub const BUCKETS_PER_PAGE:usize = 681;
pub const BUCKET_SIZE: usize = 6;

/// The key file
pub struct TableFile {
    file: Box<PageFile>,
    pub last_len: u64
}

impl TableFile {
    pub fn new (file: Box<PageFile>) -> Result<TableFile, BCDBError> {
        let last_len = file.len()?;
        Ok(TableFile {file, last_len})
    }

    fn table_offset (bucket: u32) -> Offset {
        if (bucket as u64) < BUCKETS_FIRST_PAGE as u64 {
            Offset::from((bucket as u64 / BUCKETS_FIRST_PAGE as u64) * PAGE_SIZE as u64
                + (bucket as u64 % BUCKETS_FIRST_PAGE as u64) * BUCKET_SIZE as u64 + FIRST_PAGE_HEAD as u64)
        }
        else {
            Offset::from((((bucket as u64 - BUCKETS_FIRST_PAGE as u64) / BUCKETS_PER_PAGE as u64) + 1) * PAGE_SIZE as u64
                + (bucket as u64 % BUCKETS_PER_PAGE as u64) * BUCKET_SIZE as u64)
        }
    }

    pub fn iter<'a>(&'a self) -> impl Iterator<Item=Offset> +'a {
        BucketIterator{file: self, n:0}
    }

    pub fn patch_page(&mut self, page: TablePage) -> Result<u64, BCDBError> {
        self.file.write_page(page.offset, page.page)
    }

    pub fn read_table_page(&self, offset: Offset) -> Result<Option<TablePage>, BCDBError> {
        if let Some(page) = self.read_page(offset)? {
            let key_page = TablePage::from(page);
            if key_page.offset.as_u64() != offset.as_u64() {
                return Err(BCDBError::Corrupted(format!("hash table page {} does not have the offset of its position", offset)));
            }
            return Ok(Some(key_page));
        }
        Ok(None)
    }

    pub fn write_table_page(&mut self, page: TablePage) -> Result<u64, BCDBError> {
        self.write_page(page.offset, page.page)
    }
}

impl PageFile for TableFile {
    fn flush(&mut self) -> Result<(), BCDBError> {
        self.file.flush()
    }

    fn len(&self) -> Result<u64, BCDBError> {
        self.file.len()
    }

    fn truncate(&mut self, new_len: u64) -> Result<(), BCDBError> {
        self.file.truncate(new_len)
    }

    fn sync(&self) -> Result<(), BCDBError> {
        self.file.sync()
    }

    fn read_page(&self, offset: Offset) -> Result<Option<Page>, BCDBError> {
        self.file.read_page(offset)
    }

    fn append_page(&mut self, _: Page) -> Result<u64, BCDBError> {
        unimplemented!()
    }

    fn write_page(&mut self, offset: Offset, page: Page) -> Result<u64, BCDBError> {
        self.file.write_page(offset, page)
    }

    fn shutdown (&mut self) {}
}

/// a page of the hash table
#[derive(Clone)]
pub struct TablePage {
    pub page: Page,
    pub offset: Offset
}

impl From<Page> for TablePage {
    fn from(page: Page) -> Self {
        let offset = Offset::from(&page.payload[PAGE_SIZE-6 ..]);
        TablePage {page, offset}
    }
}

impl TablePage {
    /// create a new hash table page at offset
    pub fn new (offset: Offset) -> TablePage {
        let mut page = Page::new();
        page.payload[PAGE_SIZE - 6 ..].copy_from_slice(offset.to_vec().as_slice());
        TablePage {page, offset}
    }

    pub fn from_buf (payload: [u8; PAGE_SIZE]) -> TablePage {
        TablePage {page: Page::from_buf(payload), offset: Offset::from(&payload[PAGE_SIZE-6 ..])}
    }

    /// append some data
    /// will return Error::DoesNotFit if data does not fit into the page
    pub fn write (&mut self, pos: usize, data: & [u8]) -> Result<(), BCDBError> {
        self.page.write(pos, data)
    }

    /// write an offset
    pub fn write_offset (&mut self, pos: usize, offset: Offset) -> Result<(), BCDBError> {
        self.page.write_offset(pos, offset)
    }

    /// read some data
    /// will return Error::DoesNotFit if data does not fit into the page
    pub fn read (&self, pos: usize, data: &mut [u8]) -> Result<(), BCDBError> {
        self.page.read(pos, data)
    }

    /// read a stored offset
    pub fn read_offset(&self, pos: usize) -> Result<Offset, BCDBError> {
        self.page.read_offset(pos)
    }

    pub fn read_u64(&self, pos: usize) -> Result<u64, BCDBError> {
        self.page.read_u64(pos)
    }

    pub fn write_u64(&mut self, pos: usize, n: u64) -> Result<(), BCDBError> {
        self.page.write_u64(pos, n)
    }

    /// finish a page after appends to write out
    pub fn finish (&self) -> [u8; PAGE_SIZE] {
        self.page.payload
    }
}

struct BucketIterator<'a> {
    file: &'a TableFile,
    n: u32
}

impl<'a> Iterator for BucketIterator<'a> {
    type Item = Offset;

    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        let table_offset = TableFile::table_offset(self.n);
        if let Ok(Some(page)) = self.file.read_page(table_offset.this_page()) {
            self.n += 1;
            return Some(page.read_offset(table_offset.in_page_pos()).unwrap())
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
        let mut key_page = TablePage::new(Offset::from(4711));
        let payload: &[u8] = "hello world".as_bytes();
        key_page.write(0,payload).unwrap();
        let result = key_page.finish();

        let mut check = [0u8; PAGE_SIZE];
        check[0 .. payload.len()].copy_from_slice(payload);
        check[PAGE_SIZE -1] = (4711 % 256) as u8;
        check[PAGE_SIZE -2] = (4711 / 256) as u8;
        assert_eq!(hex::encode(&result[..]), hex::encode(&check[..]));

        let page2 = TablePage::from_buf(check);
        assert_eq!(key_page.offset, page2.offset);
        assert_eq!(hex::encode(&key_page.page.payload[..]), hex::encode(&page2.page.payload[..]));
    }
}
