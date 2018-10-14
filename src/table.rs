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

use logfile::LogFile;
use page::{Page, TablePage, PageFile, PAGE_SIZE};
use error::BCDBError;
use offset::Offset;

use std::sync::{Mutex, Arc};
use std::cmp::max;

pub const FIRST_PAGE_HEAD:usize = 28;
pub const BUCKETS_FIRST_PAGE:usize = 677;
pub const BUCKETS_PER_PAGE:usize = 681;
pub const BUCKET_SIZE: usize = 6;

/// The key file
pub struct TableFile {
    file: Box<PageFile>,
    log: Arc<Mutex<LogFile>>
}

impl TableFile {
    pub fn new (file: Box<PageFile>, log: Arc<Mutex<LogFile>>) -> Result<TableFile, BCDBError> {
        Ok(TableFile {file, log})
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

    pub fn log_file (&self) -> Arc<Mutex<LogFile>> {
        self.log.clone()
    }

    pub fn read_key_page(&self, offset: Offset) -> Result<Option<TablePage>, BCDBError> {
        if let Some(page) = self.read_page(offset)? {
            let key_page = TablePage::from(page);
            if key_page.offset.as_u64() != offset.as_u64() {
                return Err(BCDBError::Corrupted(format!("hash table page {} does not have the offset of its position", offset)));
            }
            return Ok(Some(key_page));
        }
        Ok(None)
    }

    pub fn write_key_pages(&mut self, pages: Vec<TablePage>) -> Result<u64, BCDBError> {
        {
            let mut log = self.log.lock()?;
            for page in &pages {
                let offset = page.offset;
                if offset.as_u64() < log.tbl_len {
                    if let Some(page) = self.read_page(offset)? {
                        log.append_page(page)?;
                    } else {
                        return Err(BCDBError::Corrupted(format!("can not find pre-image to log {}", offset)));
                    }
                }
            }
            log.flush()?;
            log.sync()?;
        }
        let mut len = self.len()?;
        for page in pages {
            len = max(len, self.write_page(page.offset, page.page)?);
        }
        Ok(len)
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

