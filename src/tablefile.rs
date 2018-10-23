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

use page::{Page, PAGE_SIZE, PAGE_PAYLOAD_SIZE};
use pagedfile::PagedFile;
use error::HammersbaldError;
use pref::PRef;

pub const FIRST_PAGE_HEAD:usize = 28;
pub const BUCKET_SIZE: usize = 6;
pub const BUCKETS_PER_PAGE:usize = PAGE_PAYLOAD_SIZE/BUCKET_SIZE;
pub const BUCKETS_FIRST_PAGE:usize = (PAGE_PAYLOAD_SIZE - FIRST_PAGE_HEAD)/BUCKET_SIZE;

/// The key file
pub struct TableFile {
    file: Box<PagedFile>
}

impl TableFile {
    pub fn new (file: Box<PagedFile>) -> Result<TableFile, HammersbaldError> {
        Ok(TableFile {file})
    }

    pub fn table_offset (bucket: usize) -> PRef {
        if (bucket as u64) < BUCKETS_FIRST_PAGE as u64 {
            PRef::from((bucket * BUCKET_SIZE + FIRST_PAGE_HEAD) as u64)
        }
        else {
            PRef::from(
                ((bucket - BUCKETS_FIRST_PAGE)/BUCKETS_PER_PAGE + 1) as u64 * PAGE_SIZE as u64
                + (bucket % BUCKETS_PER_PAGE) as u64 * BUCKET_SIZE as u64)
        }
    }

    pub fn iter<'a>(&'a self) -> impl Iterator<Item=PRef> +'a {
        BucketIterator{file: self, n:0}
    }
}

impl PagedFile for TableFile {
    fn flush(&mut self) -> Result<(), HammersbaldError> {
        self.file.flush()
    }

    fn len(&self) -> Result<u64, HammersbaldError> {
        self.file.len()
    }

    fn truncate(&mut self, new_len: u64) -> Result<(), HammersbaldError> {
        self.file.truncate(new_len)
    }

    fn sync(&self) -> Result<(), HammersbaldError> {
        self.file.sync()
    }

    fn shutdown (&mut self) {}

    fn read_page(&self, pref: PRef) -> Result<Option<Page>, HammersbaldError> {
        if let Some(page) = self.file.read_page(pref)? {
            if page.pref() != pref {
                return Err(HammersbaldError::Corrupted(format!("table page {} does not have the pref of its position", pref)));
            }
            return Ok(Some(page));
        }
        Ok(None)
    }

    fn append_page(&mut self, _: Page) -> Result<(), HammersbaldError> {
        unimplemented!()
    }
    fn update_page(&mut self, page: Page) -> Result<u64, HammersbaldError> {
        self.file.update_page(page)
    }
}

struct BucketIterator<'a> {
    file: &'a TableFile,
    n: usize
}

impl<'a> Iterator for BucketIterator<'a> {
    type Item = PRef;

    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        let table_offset = TableFile::table_offset(self.n);
        if let Ok(Some(page)) = self.file.read_page(table_offset.this_page()) {
            self.n += 1;
            return Some(page.read_pref(table_offset.in_page_pos()))
        }
        None
    }
}

