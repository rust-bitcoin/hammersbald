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
//! # The key file
//! Specific implementation details to key file
//!

use asyncfile::AsyncFile;
use logfile::LogFile;
use datafile::{DataFile, DataEntry};
use bcdb::{RW, DBFile, PageIterator, PageFile,KEY_LEN};
use page::{Page, PAGE_SIZE};
use error::BCSError;
use types::Offset;

use std::sync::{Mutex, Arc};
use std::ops::Deref;

const BUCKETS_PER_PAGE :u64 = 340;
const BUCKET_SIZE: u64 = 12;

/// The key file
pub struct KeyFile {
    async_file: AsyncFile,
    buckets: u64,
    log_mod: u64,
    step: u64
}

impl KeyFile {
    pub fn new(rw: Box<RW>, log_file: Arc<Mutex<LogFile>>) -> KeyFile {
        KeyFile{async_file: AsyncFile::new(rw, Some(log_file)), buckets: 256, log_mod: 7, step: 0 }
    }

    pub fn init () -> Result<(), BCSError> {
        Ok(())
    }

    pub fn put (&mut self, key: &[u8], offset: Offset, data_file: &mut DataFile) -> Result<(), BCSError>{
        let hash = Self::hash(key);
        let mut bucket = hash & (!0u64 >> (64 - self.log_mod)); // hash % 2^(log_mod)
        if bucket < self.step {
            bucket = hash & (!0u64 >> (64 - self.log_mod - 1)); // hash % 2^(log_mod + 1)
        }
        self.store_to_bucket(bucket, key, offset, data_file)?;
        bucket = self.step;
        self.rehash_bucket(bucket, data_file)?;

        if self.step == 1 << self.log_mod {
            self.step = 0;
            self.log_mod += 1;
        }
        else {
            self.step += 1;
        }
        if self.buckets % BUCKETS_PER_PAGE == 0 {
            let page = Page::new(Offset::new((self.buckets/BUCKETS_PER_PAGE + 1) as usize)?);
            self.write_page(Arc::new(page));
        }
        self.buckets += 1;
        Ok(())
    }

    fn rehash_bucket(&mut self, bucket: u64, data_file: &mut DataFile) -> Result<(), BCSError> {
        let bucket_offset = Self::bucket_offset(bucket)?;
        let mut bucket_page = self.read_page(bucket_offset.this_page())?.deref().clone();
        loop {
            let data_offset = bucket_page.read_offset(bucket_offset.in_page_pos())?;
            if data_offset.as_usize() == 0 {
                // nothing to do for an empty bucket
                return Ok(());
            }
            // get previously stored key
            if let Some(prev) = data_file.get(data_offset)? {
                let hash = Self::hash(prev.data_key.as_slice());
                let new_bucket = hash & (!0u64 >> (64 - self.log_mod - 1)); // hash % 2^(log_mod + 1)
                if new_bucket != bucket {
                    // store to new bucket
                    self.store_to_bucket(new_bucket, prev.data_key.as_slice(), data_offset, data_file)?;
                    // source in first spill-over
                    let mut spillover = bucket_page.read_offset(bucket_offset.in_page_pos() + 6)?;
                    if spillover.as_usize() != 0 {
                        if let Ok(so) = data_file.get_spillover(spillover) {
                            bucket_page.write_offset(bucket_offset.in_page_pos(), so.0)?;
                            bucket_page.write_offset(bucket_offset.in_page_pos() + 6, so.1)?;
                        } else {
                            // can not find previously stored spillover
                            return Err(BCSError::Corrupted);
                        }
                    }
                }
                else {
                    break;
                }
            } else {
                // can not find previously stored data
                return Err(BCSError::Corrupted);
            }
        }
        let mut spillover = bucket_page.read_offset(bucket_offset.in_page_pos() + 6)?;
        while spillover.as_usize() != 0 {
            if let Ok(so) = data_file.get_spillover(spillover) {
                if let Some(prev) = data_file.get(so.0)? {
                    let hash = Self::hash(prev.data_key.as_slice());
                    let new_bucket = hash & (!0u64 >> (64 - self.log_mod - 1)); // hash % 2^(log_mod + 1)
                    if new_bucket != bucket {
                        // store to new bucket
                        self.store_to_bucket(new_bucket, prev.data_key.as_slice(), so.0, data_file)?;
                    }
                    // rehash next
                    spillover = so.1;
                } else {
                    // can not find previously stored data
                    return Err(BCSError::Corrupted);
                }
            } else {
                // can not find previously stored spillover
                return Err(BCSError::Corrupted);
            }
        }
        Ok(())
    }

    fn store_to_bucket(&mut self, bucket: u64, key: &[u8], offset: Offset, data_file: &mut DataFile) -> Result<(), BCSError> {
        let bucket_offset = Self::bucket_offset(bucket)?;
        let mut bucket_page = self.read_page(bucket_offset.this_page())?.deref().clone();
        let data_offset = bucket_page.read_offset(bucket_offset.in_page_pos())?;
        if data_offset.as_usize() == 0 {
            // empty bucket, just store data
            bucket_page.write_offset(bucket_offset.in_page_pos(), offset)?;
        } else {
            // check if this is overwrite of same key
            if let Some(prev) = data_file.get(data_offset)? {
                if prev.data_key == key {
                    // point to new data
                    bucket_page.write_offset(bucket_offset.in_page_pos(), offset)?;
                } else {
                    // prepend spillover chain
                    // this logically overwrites previous key association in the spillover chain
                    // since search stops at first key match
                    let spillover = bucket_page.read_offset(bucket_offset.in_page_pos() + 6)?;
                    let so = data_file.append(DataEntry::new_spillover(offset, spillover))?;
                    bucket_page.write_offset(bucket_offset.in_page_pos() + 6, so)?;
                }
            } else {
                // can not find previously stored data
                return Err(BCSError::Corrupted);
            }
        }
        self.write_page(Arc::new(bucket_page));
        Ok(())
    }

    pub fn get (&self, key: &[u8], data_file: &DataFile) -> Result<Option<Vec<u8>>, BCSError> {
        let hash = Self::hash(key);
        let mut bucket = hash & (!0u64 >> (64 - self.log_mod)); // hash % 2^(log_mod)
        if bucket <= self.step {
            bucket = hash & (!0u64 >> (64 - self.log_mod - 1)); // hash % 2^(log_mod + 1)
        }
        let bucket_offset = Self::bucket_offset(bucket)?;
        let bucket_page = self.read_page(bucket_offset.this_page())?.deref().clone();
        let data_offset = bucket_page.read_offset(bucket_offset.in_page_pos())?;
        if data_offset.as_usize() == 0 {
            return Ok(None);
        }
        if let Some(prev) = data_file.get(data_offset)? {
            if prev.data_key == key {
                return Ok(Some(prev.data));
            }
            else {
                let mut spillover = bucket_page.read_offset(bucket_offset.in_page_pos() + 6)?;
                while spillover.as_usize() != 0 {
                    if let Ok(so) = data_file.get_spillover(spillover) {
                        if let Some(prev) = data_file.get(so.0)? {
                            if prev.data_key == key {
                                return Ok(Some(prev.data));
                            }
                            spillover = so.1;
                        }
                        else {
                            // can not find previously stored data
                            return Err(BCSError::Corrupted);
                        }
                    }
                    else {
                        // can not find previously stored spillover
                        return Err(BCSError::Corrupted);
                    }
                }
            }
        }
        else {
            // can not find previously stored data
            return Err(BCSError::Corrupted);
        }
        return Ok(None)
    }

    pub fn write_page(&self, page: Arc<Page>) {
        self.async_file.write_page(page)
    }

    pub fn log_file (&self) -> Arc<Mutex<LogFile>> {
        self.async_file.log_file().unwrap()
    }

    pub fn shutdown (&mut self) {
        self.async_file.shutdown()
    }

    fn bucket_offset (bucket: u64) -> Result<Offset, BCSError> {
        Offset::new (((bucket * BUCKET_SIZE / BUCKETS_PER_PAGE + 1) * PAGE_SIZE as u64
                         + (bucket % BUCKETS_PER_PAGE) * BUCKET_SIZE) as usize)
    }

    // assuming that key is already a good hash
    fn hash (key: &[u8]) -> u64 {
        use std::mem::transmute;

        let mut buf = [0u8; 8];
        buf.copy_from_slice(&key[KEY_LEN-8 .. KEY_LEN]);
        u64::from_be(unsafe { transmute(buf)})
    }
}

impl DBFile for KeyFile {
    fn flush(&mut self) -> Result<(), BCSError> {
        self.async_file.flush()
    }

    fn sync(&mut self) -> Result<(), BCSError> {
        self.async_file.sync()
    }

    fn truncate(&mut self, offset: Offset) -> Result<(), BCSError> {
        self.async_file.truncate(offset)
    }

    fn len(&mut self) -> Result<Offset, BCSError> {
        self.async_file.len()
    }
}

impl PageFile for KeyFile {
    fn read_page(&self, offset: Offset) -> Result<Arc<Page>, BCSError> {
        self.async_file.read_page(offset)
    }
}