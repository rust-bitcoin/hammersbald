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
use cache::Cache;

use std::sync::{Mutex, Arc, Condvar};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

pub const FIRST_PAGE_HEAD:usize = 28;
pub const BUCKETS_FIRST_PAGE:usize = 677;
pub const BUCKETS_PER_PAGE:usize = 681;
pub const BUCKET_SIZE: usize = 6;

/// The key file
pub struct TableFile {
    inner: Arc<TableFileInner>,
}


struct TableFileInner {
    file: Mutex<Box<PageFile>>,
    log: Arc<Mutex<LogFile>>,
    cache: Mutex<Cache>,
    flushed: Condvar,
    work: Condvar,
    run: AtomicBool,
    flushing: AtomicBool
}

impl TableFileInner {
    pub fn new (file: Box<PageFile>, log: Arc<Mutex<LogFile>>) -> Result<TableFileInner, BCDBError> {
        let len = file.len()?;
        Ok(TableFileInner { file: Mutex::new(file), log,
            cache: Mutex::new(Cache::new(len)), flushed: Condvar::new(),
            work: Condvar::new(),
            run: AtomicBool::new(true),
            flushing: AtomicBool::new(false)
        })
    }
}

impl TableFile {
    pub fn new (file: Box<PageFile>, log: Arc<Mutex<LogFile>>) -> Result<TableFile, BCDBError> {
        let inner = Arc::new(TableFileInner::new(file, log)?);
        let inner2 = inner.clone();
        thread::spawn(move || { TableFile::background(inner2) });
        Ok(TableFile { inner })
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

    fn background (inner: Arc<TableFileInner>) {
        while inner.run.load(Ordering::Relaxed) {
            let mut writes = Vec::new();
            {
                let cache = inner.cache.lock().expect("cache lock poisoned");
                let mut just_flushed = false;
                if cache.is_empty() {
                    inner.flushed.notify_all();
                    just_flushed = inner.flushing.swap(false, Ordering::AcqRel);
                }
                if !just_flushed {
                    // TODO: timeout is just a workaround here
                    let (mut cache, _) = inner.work.wait_timeout(cache, Duration::from_millis(100)).expect("cache lock poisoned while waiting for work");
                    if inner.flushing.load(Ordering::Acquire) || cache.new_writes > 1000 {
                        writes = cache.move_writes_to_wrote();
                    }
                }
            }
            if !writes.is_empty() {
                writes.sort_unstable_by(|a, b| u64::cmp(&a.0.as_u64(), &b.0.as_u64()));
                let mut log = inner.log.lock().expect("log lock poisoned");
                let mut file = inner.file.lock().expect("file lock poisoned");
                let mut logged = false;
                for write in &writes {
                    let offset = write.0;
                    if offset.as_u64() < log.tbl_len && !log.is_logged(offset) {
                        if let Some(page) = file.read_page(offset).expect("can not read hash table file") {
                            log.append_page(page).expect("can not write log file");
                            logged = true;
                        }
                            else {
                                panic!("can not find pre-image to log {}", offset);
                            }
                    }
                }
                if logged {
                    log.flush().expect("can not flush log");
                    log.sync().expect("can not sync log");
                }
                for (offset, page) in writes {
                    file.write_page(offset, page.clone()).expect("write hash table failed");
                }
            }
        }
    }

    pub fn patch_page(&mut self, page: TablePage) -> Result<u64, BCDBError> {
        self.inner.file.lock().unwrap().write_page(page.offset, page.page)
    }

    fn read_page_from_store (&self, offset: Offset) -> Result<Option<Page>, BCDBError> {
        if offset != offset.this_page () {
            return Err(BCDBError::Corrupted(format!("data or link read is not page aligned {}", offset)))
        }
        self.inner.file.lock().unwrap().read_page(offset)
    }

    pub fn log_file (&self) -> Arc<Mutex<LogFile>> {
        self.inner.log.clone()
    }

    pub fn shutdown (&mut self) {
        self.inner.run.store(false, Ordering::Relaxed);
        self.inner.work.notify_one();
    }

    pub fn clear_cache(&mut self, len: u64) {
        self.inner.cache.lock().unwrap().clear(len);
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

    pub fn write_key_page(&mut self, page: TablePage) -> Result<u64, BCDBError> {
        self.write_page(page.offset, page.page)
    }
}

impl PageFile for TableFile {
    #[allow(unused_assignments)]
    fn flush(&mut self) -> Result<(), BCDBError> {
        let mut cache = self.inner.cache.lock().unwrap();
        if !cache.is_empty() {
            self.inner.flushing.store(true, Ordering::Release);
            self.inner.work.notify_one();
            cache = self.inner.flushed.wait(cache)?;
        }
        self.inner.file.lock().unwrap().flush()
    }

    fn len(&self) -> Result<u64, BCDBError> {
        self.inner.file.lock().unwrap().len()
    }

    fn truncate(&mut self, new_len: u64) -> Result<(), BCDBError> {
        self.inner.file.lock().unwrap().truncate(new_len)
    }

    fn sync(&self) -> Result<(), BCDBError> {
        self.inner.file.lock().unwrap().sync()
    }

    fn read_page(&self, offset: Offset) -> Result<Option<Page>, BCDBError> {
        {
            let cache = self.inner.cache.lock().unwrap();
            if let Some(page) = cache.get(offset) {
                return Ok(Some(page));
            }
        }
        if let Some(page) = self.read_page_from_store(offset)? {
            // write cache takes precedence therefore no problem if there was
            // a write between above read and this lock
            let mut cache = self.inner.cache.lock().unwrap();
            cache.cache(offset, page.clone());
            return Ok(Some(page));
        }
        Ok(None)
    }

    fn append_page(&mut self, _: Page) -> Result<u64, BCDBError> {
        unimplemented!()
    }

    fn write_page(&mut self, offset: Offset, page: Page) -> Result<u64, BCDBError> {
        let len = self.inner.cache.lock().unwrap().write(offset, page);
        self.inner.work.notify_one();
        Ok(len)

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

