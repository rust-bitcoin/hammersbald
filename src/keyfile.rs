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

use logfile::LogFile;
use datafile::{DataFile, DataEntry};
use bcdb::PageFile;
use page::{Page, PAGE_SIZE};
use error::BCSError;
use types::Offset;
use cache::Cache;

use std::sync::{Mutex, Arc, Condvar};
use std::cell::Cell;
use std::thread;
use std::time::{Duration, Instant};
use std::cmp::Ordering;

const PAGE_HEAD :u64 = 12;
const INIT_BUCKETS: u64 = 128;
const INIT_LOGMOD :u64 = 6;
const BUCKETS_PER_PAGE :u64 = 226;
const BUCKET_SIZE: u64 = 18;

/// The key file
pub struct KeyFile {
    async_file: KeyPageFile,
    step: u64,
    buckets: u64,
    log_mod: u64
}

impl KeyFile {
    pub fn new(rw: Box<PageFile>, log_file: Arc<Mutex<LogFile>>) -> KeyFile {
        KeyFile{async_file: KeyPageFile::new(rw, log_file), step: 0, buckets: INIT_BUCKETS, log_mod: INIT_LOGMOD }
    }

    pub fn init (&mut self) -> Result<(), BCSError> {
        if let Ok(first_page) = self.read_page(Offset::new(0).unwrap()) {
            let buckets = first_page.read_offset(0).unwrap().as_u64();
            if buckets > 0 {
                self.buckets = buckets;
                self.step = first_page.read_offset(6).unwrap().as_u64();
                self.log_mod = (63 - buckets.leading_zeros()) as u64 - 1;
            }
            info!("open BCDB. buckets {}, step {}, log_mod {}", buckets, self.step, self.log_mod);
        }
        else {
            let mut fp = Page::new(Offset::new(0)?);
            fp.write_offset(0, Offset::new(self.buckets)?)?;
            fp.write_offset(6, Offset::new(self.step)?)?;
            self.write_page(fp)?;
            info!("open empty BCDB");
        };

        Ok(())
    }

    pub fn put (&mut self, key: &[u8], offset: Offset, data_file: &mut DataFile) -> Result<i32, BCSError>{
        let mut spill = 0;
        let hash = Self::hash(key);
        let mut bucket = hash & (!0u64 >> (64 - self.log_mod)); // hash % 2^(log_mod)
        let step = self.step;
        if bucket < step {
            bucket = hash & (!0u64 >> (64 - self.log_mod - 1)); // hash % 2^(log_mod + 1)
        }

        spill += self.store_to_bucket(bucket, key, offset, data_file)?;
        spill += self.rehash_bucket(step, data_file)?;

        self.step +=1;
        if self.step == (1 << (self.log_mod + 1))  {
            self.log_mod += 1;
            self.step = 0;
        }

        self.buckets += 1;
        if self.buckets % BUCKETS_PER_PAGE == 0 {
            let page = Page::new(Offset::new((self.buckets /BUCKETS_PER_PAGE)*PAGE_SIZE as u64)?);
            self.write_page(page)?;
        }

        if let Ok(mut first_page) = self.async_file.read_page(Offset::new(0).unwrap()) {
            first_page.write_offset(0, Offset::new(self.buckets)?)?;
            first_page.write_offset(6, Offset::new(self.step)?)?;
            self.async_file.write_page(first_page)?;
        }


        Ok(spill)
    }

    fn rehash_bucket(&mut self, bucket: u64, data_file: &mut DataFile) -> Result<i32, BCSError> {
        let mut spill = 0;
        let bucket_offset = Self::bucket_offset(bucket)?;

        loop {
            let mut bucket_page = self.read_page(bucket_offset.this_page())?;
            // first slot
            let data_offset = bucket_page.read_offset(bucket_offset.in_page_pos())?;
            if data_offset.as_u64() == 0 {
                break;
            }
            // get previously stored key
            if let Some(prev) = data_file.get(data_offset)? {
                let hash = Self::hash(prev.data_key.as_slice());
                let new_bucket = hash & (!0u64 >> (64 - self.log_mod - 1)); // hash % 2^(log_mod + 1)
                let mut write = false;
                if new_bucket != bucket {
                    // store to new bucket
                    spill += self.store_to_bucket(new_bucket, prev.data_key.as_slice(), data_offset, data_file)?;
                    bucket_page = self.read_page(bucket_offset.this_page())?;
                    // move second slot to first
                    let second = bucket_page.read_offset(bucket_offset.in_page_pos() + 6)?;
                    bucket_page.write_offset(bucket_offset.in_page_pos(), second)?;
                    write = true;
                }
                // second slot
                let data_offset = bucket_page.read_offset(bucket_offset.in_page_pos() + 6)?;
                if data_offset.as_u64() == 0 {
                    if write {
                        self.write_page(bucket_page)?;
                    }
                    break;
                }
                if let Some(prev) = data_file.get(data_offset)? {
                    let hash = Self::hash(prev.data_key.as_slice());
                    let new_bucket = hash & (!0u64 >> (64 - self.log_mod - 1)); // hash % 2^(log_mod + 1)
                    if new_bucket != bucket {
                        // store to new bucket
                        spill += self.store_to_bucket(new_bucket, prev.data_key.as_slice(), data_offset, data_file)?;
                        bucket_page = self.read_page(bucket_offset.this_page())?;
                        // source in first spill-over
                        let spillover = bucket_page.read_offset(bucket_offset.in_page_pos() + 12)?;
                        if spillover.as_u64() != 0 {
                            spill -= 1;
                            if let Ok(so) = data_file.get_spillover(spillover) {
                                bucket_page.write_offset(bucket_offset.in_page_pos() + 6, so.0)?;
                                bucket_page.write_offset(bucket_offset.in_page_pos() + 12, so.1)?;
                                self.write_page(bucket_page)?;
                            } else {
                                return Err(BCSError::Corrupted(format!("can not find previously stored spillover (1) {}", spillover.as_u64())));
                            }
                        } else {
                            bucket_page.write_offset(bucket_offset.in_page_pos() + 6, Offset::new(0)?)?;
                            self.write_page(bucket_page)?;
                            break;
                        }
                    }
                    else {
                        if write {
                            self.write_page(bucket_page)?;
                        }
                        break;
                    }
                }
            } else {
                return Err(BCSError::Corrupted(format!("can not find previously stored data (1) {}", data_offset.as_u64())));
            }
        }
        // rehash spillover chain
        let mut remaining_spillovers = Vec::new();
        let mut some_moved = false;
        let mut bucket_page = self.read_page(bucket_offset.this_page())?;
        let mut spillover = bucket_page.read_offset(bucket_offset.in_page_pos() + 12)?;
        while spillover.as_u64() != 0 {
            if let Ok(so) = data_file.get_spillover(spillover) {
                if let Some(prev) = data_file.get(so.0)? {
                    let hash = Self::hash(prev.data_key.as_slice());
                    let new_bucket = hash & (!0u64 >> (64 - self.log_mod - 1)); // hash % 2^(log_mod + 1)
                    if new_bucket != bucket {
                        spill -= 1;
                        spill += self.store_to_bucket(new_bucket, prev.data_key.as_slice(), so.0, data_file)?;
                        bucket_page = self.read_page(bucket_offset.this_page())?;
                        some_moved = true;
                    }
                    else {
                        remaining_spillovers.push(so.0);
                    }
                }
                else {
                    return Err(BCSError::Corrupted(format!("can not find previously stored data (6) {}", so.0.as_u64())));
                }
                spillover = so.1;
            } else {
                return Err(BCSError::Corrupted(format!("can not find previously stored spillover (2) {}", spillover.as_u64())));
            }
        }
        if some_moved {
            let mut prev = Offset::new(0).unwrap();
            for offset in remaining_spillovers.iter().rev() {
                let so = data_file.append(DataEntry::new_spillover(*offset, prev))?;
                prev = so;
            }
            bucket_page.write_offset(bucket_offset.in_page_pos() + 12, prev)?;
            self.write_page(bucket_page)?;
        }


        Ok(spill)
    }

    fn store_to_bucket(&mut self, bucket: u64, key: &[u8], offset: Offset, data_file: &mut DataFile) -> Result<i32, BCSError> {
        let mut spill = 0;
        let bucket_offset = Self::bucket_offset(bucket)?;
        let mut bucket_page = self.read_page(bucket_offset.this_page())?;
        let data_offset = bucket_page.read_offset(bucket_offset.in_page_pos())?;
        if data_offset.as_u64() == 0 {
            // empty slot, just store data
            bucket_page.write_offset(bucket_offset.in_page_pos(), offset)?;
        } else {
            // check if this is overwrite of same key
            if let Some(prev) = data_file.get(data_offset)? {
                if prev.data_key == key {
                    // point to new data
                    bucket_page.write_offset(bucket_offset.in_page_pos(), offset)?;
                } else {
                    // second slot
                    let data_offset = bucket_page.read_offset(bucket_offset.in_page_pos() + 6)?;
                    if data_offset.as_u64() == 0 {
                        // empty slot, just store data
                        bucket_page.write_offset(bucket_offset.in_page_pos() + 6, offset)?;
                    }
                    else {
                        // check if this is overwrite of same key
                        if let Some(prev) = data_file.get(data_offset)? {
                            if prev.data_key == key {
                                // point to new data
                                bucket_page.write_offset(bucket_offset.in_page_pos() + 6, offset)?;
                            } else {
                                spill += 1;
                                // prepend spillover chain
                                // this logically overwrites previous key association in the spillover chain
                                // since search stops at first key match
                                let spillover = bucket_page.read_offset(bucket_offset.in_page_pos() + 12)?;
                                let so = data_file.append(DataEntry::new_spillover(offset, spillover))?;
                                bucket_page.write_offset(bucket_offset.in_page_pos() + 12, so)?;
                            }
                        } else {
                            // can not find previously stored data
                            return Err(BCSError::Corrupted(format!("can not find previously stored data (4) {}", data_offset.as_u64())));
                        }
                    }
                }
            } else {
                // can not find previously stored data
                return Err(BCSError::Corrupted(format!("can not find previously stored data (4) {}", data_offset.as_u64())));
            }
        }
        self.write_page(bucket_page)?;
        Ok(spill)
    }

    pub fn get (&self, key: &[u8], data_file: &DataFile) -> Result<Option<Vec<u8>>, BCSError> {
        let hash = Self::hash(key);
        let mut bucket = hash & (!0u64 >> (64 - self.log_mod)); // hash % 2^(log_mod)
        if bucket < self.step {
            bucket = hash & (!0u64 >> (64 - self.log_mod - 1)); // hash % 2^(log_mod + 1)
        }
        // first slot
        let bucket_offset = Self::bucket_offset(bucket)?;
        let bucket_page = self.read_page(bucket_offset.this_page())?;
        let data_offset = bucket_page.read_offset(bucket_offset.in_page_pos())?;
        if data_offset.as_u64() == 0 {
            // assuming that second slot is also empty
            return Ok(None);
        }
        if let Some(prev) = data_file.get(data_offset)? {
            if prev.data_key == key {
                return Ok(Some(prev.data));
            }
            // second slot
            let data_offset = bucket_page.read_offset(bucket_offset.in_page_pos() + 6)?;
            if data_offset.as_u64() != 0 {
                if let Some(prev) = data_file.get(data_offset)? {
                    if prev.data_key == key {
                        return Ok(Some(prev.data));
                    }
                } else {
                    return Err(BCSError::Corrupted(format!("can not find previously stored data (5) {}", data_offset.as_u64())));
                }
            }
            // spill-over
            let mut spillover = bucket_page.read_offset(bucket_offset.in_page_pos() + 12)?;
            while spillover.as_u64() != 0 {
                if let Ok(so) = data_file.get_spillover(spillover) {
                    if let Some(prev) = data_file.get(so.0)? {
                        if prev.data_key == key {
                            return Ok(Some(prev.data));
                        }
                        spillover = so.1;
                    }
                    else {
                        return Err(BCSError::Corrupted(format!("can not find previously stored spillover (3) {}", so.0.as_u64())));
                    }
                }
                else {
                    return Err(BCSError::Corrupted(format!("can not find previously stored spillover (4) {}", spillover.as_u64())));
                }
            }
            return Ok(None);
        }
        else {
            return Err(BCSError::Corrupted(format!("can not find previously stored data (5) {}", data_offset.as_u64())));
        }
    }

    pub fn patch_page(&mut self, page: Page) -> Result<(), BCSError> {
        self.async_file.patch_page(page)
    }

    pub fn clear_cache(&mut self) {
        self.async_file.clear_cache();
    }

    pub fn log_file (&self) -> Arc<Mutex<LogFile>> {
        self.async_file.log_file()
    }

    pub fn shutdown (&mut self) {
        self.async_file.shutdown()
    }

    fn bucket_offset (bucket: u64) -> Result<Offset, BCSError> {
        Offset::new ((bucket / BUCKETS_PER_PAGE) * PAGE_SIZE as u64
                         + (bucket % BUCKETS_PER_PAGE) * BUCKET_SIZE + PAGE_HEAD)
    }

    // assuming that key is already a good hash
    fn hash (key: &[u8]) -> u64 {
        use std::mem::transmute;

        let mut buf = [0u8; 8];
        buf.copy_from_slice(&key[0 .. 8]);
        u64::from_be(unsafe { transmute(buf)})
    }
}

impl PageFile for KeyFile {
    fn flush(&mut self) -> Result<(), BCSError> {
        self.async_file.flush()
    }

    fn len(&self) -> Result<u64, BCSError> {
        self.async_file.len()
    }

    fn truncate(&mut self, len: u64) -> Result<(), BCSError> {
        self.async_file.truncate(len)
    }

    fn sync(&self) -> Result<(), BCSError> {
        self.async_file.sync()
    }

    fn read_page(&self, offset: Offset) -> Result<Page, BCSError> {
        self.async_file.read_page(offset)
    }

    fn append_page(&mut self, _: Page) -> Result<(), BCSError> {
        unimplemented!()
    }

    fn write_page(&mut self, page: Page) -> Result<(), BCSError> {
        self.async_file.write_page(page)
    }
}

struct KeyPageFile {
    inner: Arc<KeyPageFileInner>
}

struct KeyPageFileInner {
    file: Mutex<Box<PageFile>>,
    log: Arc<Mutex<LogFile>>,
    cache: Mutex<Cache>,
    flushed: Condvar,
    work: Condvar,
    run: Mutex<Cell<bool>>
}

impl KeyPageFileInner {
    pub fn new (file: Box<PageFile>, log: Arc<Mutex<LogFile>>) -> KeyPageFileInner {
        KeyPageFileInner { file: Mutex::new(file), log,
            cache: Mutex::new(Cache::default()), flushed: Condvar::new(), work: Condvar::new(), run: Mutex::new(Cell::new(true)) }
    }

    fn read_page_from_store (&self, offset: Offset) -> Result<Page, BCSError> {
        self.file.lock().unwrap().read_page(offset)
    }
}

impl KeyPageFile {
    pub fn new (file: Box<PageFile>, log: Arc<Mutex<LogFile>>) -> KeyPageFile {
        let inner = Arc::new(KeyPageFileInner::new(file, log));
        let inner2 = inner.clone();
        thread::spawn(move || { KeyPageFile::background(inner2) });
        KeyPageFile { inner }
    }

    fn background (inner: Arc<KeyPageFileInner>) {
        let mut run = true;
        let mut last_loop;
        while run {
            let mut writes;
            last_loop = Instant::now();
            {
                loop {
                    {
                        let mut cache = inner.cache.lock().expect("cache lock poisoned");
                        while run && cache.is_empty() {
                            inner.flushed.notify_all();
                            cache = inner.work.wait(cache).expect("cache lock poisoned while waiting for work");
                            run = inner.run.lock().expect("run lock poisoned").get();
                        }

                        if cache.writes_len() > 1000 || (Instant::now() - last_loop).cmp(&Duration::from_millis(500)) == Ordering::Greater {
                            writes = cache.move_writes_to_wrote();
                            break;
                        }
                    }
                    if run {
                        thread::sleep(Duration::from_millis(10));
                    }
                }
            }
            writes.sort_unstable_by(|a, b| u64::cmp(&a.offset.as_u64(), &b.offset.as_u64()));
            let mut log = inner.log.lock().expect("log lock poisoned");
            let mut log_write = false;
            for page in &writes {
                if page.offset.as_u64() < log.tbl_len && !log.has_page(page.offset) {
                    if let Ok(prev) = inner.read_page_from_store(page.offset) {
                        log.append_page(prev).expect("can not write log");
                        log_write = true;
                    }
                }
            }
            if log_write {
                log.flush().expect("can not flush log");
                log.sync().expect("can not sync log");
            }
            if !writes.is_empty() {
                use std::ops::Deref;

                let mut file = inner.file.lock().expect("file lock poisoned");
                for page in &writes {
                    file.write_page(page.deref().clone()).expect("can not write key file");
                }
            }
        }
    }

    pub fn patch_page(&mut self, page: Page) -> Result<(), BCSError> {
        self.inner.file.lock().unwrap().write_page(page)
    }

    fn read_page_from_store (&self, offset: Offset) -> Result<Page, BCSError> {
        self.inner.file.lock().unwrap().read_page(offset)
    }

    pub fn log_file (&self) -> Arc<Mutex<LogFile>> {
        self.inner.log.clone()
    }

    pub fn shutdown (&mut self) {
        self.inner.run.lock().unwrap().set(false);
        self.inner.work.notify_one();
    }

    pub fn clear_cache(&mut self) {
        self.inner.cache.lock().unwrap().clear();
    }
}

impl PageFile for KeyPageFile {
    #[allow(unused_assignments)]
    fn flush(&mut self) -> Result<(), BCSError> {
        let mut cache = self.inner.cache.lock().unwrap();
        self.inner.work.notify_one();
        cache = self.inner.flushed.wait(cache)?;
        self.inner.file.lock().unwrap().flush()
    }

    fn len(&self) -> Result<u64, BCSError> {
        self.inner.file.lock().unwrap().len()
    }

    fn truncate(&mut self, new_len: u64) -> Result<(), BCSError> {
        self.inner.file.lock().unwrap().truncate(new_len)
    }

    fn sync(&self) -> Result<(), BCSError> {
        self.inner.file.lock().unwrap().sync()
    }

    fn read_page(&self, offset: Offset) -> Result<Page, BCSError> {

        use std::ops::Deref;

        {
            let cache = self.inner.cache.lock().unwrap();
            if let Some(page) = cache.get(offset) {
                return Ok(page.deref().clone());
            }
        }

        // read outside of cache lock
        let page = self.read_page_from_store(offset)?;

        {
            // write cache takes precedence, therefore insert of outdated read will be ignored
            let mut cache = self.inner.cache.lock().unwrap();
            cache.cache(page.clone());
        }

        Ok(page)
    }

    fn append_page(&mut self, _: Page) -> Result<(), BCSError> {
        unimplemented!()
    }

    fn write_page(&mut self, page: Page) -> Result<(), BCSError> {
        self.inner.cache.lock().unwrap().write(page);
        self.inner.work.notify_one();
        Ok(())
    }
}
