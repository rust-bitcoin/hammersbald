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

use rand::{thread_rng, RngCore};
use siphasher::sip::SipHasher;

use std::sync::{Mutex, Arc, Condvar};
use std::cell::Cell;
use std::thread;
use std::time::{Duration, Instant};
use std::hash::Hasher;
use std::cmp::Ordering;

const PAGE_HEAD :u64 = 28;
const INIT_BUCKETS: u64 = 128;
const INIT_LOGMOD :u64 = 6;
const BUCKETS_PER_PAGE :u64 = 225;
const BUCKET_SIZE: u64 = 18;

/// The key file
pub struct KeyFile {
    async_file: KeyPageFile,
    step: u64,
    buckets: u64,
    log_mod: u64,
    sip0: u64,
    sip1: u64
}

impl KeyFile {
    pub fn new(rw: Box<PageFile>, log_file: Arc<Mutex<LogFile>>) -> KeyFile {
        let mut rng = thread_rng();
        KeyFile{async_file: KeyPageFile::new(rw, log_file), step: 0,
            buckets: INIT_BUCKETS, log_mod: INIT_LOGMOD,
        sip0: rng.next_u64(), sip1: rng.next_u64() }
    }

    pub fn init (&mut self) -> Result<(), BCSError> {
        if let Ok(first_page) = self.read_page(Offset::new(0).unwrap()) {
            let buckets = first_page.read_offset(0).unwrap().as_u64();
            if buckets > 0 {
                self.buckets = buckets;
                self.step = first_page.read_offset(6).unwrap().as_u64();
                self.log_mod = (63 - buckets.leading_zeros()) as u64 - 1;
                self.sip0 = first_page.read_u64(12).unwrap();
                self.sip1 = first_page.read_u64(20).unwrap();
            }
            info!("open BCDB. buckets {}, step {}, log_mod {}", buckets, self.step, self.log_mod);
        }
        else {
            let page = Page::new(Offset::new(0)?);
            let mut fp = LoggedPage { preimage: page.clone(), page};
            fp.write_offset(0, Offset::new(self.buckets)?)?;
            fp.write_offset(6, Offset::new(self.step)?)?;
            fp.write_u64(12, self.sip0)?;
            fp.write_u64(20, self.sip1)?;
            self.write_page(fp)?;
            info!("open empty BCDB");
        };

        Ok(())
    }

    pub fn put (&mut self, key: &[u8], offset: Offset, data_file: &mut DataFile) -> Result<i32, BCSError>{
        let mut spill = 0;
        let hash = self.hash(key);
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
            let lp = LoggedPage { preimage: page.clone(), page };
            self.write_page(lp)?;
        }

        if let Ok(mut first_page) = self.read_page(Offset::new(0).unwrap()) {
            first_page.write_offset(0, Offset::new(self.buckets)?)?;
            first_page.write_offset(6, Offset::new(self.step)?)?;
            self.write_page(first_page)?;
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
                let hash = self.hash(prev.data_key.as_slice());
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
                    let hash = self.hash(prev.data_key.as_slice());
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
                    let hash = self.hash(prev.data_key.as_slice());
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
        let hash = self.hash(key);
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

    fn hash (&self, key: &[u8]) -> u64 {
        let mut hasher = SipHasher::new_with_keys(self.sip0, self.sip1);
        hasher.write(key);
        hasher.finish()
    }
}

impl KeyFile {
    pub fn flush(&mut self) -> Result<(), BCSError> {
        self.async_file.flush()
    }

    pub fn len(&self) -> Result<u64, BCSError> {
        self.async_file.len()
    }

    pub fn truncate(&mut self, len: u64) -> Result<(), BCSError> {
        self.async_file.truncate(len)
    }

    pub fn sync(&self) -> Result<(), BCSError> {
        self.async_file.sync()
    }

    fn read_page(&self, offset: Offset) -> Result<LoggedPage, BCSError> {
        let page = self.async_file.read_page(offset)?;
        Ok(LoggedPage { preimage: page.clone(), page })
    }

    fn write_page(&mut self, page: LoggedPage) -> Result<(), BCSError> {
        self.async_file.inner.log.lock().unwrap().preimage(page.preimage);
        self.async_file.write_page(page.page)
    }
}

struct LoggedPage {
    preimage: Page,
    page: Page
}

impl LoggedPage {

    pub fn write_offset (&mut self, pos: usize, offset: Offset) -> Result<(), BCSError> {
        self.page.write_offset(pos, offset)
    }

    pub fn read_offset(&self, pos: usize) -> Result<Offset, BCSError> {
        self.page.read_offset(pos)
    }

    pub fn read_u64(&self, pos: usize) -> Result<u64, BCSError> {
        self.page.read_u64(pos)
    }

    pub fn write_u64(&mut self, pos: usize, n: u64) -> Result<(), BCSError> {
        self.page.write_u64(pos, n)
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
        let mut last_loop= Instant::now();
        while run {
            run = inner.run.lock().expect("run lock poisoned").get();
            let mut writes;
            loop {
                let mut cache = inner.cache.lock().expect("cache lock poisoned");
                if cache.is_empty() {
                    inner.flushed.notify_all();
                }
                else {
                    if cache.writes_len() > 1000 {
                        writes = cache.move_writes_to_wrote();
                        break;
                    }
                }
                let time_spent = Instant::now() - last_loop;
                if time_spent.cmp(&Duration::from_millis(2000)) == Ordering::Greater {
                    writes = cache.move_writes_to_wrote();
                    break;
                }
                else {
                    let (c, t) = inner.work.wait_timeout(cache, Duration::from_millis(2000) - time_spent).expect("cache lock poisoned while waiting for work");
                    if t.timed_out() {
                        cache = c;
                        writes = cache.move_writes_to_wrote();
                        break;
                    }
                }
            }
            last_loop = Instant::now();
            if !writes.is_empty() {
                {
                    let mut log = inner.log.lock().expect("log lock poisoned");
                    log.flush().expect("can not flush log");
                    log.sync().expect("can not sync log");
                }

                use std::ops::Deref;
                writes.sort_unstable_by(|a, b| u64::cmp(&a.offset.as_u64(), &b.offset.as_u64()));
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
        if !cache.is_empty() {
            self.inner.work.notify_one();
            cache = self.inner.flushed.wait(cache)?;
        }
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
