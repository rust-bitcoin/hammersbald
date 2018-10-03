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
use datafile::{DataFile, Content};
use linkfile::LinkFile;
use keyfile::KeyFile;
use page::{Page, TablePage, PageFile, PAGE_SIZE};
use error::BCDBError;
use types::Offset;
use cache::Cache;

use rand::{thread_rng, RngCore};
use siphasher::sip::SipHasher;

use std::sync::{Mutex, Arc, Condvar};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::hash::Hasher;
use std::collections::HashMap;

const FIRST_PAGE_HEAD:u64 = 28;
const INIT_BUCKETS: u64 = 512;
const INIT_LOGMOD :u64 = 8;
const FIRST_BUCKETS_PER_PAGE:u64 = 677;
const BUCKETS_PER_PAGE:u64 = 681;
const BUCKET_SIZE: u64 = 6;

/// The key file
pub struct TableFile {
    async_file: TablePageFile,
    step: u32,
    buckets: u32,
    log_mod: u32,
    sip0: u64,
    sip1: u64
}

impl TableFile {
    pub fn new(rw: Box<PageFile>, log_file: Arc<Mutex<LogFile>>) -> Result<TableFile, BCDBError> {
        let mut rng = thread_rng();
        Ok(TableFile {async_file: TablePageFile::new(rw, log_file)?, step: 0,
            buckets: INIT_BUCKETS as u32, log_mod: INIT_LOGMOD as u32,
        sip0: rng.next_u64(), sip1: rng.next_u64() })
    }

    pub fn init (&mut self) -> Result<(), BCDBError> {
        if let Some(first_page) = self.read_page(Offset::from(0))? {
            let buckets = first_page.read_offset(0).unwrap().as_u64() as u32;
            if buckets > 0 {
                self.buckets = buckets;
                self.step = first_page.read_offset(6).unwrap().as_u64() as u32;
                self.log_mod = (32 - buckets.leading_zeros()) as u32 - 2;
                self.sip0 = first_page.read_u64(12).unwrap();
                self.sip1 = first_page.read_u64(20).unwrap();
            }
            info!("open BCDB. buckets {}, step {}, log_mod {}", buckets, self.step, self.log_mod);
        }
        else {
            let mut fp = TablePage::new(Offset::from(0));
            fp.write_offset(0, Offset::from(self.buckets as u64))?;
            fp.write_offset(6, Offset::from(self.step as u64))?;
            fp.write_u64(12, self.sip0)?;
            fp.write_u64(20, self.sip1)?;
            self.write_page(fp)?;
            info!("open empty BCDB");
        };

        Ok(())
    }

    pub fn put (&mut self, keys: Vec<Vec<u8>>, data_offset: Offset, link_file: &mut LinkFile, key_file: &mut KeyFile) -> Result<(), BCDBError>{
        for key in keys {
            let hash = self.hash(key.as_slice());
            let mut bucket = hash & (!0u32 >> (32 - self.log_mod)); // hash % 2^(log_mod)
            if bucket < self.step {
                bucket = hash & (!0u32 >> (32 - self.log_mod - 1)); // hash % 2^(log_mod + 1)
            }
            let mut key_offset= key_file.append_key(key.as_slice(), data_offset)?;
            self.store_to_bucket(bucket, hash, key_offset, link_file)?;
        }

        if thread_rng().next_u32() % 16 == 0 && self.step < <u32>::max_value() {

            if self.step < (1 << self.log_mod) {
                let step = self.step;
                self.rehash_bucket(step, link_file)?;
            }

            self.step += 1;
            if self.step > (1 << (self.log_mod + 1)) && (self.buckets - FIRST_BUCKETS_PER_PAGE as u32) % BUCKETS_PER_PAGE as u32 == 0 {
                self.log_mod += 1;
                self.step = 0;
            }

            self.buckets += 1;
            if self.buckets as u64 >= FIRST_BUCKETS_PER_PAGE && (self.buckets as u64 - FIRST_BUCKETS_PER_PAGE) % BUCKETS_PER_PAGE == 0 {
                let page = TablePage::new(Offset::from(((self.buckets as u64 - FIRST_BUCKETS_PER_PAGE)/ BUCKETS_PER_PAGE + 1) * PAGE_SIZE as u64));
                self.write_page(page)?;
            }

            if let Some(mut first_page) = self.read_page(Offset::from(0))? {
                first_page.write_offset(0, Offset::from(self.buckets as u64))?;
                first_page.write_offset(6, Offset::from(self.step as u64))?;
                self.write_page(first_page)?;
            }
        }

        Ok(())
    }

    fn rehash_bucket(&mut self, bucket: u32, link_file: &mut LinkFile) -> Result<(), BCDBError> {
        let table_offset = Self::table_offset(bucket);

        if let Some(mut table_page) = self.read_page(table_offset.this_page())? {
            let mut link_offset = table_page.read_offset(table_offset.in_page_pos())?;

            let mut current_links = Vec::new();
            let mut rewrite = false;
            loop {
                if !link_offset.is_valid() {
                    break;
                }
                match link_file.get_content(link_offset)? {
                    Some(Content::Link(links, next)) => {
                        current_links.extend(links);
                        link_offset = next;
                        if link_offset.is_valid() {
                            rewrite = true;
                        }
                    },
                    _ => return Err(BCDBError::Corrupted(format!("expected link at rehash {}", link_offset).to_string()))
                }
            }
            // process in reverse order to ensure latest put takes precedence in the new bucket
            current_links.reverse();

            let mut remaining_links = Vec::new();
            for (hash, offset)  in current_links {
                let new_bucket = hash & (!0u32 >> (32 - self.log_mod - 1)); // hash % 2^(log_mod + 1)
                if new_bucket != bucket {
                    // store to new bucket
                    self.store_to_bucket(new_bucket, hash, offset, link_file)?;
                    if let Some(bp) = self.read_page(table_offset.this_page())? {
                        table_page = bp;
                    }
                    rewrite = true;
                } else {
                    // merge links of same hash
                    remaining_links.push((hash, offset));
                }
            }

            if rewrite {
                remaining_links.sort_unstable_by(|a, b|{
                    //reverse
                    u64::cmp(&b.1.as_u64(), &a.1.as_u64())
                });
                let mut next = Offset::invalid();
                for link in remaining_links.chunks(255).rev() {
                    next = link_file.append_link(link.to_vec(), next)?;
                }
                table_page.write_offset(table_offset.in_page_pos(), next)?;
                self.write_page(table_page)?;
            }
        }
        else {
            return Err(BCDBError::Corrupted(format!("missing hash table page {} in rehash", table_offset)));
        }
        Ok(())
    }

    pub fn dedup(&mut self, key: &[u8], link_file: &mut LinkFile, key_file: &KeyFile) -> Result<(), BCDBError> {
        let hash = self.hash(key);
        let mut bucket = hash & (!0u32 >> (32 - self.log_mod)); // hash % 2^(log_mod)
        if bucket < self.step {
            bucket = hash & (!0u32 >> (32 - self.log_mod - 1)); // hash % 2^(log_mod + 1)
        }

        let table_offset = Self::table_offset(bucket);
        if let Some(mut table_page) = self.read_page(table_offset.this_page())? {
            let mut link_offset = table_page.read_offset(table_offset.in_page_pos())?;

            let mut remaining_links = HashMap::new();
            loop {
                if !link_offset.is_valid() {
                    break;
                }
                match link_file.get_content(link_offset)? {
                    Some(Content::Link(links, next)) => {
                        for s in &links {
                            let h = s.0;
                            let offset = s.1;
                            // merge links of same hash
                            remaining_links.entry(h).or_insert(Vec::new()).push(offset);
                        }
                        link_offset = next;
                    },
                    _ => return Err(BCDBError::Corrupted("unknown content at dedup".to_string()))
                };
            }

            {
                let ds = remaining_links.entry(hash).or_insert(Vec::new());
                ds.dedup_by_key(|offset| {
                    if let Ok((k, _)) = key_file.get_key(*offset) {
                        return k;
                    }
                    Vec::new()
                });
            }

            let mut pairs = Vec::new ();
            for hash in remaining_links.keys() {
                for v in remaining_links.get(hash) {
                    for offset in v {
                        pairs.push((*hash, *offset));
                    }
                }
            }

            let mut next = Offset::invalid();
            for link in pairs.chunks(255).rev() {
                if !link.is_empty() {
                    let so = link_file.append_link(link.to_vec(), next)?;
                    next = so;
                }
            }
            table_page.write_offset(table_offset.in_page_pos(), next)?;
            self.write_page(table_page)?;
        }
        else {
            return Err(BCDBError::Corrupted(format!("missing hash table page {} in dedup", table_offset)));
        }
        Ok(())
    }

    fn store_to_bucket(&mut self, bucket: u32, hash: u32, offset: Offset, link_file: &mut LinkFile) -> Result<(), BCDBError> {
        let table_offset = Self::table_offset(bucket);
        if let Some(mut table_page) = self.read_page(table_offset.this_page())? {
            let link_offset = table_page.read_offset(table_offset.in_page_pos())?;

            if link_offset.is_valid() {
                let (mut current_links, next) = link_file.get_link(link_offset)?;
                if current_links.len() == 255 {
                    // prepend with new
                    let so = link_file.append_link(vec!((hash, offset)), link_offset)?;
                    table_page.write_offset(table_offset.in_page_pos(), so)?;
                } else {
                    // clone and extend current
                    current_links.insert(0, (hash, offset));
                    let so = link_file.append_link(current_links, next)?;
                    table_page.write_offset(table_offset.in_page_pos(), so)?;
                }
            }
            else {
                // create new
                let so = link_file.append_link(vec!((hash, offset)), Offset::invalid())?;
                table_page.write_offset(table_offset.in_page_pos(), so)?;
            }

            self.write_page(table_page)?;
        }
        else {
            return Err(BCDBError::Corrupted(format!("missing hash table page {} in store to bucket", table_offset)));
        }
        Ok(())
    }

    pub fn get_unique (&self, key: &[u8], key_file: &KeyFile, link_file: &LinkFile, data_file: &DataFile) -> Result<Option<Vec<u8>>, BCDBError> {
        let hash = self.hash(key);
        let mut bucket = hash & (!0u32 >> (32 - self.log_mod)); // hash % 2^(log_mod)
        if bucket < self.step {
            bucket = hash & (!0u32 >> (32 - self.log_mod - 1)); // hash % 2^(log_mod + 1)
        }
        let table_offset = Self::table_offset(bucket);
        if let Some (table_page) = self.read_page(table_offset.this_page())? {
            let mut link_offset = table_page.read_offset(table_offset.in_page_pos())?;
            loop {
                if !link_offset.is_valid() {
                    return Ok(None);
                }
                match link_file.get_content(link_offset)? {
                    Some(Content::Link(links, next)) => {
                        for s in links {
                            let h = s.0;
                            let offset = s.1;
                            if h == hash {
                                let (data_key, data_offset) = key_file.get_key(offset)?;
                                if data_key == key {
                                    if let Some(Content::Data(data)) = data_file.get_content(data_offset)? {
                                        return Ok(Some(data));
                                    } else {
                                        return Err(BCDBError::Corrupted("key should point to data".to_string()))
                                    }
                                }
                            }
                        }
                        link_offset = next;
                    },
                    _ => return Err(BCDBError::Corrupted("unexpected content".to_string()))
                }
            }
        }
        Ok(None)
    }

    pub fn get (&self, key: &[u8], key_file: &KeyFile, link_file: &LinkFile) -> Result<Vec<Offset>, BCDBError> {
        let hash = self.hash(key);
        let mut bucket = hash & (!0u32 >> (32 - self.log_mod)); // hash % 2^(log_mod)
        if bucket < self.step {
            bucket = hash & (!0u32 >> (32 - self.log_mod - 1)); // hash % 2^(log_mod + 1)
        }
        let table_offset = Self::table_offset(bucket);
        if let Some(table_page) = self.read_page(table_offset.this_page())? {
            let mut link_offset = table_page.read_offset(table_offset.in_page_pos())?;
            let mut result = Vec::new();
            loop {
                if !link_offset.is_valid() {
                    return Ok(result);
                }
                match link_file.get_content(link_offset)? {
                    Some(Content::Link(links, next)) => {
                        for s in links {
                            let h = s.0;
                            let offset = s.1;
                            if h == hash {
                                let (data_key, data_offset) = key_file.get_key(offset)?;
                                if data_key == key {
                                    result.push(data_offset);
                                }
                                else {
                                    return Err(BCDBError::Corrupted("key should point to data".to_string()))
                                }
                            }
                        }
                        link_offset = next;
                    },
                    _ => return Err(BCDBError::Corrupted("unexpected content".to_string()))
                }
            }
        }
        else {
            return Err(BCDBError::Corrupted(format!("missing hash table page {} in get", table_offset)));
        }
    }

    pub fn iter<'a>(&'a self) -> impl Iterator<Item=Offset> +'a {
        BucketIterator{file: self, n:0}
    }

    pub fn patch_page(&mut self, page: TablePage) -> Result<u64, BCDBError> {
        self.async_file.patch_page(page)
    }

    pub fn clear_cache(&mut self, len: u64) {
        self.async_file.clear_cache(len);
    }

    pub fn log_file (&self) -> Arc<Mutex<LogFile>> {
        self.async_file.log_file()
    }

    pub fn shutdown (&mut self) {
        self.async_file.shutdown()
    }

    fn table_offset (bucket: u32) -> Offset {
        if (bucket as u64) < FIRST_BUCKETS_PER_PAGE {
            Offset::from((bucket as u64 / FIRST_BUCKETS_PER_PAGE) * PAGE_SIZE as u64
                + (bucket as u64 % FIRST_BUCKETS_PER_PAGE) * BUCKET_SIZE + FIRST_PAGE_HEAD)
        }
        else {
            Offset::from((((bucket as u64 - FIRST_BUCKETS_PER_PAGE) / BUCKETS_PER_PAGE) + 1) * PAGE_SIZE as u64
                + (bucket as u64 % BUCKETS_PER_PAGE) * BUCKET_SIZE)
        }
    }

    fn hash (&self, key: &[u8]) -> u32 {
        let mut hasher = SipHasher::new_with_keys(self.sip0, self.sip1);
        hasher.write(key);
        hasher.finish() as u32
    }
}

impl TableFile {
    pub fn flush(&mut self) -> Result<(), BCDBError> {
        self.async_file.flush()
    }

    pub fn len(&self) -> Result<u64, BCDBError> {
        self.async_file.len()
    }

    pub fn truncate(&mut self, len: u64) -> Result<(), BCDBError> {
        self.async_file.truncate(len)
    }

    pub fn sync(&self) -> Result<(), BCDBError> {
        self.async_file.sync()
    }

    fn read_page(&self, offset: Offset) -> Result<Option<TablePage>, BCDBError> {
        if let Some(page) = self.async_file.read_page(offset)? {
            let key_page = TablePage::from(page);
            if key_page.offset.as_u64() != offset.as_u64() {
                return Err(BCDBError::Corrupted(format!("hash table page {} does not have the offset of its position", offset)));
            }
            return Ok(Some(key_page));
        }
        Ok(None)
    }

    fn write_page(&mut self, page: TablePage) -> Result<u64, BCDBError> {
        self.async_file.write_page(page.offset, page.page)
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

struct TablePageFile {
    inner: Arc<TablePageFileInner>
}

struct TablePageFileInner {
    file: Mutex<Box<PageFile>>,
    log: Arc<Mutex<LogFile>>,
    cache: Mutex<Cache>,
    flushed: Condvar,
    work: Condvar,
    run: AtomicBool,
    flushing: AtomicBool
}

impl TablePageFileInner {
    pub fn new (file: Box<PageFile>, log: Arc<Mutex<LogFile>>) -> Result<TablePageFileInner, BCDBError> {
        let len = file.len()?;
        Ok(TablePageFileInner { file: Mutex::new(file), log,
            cache: Mutex::new(Cache::new(len)), flushed: Condvar::new(),
            work: Condvar::new(),
            run: AtomicBool::new(true),
            flushing: AtomicBool::new(false)
        })
    }
}

impl TablePageFile {
    pub fn new (file: Box<PageFile>, log: Arc<Mutex<LogFile>>) -> Result<TablePageFile, BCDBError> {
        let inner = Arc::new(TablePageFileInner::new(file, log)?);
        let inner2 = inner.clone();
        thread::spawn(move || { TablePageFile::background(inner2) });
        Ok(TablePageFile { inner })
    }

    fn background (inner: Arc<TablePageFileInner>) {
        while inner.run.load(Ordering::Relaxed) {
            let mut writes = Vec::new();
            {
                let mut cache = inner.cache.lock().expect("cache lock poisoned");
                let mut just_flushed = false;
                if cache.is_empty() {
                    inner.flushed.notify_all();
                    just_flushed = inner.flushing.swap(false, Ordering::AcqRel);
                }
                if !just_flushed {
                    cache = inner.work.wait(cache).expect("cache lock poisoned while waiting for work");
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
}

impl PageFile for TablePageFile {
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
