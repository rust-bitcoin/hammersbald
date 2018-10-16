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
//! # Asynchronous file
//! an append only file written in background
//!

use page::{Page, PAGE_SIZE};
use pagedfile::PagedFile;

use cache::Cache;
use error::BCDBError;
use offset::Offset;

use std::sync::{Mutex,Arc,Condvar};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

pub struct AsyncFile {
    inner: Arc<AsyncFileInner>
}

struct AsyncFileInner {
    file: Mutex<Box<PagedFile>>,
    cache: Mutex<Cache>,
    work: Condvar,
    run: AtomicBool,
}

impl AsyncFileInner {
    pub fn new (file: Box<PagedFile>) -> Result<AsyncFileInner, BCDBError> {
        let len = file.len()?;
        Ok(AsyncFileInner { file: Mutex::new(file), cache: Mutex::new(Cache::new(len)),
            work: Condvar::new(), run: AtomicBool::new(true)})
    }
}

impl AsyncFile {
    pub fn new (file: Box<PagedFile>) -> Result<AsyncFile, BCDBError> {
        let inner = Arc::new(AsyncFileInner::new(file)?);
        let inner2 = inner.clone();
        thread::spawn(move || { AsyncFile::background(inner2) });
        Ok(AsyncFile { inner })
    }

    fn background (inner: Arc<AsyncFileInner>) {
        let mut cache = inner.cache.lock().expect("cache lock poisoned");
        loop {
            while cache.has_writes() && inner.run.load(Ordering::Acquire) {
                cache = inner.work.wait(cache).expect("cache lock poisoned while waiting for work");
            }
            if inner.run.load(Ordering::Acquire) == false {
                break;
            }
            let mut file = inner.file.lock().expect("file lock poisoned");
            let mut next = file.len().unwrap();
            for (o, page) in cache.new_writes() {
                use std::ops::Deref;

                if o.as_u64() != next as u64 {
                    panic!("non consecutive append {} {}", next, o);
                }
                next = o.as_u64() + PAGE_SIZE as u64;
                file.append_page(page.deref().clone()).expect("can not extend data file");
            }
            cache.clear_writes();
            inner.work.notify_one();
        }
    }

    fn read_page_from_store (&self, offset: Offset) -> Result<Option<Page>, BCDBError> {
        if offset != offset.this_page () {
            return Err(BCDBError::Corrupted(format!("data or link read is not page aligned {}", offset)))
        }
        self.inner.file.lock().unwrap().read_page(offset)
    }
}

impl PagedFile for AsyncFile {
    #[allow(unused_assignments)]
    fn flush(&mut self) -> Result<(), BCDBError> {
        let mut cache = self.inner.cache.lock().unwrap();
        while !cache.has_writes() {
            cache = self.inner.work.wait(cache).expect("cache lock poisoned while waiting for flush");
        }
        self.inner.file.lock().unwrap().flush()
    }

    fn len(&self) -> Result<u64, BCDBError> {
        self.inner.file.lock().unwrap().len()
    }

    fn truncate(&mut self, new_len: u64) -> Result<(), BCDBError> {
        let mut cache = self.inner.cache.lock().unwrap();
        cache.clear_writes();
        cache.reset_len(new_len);
        self.inner.file.lock().unwrap().truncate(new_len)
    }

    fn sync(&self) -> Result<(), BCDBError> {
        self.inner.file.lock().unwrap().sync()
    }

    fn read_page(&self, offset: Offset) -> Result<Option<Page>, BCDBError> {
        let mut cache = self.inner.cache.lock().unwrap();
        if let Some(page) = cache.get(offset) {
            return Ok(Some(page));
        }
        if let Some(page) = self.read_page_from_store(offset)? {
            cache.cache(offset, Arc::new(page.clone()));
            return Ok(Some(page));
        }
        Ok(None)
    }

    fn append_page(&mut self, page: Page) -> Result<u64, BCDBError> {
        let mut cache = self.inner.cache.lock().unwrap();
        let len = cache.append(page);
        self.inner.work.notify_all();
        Ok(len)
    }

    fn write_page(&mut self, _: Offset, _: Page) -> Result<u64, BCDBError> {
        unimplemented!()
    }

    fn shutdown (&mut self) {
        let _cache = self.inner.cache.lock().unwrap();
        self.inner.run.store(false, Ordering::Release);
        self.inner.work.notify_all();
    }
}
