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
//! # A single file with its own working thread
//! Buffers IO and allows highly concurrent read and write through the API
//! Write operations are performed in a dedicated background thread.


use bcdb::{DBFile, RW, PageFile};
use page::{Page, PAGE_SIZE};
use error::BCSError;
use types::Offset;
use cache::Cache;
use logfile::LogFile;

use std::io::{Read,Write,Seek,SeekFrom};
use std::sync::{Arc, Mutex, Condvar};
use std::thread;
use std::cell::Cell;

/// The buffer pool
pub struct AsyncFile {
    inner: Arc<Inner>
}

struct Inner {
    rw: Mutex<Box<RW>>,
    cache: Mutex<Cache>,
    haswork: Condvar,
    flushed: Condvar,
    run: Mutex<Cell<bool>>,
    log_file: Option<Arc<Mutex<LogFile>>>
}

impl Inner {
    pub fn new (rw: Box<RW>, log_file: Option<Arc<Mutex<LogFile>>>) -> Inner {
        Inner{
            rw: Mutex::new(rw),
            cache: Mutex::new(Cache::default()),
            haswork: Condvar::new(),
            flushed: Condvar::new(),
            run: Mutex::new(Cell::new(true)),
            log_file
        }
    }

    fn read_page (&self, offset: Offset) -> Result<Arc<Page>, BCSError> {
        {
            let cache = self.cache.lock().unwrap();
            if let Some(page) = cache.get(offset) {
                return Ok(page);
            }
        }

        let page = self.read_page_from_store(offset)?;

        {
            // if there was a write between above read and this lock
            // then this cache was irrelevant as write cache has priority
            let mut cache = self.cache.lock().unwrap();
            cache.cache(page.clone());
        }
        Ok(page)
    }

    fn read_page_from_store (&self, offset: Offset) -> Result<Arc<Page>, BCSError> {
        let mut buffer = [0u8; PAGE_SIZE];
        let mut rw = self.rw.lock().unwrap();
        rw.seek(SeekFrom::Start(offset.as_u64()))?;
        rw.read(&mut buffer)?;
        let page = Arc::new(Page::from_buf(buffer));
        Ok(page)
    }

}

impl AsyncFile {
    pub fn new(rw: Box<RW>, log_file: Option<Arc<Mutex<LogFile>>>) -> AsyncFile {
        let inner = Arc::new(Inner::new(rw, log_file));
        let inner2 = inner.clone();
        thread::spawn(move || { AsyncFile::background(inner2) });
        AsyncFile { inner: inner }
    }

    pub fn log_file (&self) -> Option<Arc<Mutex<LogFile>>> {
        if let Some (ref log_file) = self.inner.log_file {
            return Some(log_file.clone());
        }
        None
    }

    fn background(inner: Arc<Inner>) {
        let mut run = true;
        while run {

            let writes;
            {
                // limit scope of cache lock to collection of work
                // since clear_writes() moves work to read cache
                // lock can be released without risking that subsequent reads do not yet get
                // the written data
                let mut cache = inner.cache.lock().unwrap();
                while cache.is_empty() {
                    cache = inner.haswork.wait(cache).unwrap();
                }
                let mut logged = false;
                for (append, _) in cache.writes() {
                    if !append {
                        logged = true;
                        break;
                    }
                }
                if logged {
                    if let Some(ref log_file) = inner.log_file {
                        let mut log = log_file.lock().unwrap();
                        let mut log_write = false;
                        for (append, page) in cache.writes() {
                            if !append && page.offset.as_u64() < log.tbl_len && !log.has_page(page.offset) {
                                if let Ok(prev) = inner.read_page_from_store(page.offset) {
                                    log_write |= log.append_page(prev).unwrap();
                                }
                            }
                        }
                        if log_write {
                            log.flush().unwrap();
                            log.sync().unwrap();
                        }
                    }
                }
                writes = cache.writes().into_iter().map(|e| e.clone()).collect::<Vec<_>>();
                cache.move_writes_to_wrote();
            }

            let mut rw = inner.rw.lock().unwrap();
            for (append, page) in writes {
                if !append {
                    let pos = page.offset.as_u64();
                    rw.seek(SeekFrom::Start(pos)).expect(format!("can not seek to {}", pos).as_str());
                }
                rw.write(&page.finish()).unwrap();
            }

            rw.flush().unwrap();
            inner.flushed.notify_all();

            run = inner.run.lock().unwrap().get();
        }
    }

    pub fn shutdown (&mut self) {
        let run = self.inner.run.lock().unwrap();
        run.set(false);
    }

    pub fn patch_page(&mut self, page: Arc<Page>) {
        let mut rw = self.inner.rw.lock().unwrap();
        let pos = page.offset.as_u64();
        rw.seek(SeekFrom::Start(pos)).expect(format!("can not seek to {}", pos).as_str());
        rw.write(&page.finish()).unwrap();
    }

    pub fn write_page(&self, page: Arc<Page>) {
        self.inner.cache.lock().unwrap().update(page);
        self.inner.haswork.notify_one();
    }

    pub fn append_page (&self, page: Arc<Page>) {
        self.inner.cache.lock().unwrap().append(page);
        self.inner.haswork.notify_one();
    }

    pub fn clear_cache(&mut self) {
        self.inner.cache.lock().unwrap().clear();
    }
}

impl DBFile for AsyncFile {
    fn flush(&mut self) -> Result<(), BCSError> {
        let mut cache = self.inner.cache.lock().unwrap();
        while !cache.is_empty() {
            cache = self.inner.flushed.wait(cache).unwrap();
        }
        Ok(())
    }

    fn sync(&mut self) -> Result<(), BCSError> {
        let rw = self.inner.rw.lock().unwrap();
        rw.sync()
    }

    fn truncate(&mut self, offset: Offset) -> Result<(), BCSError> {
        let mut rw = self.inner.rw.lock().unwrap();
        rw.truncate(offset.as_u64() as usize)
    }

    fn len(&mut self) -> Result<Offset, BCSError> { ;
        let mut rw = self.inner.rw.lock().unwrap();
        Offset::new(rw.len()? as u64)
    }
}

impl PageFile for AsyncFile {
    fn read_page (&self, offset: Offset) -> Result<Arc<Page>, BCSError> {
        let page = self.inner.read_page(offset)?;
        if page.offset != offset {
            return Err(BCSError::Corrupted);
        }
        Ok(page)
    }
}
