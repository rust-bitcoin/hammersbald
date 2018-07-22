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
use cache::{ReadCache, WriteCache};
use logfile::LogFile;

use std::io::{Read,Write,Seek,SeekFrom};
use std::sync::{Arc, Mutex, RwLock, Condvar};
use std::collections::VecDeque;
use std::thread;
use std::cell::Cell;
use std::time::Duration;

/// The buffer pool
pub struct AsyncFile {
    inner: Arc<Inner>
}

struct Inner {
    rw: Mutex<Box<RW>>,
    read_cache: RwLock<ReadCache>,
    write_cache: Mutex<WriteCache>,
    haswork: Condvar,
    flushed: Condvar,
    run: Mutex<Cell<bool>>,
    log_file: Option<Arc<Mutex<LogFile>>>
}

impl Inner {
    pub fn new (rw: Box<RW>, log_file: Option<Arc<Mutex<LogFile>>>) -> Inner {
        Inner{
            rw: Mutex::new(rw),
            read_cache: RwLock::new(ReadCache::default()),
            write_cache: Mutex::new(WriteCache::default()),
            haswork: Condvar::new(),
            flushed: Condvar::new(),
            run: Mutex::new(Cell::new(true)),
            log_file
        }
    }

    fn read_page (&self, offset: Offset) -> Result<Arc<Page>, BCSError> {
        if let Some(page) = self.read_cache.read().unwrap().get(offset) {
            return Ok(page);
        }
        let mut buffer = [0u8; PAGE_SIZE];
        let mut read_cache = self.read_cache.write().unwrap();
        let mut rw = self.rw.lock().unwrap();
        rw.seek(SeekFrom::Start(offset.as_usize() as u64))?;
        rw.read(&mut buffer)?;
        let page = Arc::new(Page::from_buf(buffer));
        read_cache.put(page.clone());
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
            let mut write_cache = inner.write_cache.lock().unwrap();
            while write_cache.is_empty() {
                write_cache = inner.haswork.wait(write_cache).unwrap();
            }
            if let Some(ref log_file) = inner.log_file {
                let mut log = log_file.lock().unwrap();
                let mut log_write = false;
                for (append, page) in write_cache.iter() {
                    if !append {
                        let prev = inner.read_page(page.offset).unwrap();
                        log_write |= log.append_page(prev).unwrap();
                    }
                }
                if log_write {
                    log.flush().unwrap();
                    log.sync().unwrap();
                }
            }

            let mut rw = inner.rw.lock().unwrap();
            while let Some((append, page)) = write_cache.pop_front() {
                if !append {
                    let pos = page.offset.as_usize() as u64;
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

    pub fn write_page(&self, page: Arc<Page>) {
        self.inner.write_cache.lock().unwrap().push_back(false, page.clone());
        self.inner.haswork.notify_one();
        self.inner.read_cache.write().unwrap().put(page);
    }

    pub fn append_page (&self, page: Arc<Page>) {
        self.inner.write_cache.lock().unwrap().push_back(true, page.clone());
        self.inner.haswork.notify_one();
        self.inner.read_cache.write().unwrap().put(page);
    }
}

impl DBFile for AsyncFile {
    fn flush(&mut self) -> Result<(), BCSError> {
        let mut write_cache = self.inner.write_cache.lock().unwrap();
        while !write_cache.is_empty() {
            write_cache = self.inner.flushed.wait(write_cache).unwrap();
        }
        Ok(())
    }

    fn sync(&mut self) -> Result<(), BCSError> {
        let rw = self.inner.rw.lock().unwrap();
        rw.sync()
    }

    fn truncate(&mut self, offset: Offset) -> Result<(), BCSError> {
        self.inner.read_cache.write().unwrap().clear();
        self.flush()?;
        let mut rw = self.inner.rw.lock().unwrap();
        rw.truncate(offset.as_usize())
    }

    fn len(&mut self) -> Result<Offset, BCSError> {
        self.flush()?;
        let mut rw = self.inner.rw.lock().unwrap();
        Offset::new(rw.len()?)
    }
}

impl PageFile for AsyncFile {
    fn read_page (&self, offset: Offset) -> Result<Arc<Page>, BCSError> {
        self.inner.read_page(offset)
    }
}
