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
//! Actual read and write operations are performed in a dedicated background thread.


use blockdb::{DBFile,RW};
use block::{Block, BLOCK_SIZE};
use error::BCSError;
use types::Offset;
use cache::Cache;

use std::io::{Read,Write,Seek,SeekFrom};
use std::sync::{Arc, Mutex, RwLock, Condvar};
use std::collections::{HashMap, VecDeque};
use std::thread;
use std::cell::Cell;

// background writer will loop with this delay
const WRITE_DELAY_MS: u32 = 1000;

/// The buffer pool
pub struct AsyncFile {
    inner: Arc<Inner>
}

struct Inner {
    rw: Mutex<Box<RW>>,
    read_cache: RwLock<Cache>,
    write_cache: Mutex<VecDeque<(bool, Arc<Block>)>>,
    flushed: Condvar,
    run: Mutex<Cell<bool>>
}

impl Inner {
    pub fn new (rw: Box<RW>) -> Inner {
        Inner{
            rw: Mutex::new(rw),
            read_cache: RwLock::new(Cache::default()),
            write_cache: Mutex::new(VecDeque::new()),
            flushed: Condvar::new(),
            run: Mutex::new(Cell::new(true))
        }
    }
}

impl AsyncFile {
    pub fn new(rw: Box<RW>) -> AsyncFile {
        let inner = Arc::new(Inner::new(rw));
        let inner2 = inner.clone();
        thread::spawn(move || { AsyncFile::background(inner2) });
        AsyncFile { inner: inner }
    }

    fn background(inner: Arc<Inner>) {
        let mut run = true;
        while run {
            thread::sleep_ms(WRITE_DELAY_MS);

            let mut write_cache = inner.write_cache.lock().unwrap();
            let wrote = !write_cache.is_empty();
            while let Some((append, block)) = write_cache.pop_front() {
                let mut rw = inner.rw.lock().unwrap();
                if !append {
                    let pos = block.offset.as_usize() as u64;
                    rw.seek(SeekFrom::Start(pos)).expect(format!("can not seek to {}", pos).as_str());
                }
                rw.write(&block.finish()).unwrap();
            }
            if wrote {
                let mut rw = inner.rw.lock().unwrap();
                rw.flush().unwrap();
            }
            inner.flushed.notify_one();
            run = inner.run.lock().unwrap().get();
        }
    }

    pub fn shutdown (&mut self) {
        let run = self.inner.run.lock().unwrap();
        run.set(false);
    }

    pub fn write_block(&self, block: Arc<Block>) {
        self.inner.write_cache.lock().unwrap().push_back((false, block.clone()));
        self.inner.read_cache.write().unwrap().put(block);
    }

    pub fn append_block (&self, block: Arc<Block>) {
        self.inner.write_cache.lock().unwrap().push_back((true, block.clone()));
        self.inner.read_cache.write().unwrap().put(block);
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

    fn sync (&mut self) -> Result<(), BCSError> {
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

    fn read_block (&self, offset: Offset) -> Result<Arc<Block>, BCSError> {
        if let Some(block) = self.inner.read_cache.read().unwrap().get(offset) {
            return Ok(block);
        }
        let mut buffer = [0u8; BLOCK_SIZE];
        let mut read_cache = self.inner.read_cache.write().unwrap();
        self.inner.rw.lock().unwrap().read(&mut buffer)?;
        let block = Arc::new(Block::from_buf(buffer)?);
        read_cache.put(block.clone());
        Ok(block)
    }
}