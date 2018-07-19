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
//! # A pool of blocks of a single file with its own working thread
//! Buffers IO and allows highly concurrent read and write through the API
//! Actual read and write operations are performed in a dedicated background thread.

use block::{Block, BLOCK_SIZE};
use error::BCSError;
use types::Offset;

use std::io::{Read,Write,Seek,SeekFrom};
use std::sync::{Arc, Mutex, RwLock, Condvar};
use std::collections::{HashMap, VecDeque};
use std::thread;
use std::cell::Cell;

// background writer will loop with this delay
const WRITE_DELAY_MS: u32 = 1000;
// read cache size
const READ_CACHE_BLOCKS: usize = 100;

pub trait RW : Read + Write + Seek + Send {
    fn len (&mut self) -> Result<usize, BCSError>;
    fn truncate(&mut self, new_len: usize) -> Result<(), BCSError>;
    fn sync (&self) -> Result<(), BCSError>;
}

/// The buffer pool
pub struct BlockPool {
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

#[derive(Default)]
struct Cache {
    map: HashMap<Offset, Arc<Block>>,
    list: VecDeque<Arc<Block>>
}

impl Cache {
    fn put (&mut self, block: Arc<Block>) {
        self.map.insert(block.offset, block.clone());
        self.list.push_back(block);
        if self.list.len () > READ_CACHE_BLOCKS {
            let remove = self.list.pop_front().unwrap();
            self.map.remove(&remove.offset);
        }
    }

    fn clear (&mut self) {
        self.map.clear();
        self.list.clear();
    }

    fn get(&self, offset: Offset) -> Option<Arc<Block>> {
        match self.map.get(&offset) {
            Some(b) => Some(b.clone()),
            None => None
        }
    }
}

impl BlockPool {
    pub fn new (rw: Box<RW>) -> BlockPool {
        let inner = Arc::new(Inner::new(rw));
        let inner2 = inner.clone();
        thread::spawn(move || { BlockPool::background(inner2)});
        BlockPool {inner: inner}
    }

    fn background (inner: Arc<Inner>) {
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

    pub fn flush(&mut self) -> Result<(), BCSError> {
        let mut write_cache = self.inner.write_cache.lock().unwrap();
        while !write_cache.is_empty() {
            write_cache = self.inner.flushed.wait(write_cache).unwrap();
        }
        Ok(())
    }

    pub fn sync (&mut self) -> Result<(), BCSError> {
        let rw = self.inner.rw.lock().unwrap();
        rw.sync()
    }

    pub fn shutdown (&mut self) {
        let run = self.inner.run.lock().unwrap();
        run.set(false);
    }

    pub fn truncate(&mut self, offset: Offset) -> Result<(), BCSError> {
        self.inner.read_cache.write().unwrap().clear();
        self.flush()?;
        let mut rw = self.inner.rw.lock().unwrap();
        rw.truncate(offset.as_usize())
    }

    pub fn len(&mut self) -> Result<Offset, BCSError> {
        self.flush()?;
        let mut rw = self.inner.rw.lock().unwrap();
        Offset::new(rw.len()?)
    }

    pub fn read_block (&self, offset: Offset) -> Result<Arc<Block>, BCSError> {
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

    pub fn write_block(&self, block: Arc<Block>) {
        self.inner.write_cache.lock().unwrap().push_back((false, block.clone()));
        self.inner.read_cache.write().unwrap().put(block);
    }

    pub fn append_block (&self, block: Arc<Block>) {
        self.inner.write_cache.lock().unwrap().push_back((true, block));
    }
}