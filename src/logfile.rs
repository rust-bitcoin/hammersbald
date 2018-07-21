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
//! # The log file
//!

use blockdb::{DBFile,RW, BlockIterator,BlockFile};
use block::{Block, BLOCK_SIZE};
use error::BCSError;
use types::Offset;
use cache::Cache;

use std::io::{Read,Write,Seek,SeekFrom};
use std::sync::{Arc, Mutex, RwLock, Condvar};
use std::collections::HashSet;
use std::thread;
use std::cell::Cell;

// background writer will loop with this delay
const WRITE_DELAY_MS: u32 = 1000;

/// The buffer pool
pub struct LogFile {
    rw: Mutex<Box<RW>>,
    appended: HashSet<Offset>
}

impl LogFile {
    pub fn new(rw: Box<RW>) -> LogFile {
        LogFile { rw: Mutex::new(rw), appended: HashSet::new() }
    }

    pub fn append_block (&mut self, block: Arc<Block>) -> Result<bool, BCSError> {
        if !self.appended.contains(&block.offset) {
            self.appended.insert(block.offset);
            self.rw.lock().unwrap().write(&block.finish())?;
            return Ok(true);
        }
        Ok(false)
    }

    pub fn reset (&mut self) {
        self.appended.clear();
    }

    fn block_iter (&self) -> BlockIterator {
        BlockIterator::new(self)
    }
}

impl DBFile for LogFile {
    fn flush(&mut self) -> Result<(), BCSError> {
        Ok(self.rw.lock().unwrap().flush()?)
    }

    fn sync(&mut self) -> Result<(), BCSError> {
        let rw = self.rw.lock().unwrap();
        rw.sync()
    }

    fn truncate(&mut self, offset: Offset) -> Result<(), BCSError> {
        let mut rw = self.rw.lock().unwrap();
        rw.truncate(offset.as_usize())
    }

    fn len(&mut self) -> Result<Offset, BCSError> {
        self.flush()?;
        let mut rw = self.rw.lock().unwrap();
        Offset::new(rw.len()?)
    }
}

impl BlockFile for LogFile {
    fn read_block (&self, offset: Offset) -> Result<Arc<Block>, BCSError> {
        let mut buffer = [0u8; BLOCK_SIZE];
        self.rw.lock().unwrap().read(&mut buffer)?;
        let block = Arc::new(Block::from_buf(buffer)?);
        Ok(block)
    }
}