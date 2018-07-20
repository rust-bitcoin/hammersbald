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
//! # a disk block cache
//!
//! A very fast persistent blockchain store and a convenience library for blockchain in-memory cache.
//!

use block::Block;
use types::Offset;

use std::collections::{HashMap,VecDeque};
use std::sync::Arc;

// read cache size
pub const READ_CACHE_BLOCKS: usize = 100;

#[derive(Default)]
pub struct Cache {
    map: HashMap<Offset, Arc<Block>>,
    list: VecDeque<Arc<Block>>
}

impl Cache {
    pub fn put (&mut self, block: Arc<Block>) {
        if self.list.len () >= READ_CACHE_BLOCKS {
            let remove = self.list.pop_front().unwrap();
            self.map.remove(&remove.offset);
        }
        if self.map.insert(block.offset, block.clone()).is_none() {
            self.list.push_back(block);
        }
    }

    pub fn clear (&mut self) {
        self.map.clear();
        self.list.clear();
    }

    pub fn get(&self, offset: Offset) -> Option<Arc<Block>> {
        match self.map.get(&offset) {
            Some(b) => Some(b.clone()),
            None => None
        }
    }
}