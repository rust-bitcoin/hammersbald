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
//! # a disk page cache
//!
//! A very fast persistent blockchain store and a convenience library for blockchain in-memory cache.
//!

use page::Page;
use types::Offset;

use std::collections::{HashMap,VecDeque};
use std::sync::Arc;

// read cache size
pub const READ_CACHE_PAGES: usize = 100;

#[derive(Default)]
pub struct ReadCache {
    map: HashMap<Offset, Arc<Page>>,
    list: VecDeque<Arc<Page>>
}

impl ReadCache {
    pub fn put (&mut self, page: Arc<Page>) {
        if self.list.len () >= READ_CACHE_PAGES {
            if let Some(old) = self.list.pop_front() {
                self.map.remove(&old.offset);
            }
        }
        if self.map.insert(page.offset, page.clone()).is_none() {
            self.list.push_back(page);
        }
    }

    pub fn clear (&mut self) {
        self.map.clear();
        self.list.clear();
    }

    pub fn get(&self, offset: Offset) -> Option<Arc<Page>> {
        match self.map.get(&offset) {
            Some(b) => Some(b.clone()),
            None => None
        }
    }
}

#[derive(Default)]
pub struct WriteCache {
    map: HashMap<Offset, (bool, Arc<Page>)>
}

impl WriteCache {
    pub fn put (&mut self, append: bool, page: Arc<Page>) {
        let offset = page.offset;
        self.map.insert(offset, (append, page));
    }

    pub fn is_empty (&self) -> bool {
        self.map.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item=&(bool, Arc<Page>)> {
        self.map.values().into_iter()
    }

    pub fn get(&self, offset: Offset) -> Option<Arc<Page>> {
        if let Some(ref content) = self.map.get(&offset) {
            return Some(content.1.clone())
        }
        None
    }

    pub fn clear(&mut self) {
        self.map.clear()
    }
}