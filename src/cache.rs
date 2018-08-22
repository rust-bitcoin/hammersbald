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
pub struct Cache {
    writes: HashMap<Offset, (bool, Arc<Page>)>,
    wrote: HashMap<Offset, Arc<Page>>,
    reads: HashMap<Offset, Arc<Page>>,
    fifo: VecDeque<Arc<Page>>
}

impl Cache {
    pub fn append (&mut self, page: Arc<Page>) {
        self.put(true, page)
    }

    pub fn update (&mut self, page: Arc<Page>) {
        self.put(false, page)
    }

    pub fn cache (&mut self, page: Arc<Page>) {
        if !self.writes.contains_key(&page.offset) && !self.wrote.contains_key(&page.offset) {
            if self.fifo.len() >= READ_CACHE_PAGES {
                if let Some(old) = self.fifo.pop_front() {
                    self.reads.remove(&old.offset);
                }
            }

            self.fifo.push_back(page.clone());
            if self.reads.insert(page.offset, page.clone()).is_some() {
                if let Some(prev) = self.fifo.iter().position(move |p| p.offset == page.offset) {
                    self.fifo.swap_remove_back(prev);
                }
            }
        }
    }

    fn put (&mut self, append: bool, page: Arc<Page>) {
        let offset = page.offset;
        if self.reads.remove(&page.offset).is_some() {
            if let Some(prev) = self.fifo.iter().position(move |p| p.offset == offset) {
                self.fifo.remove(prev);
            }
        }
        self.wrote.remove(&offset);
        self.writes.insert(offset, (append, page));
    }

    pub fn is_empty (&self) -> bool {
        self.writes.is_empty()
    }

    pub fn writes(&self) -> impl Iterator<Item=&(bool, Arc<Page>)> {
        self.writes.values().into_iter()
    }

    pub fn get(&self, offset: Offset) -> Option<Arc<Page>> {
        if let Some(ref content) = self.writes.get(&offset) {
            return Some(content.1.clone())
        }
        if let Some(content) = self.wrote.get(&offset) {
            return Some(content.clone())
        }
        if let Some(content) = self.reads.get(&offset) {
            return Some(content.clone())
        }
        None
    }

    pub fn move_writes_to_wrote(&mut self) {
        let values = self.writes.values().into_iter().map(|e| e.clone()).collect::<Vec<_>>();
        for (_, page) in values {
            self.wrote.insert(page.offset, page);
        }
        self.writes.clear()
    }

    pub fn clear (&mut self) {
        self.writes.clear();
        self.wrote.clear();
        self.reads.clear();
        self.fifo.clear();
    }
}