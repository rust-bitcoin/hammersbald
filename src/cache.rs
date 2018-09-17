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
pub const READ_CACHE_PAGES: usize = 1000;

#[derive(Default)]
pub struct Cache {
    writes: HashMap<Offset, Arc<Page>>,
    wrote: HashMap<Offset, Arc<Page>>,
    reads: HashMap<Offset, Arc<Page>>,
    fifo: VecDeque<Arc<Page>>
}

impl Cache {
    pub fn cache (&mut self, page: Page) {
        if !self.writes.contains_key(&page.offset) && !self.wrote.contains_key(&page.offset) {
            if self.fifo.len() >= READ_CACHE_PAGES {
                if let Some(old) = self.fifo.pop_front() {
                    self.reads.remove(&old.offset);
                }
            }

            let pp = Arc::new(page);
            self.fifo.push_back(pp.clone());
            if self.reads.insert(pp.offset, pp.clone()).is_some() {
                if let Some(prev) = self.fifo.iter().position(move |p| p.offset == pp.offset) {
                    self.fifo.swap_remove_back(prev);
                }
            }
        }
    }

    pub fn write (&mut self, page: Page) {
        let offset = page.offset;
        if self.reads.remove(&page.offset).is_some() {
            if let Some(prev) = self.fifo.iter().position(move |p| p.offset == offset) {
                self.fifo.remove(prev);
            }
        }
        self.wrote.remove(&offset);
        self.writes.insert(offset, Arc::new(page));
    }

    pub fn is_empty (&self) -> bool {
        self.writes.is_empty()
    }

    pub fn writes(&self) -> impl Iterator<Item=&Arc<Page>> {
        self.writes.values().into_iter()
    }

    pub fn get(&self, offset: Offset) -> Option<Arc<Page>> {
        if let Some(content) = self.writes.get(&offset) {
            return Some(content.clone())
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
        for page in values {
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