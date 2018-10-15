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

use page::{Page, PAGE_SIZE};
use offset::Offset;

use std::collections::{HashMap,VecDeque};
use std::sync::Arc;
use std::cmp::max;

// read cache size
pub const READ_CACHE_PAGES: usize = 1000;

pub struct Cache {
    writes: Vec<(Offset, Arc<Page>)>,
    reads: HashMap<Offset, Arc<Page>>,
    age_desc: VecDeque<Offset>,
    len: u64
}

impl Cache {
    pub fn new (len: u64) -> Cache {
        Cache { writes: Vec::new(), reads: HashMap::new(), age_desc: VecDeque::new(), len }
    }

    pub fn cache (&mut self, offset: Offset, page: Arc<Page>) {
        if self.reads.insert(offset, page).is_none() {
            self.age_desc.push_back(offset);
            if self.reads.len() > READ_CACHE_PAGES {
                while let Some(old) = self.age_desc.pop_front() {
                    if self.reads.remove(&old).is_some() {
                        break;
                    }
                }
            }
        }
        else {
            if let Some(pos) = self.age_desc.iter().rposition(|o| *o == offset) {
                let last = self.age_desc.len() - 1;
                self.age_desc.swap(pos, last);
            }
        }
    }

    pub fn append (&mut self, page: Page) ->u64 {
        let offset = Offset::from(self.len);
        let page = Arc::new(page);
        self.writes.push((offset, page.clone()));
        self.cache(offset, page);
        self.len = max(self.len, offset.as_u64() + PAGE_SIZE as u64);
        self.len
    }

    pub fn get(&self, offset: Offset) -> Option<Page> {
        use std::ops::Deref;
        if let Some(content) = self.reads.get(&offset) {
            return Some(content.deref().clone())
        }
        None
    }

    pub fn has_writes(&self) -> bool {
        return self.writes.is_empty()
    }

    pub fn new_writes(&mut self) -> impl Iterator<Item=&(Offset, Arc<Page>)> {
        self.writes.iter()
    }

    pub fn clear_writes(&mut self) {
        self.writes.clear()
    }

    pub fn reset_len(&mut self, len: u64) {
        self.len = len;
        let to_delete: Vec<_> = self.reads.keys().filter_map(
            |o| {
                let l = o.as_u64();
                if l >= len {
                    Some(l)
                }
                else {
                    None
                }
            }).collect();
        for o in to_delete {
            self.reads.remove(&Offset::from(o));
        }
    }
}