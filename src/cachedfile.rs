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
//! # read cached file
//!

use page::{Page, PAGE_SIZE};
use pagedfile::PagedFile;
use pref::PRef;
use error::HammersbaldError;

use std::collections::{HashMap,VecDeque};
use std::sync::{Arc, RwLock};
use std::cmp::{max, min};

// does not make sense to have a bigger cache
// until age_desc is iterated sequentially
// TODO: find a better cache collection
const MAX_CACHE: usize = 100;

pub struct CachedFile {
    file: Box<PagedFile>,
    cache: RwLock<Cache>
}

impl CachedFile {
    /// create a read cached file with a page cache of given size
    pub fn new (file: Box<PagedFile>, pages: usize) -> Result<CachedFile, HammersbaldError> {
        let len = file.len()?;
        Ok(CachedFile{file, cache: RwLock::new(Cache::new(len, min(MAX_CACHE,pages)))})
    }
}

impl PagedFile for CachedFile {
    fn read_page(&self, pref: PRef) -> Result<Option<Page>, HammersbaldError> {
        {
            let cache = self.cache.read().unwrap();
            if let Some(page) = cache.get(pref) {
                return Ok(Some(page));
            }
        }
        let mut cache = self.cache.write().unwrap();
        if let Some(page) = cache.get(pref) {
            return Ok(Some(page));
        }
        if let Some(page) = self.file.read_page (pref)? {
            cache.cache(pref, Arc::new(page.clone()));
            return Ok(Some(page));
        }
        Ok(None)
    }

    fn len(&self) -> Result<u64, HammersbaldError> {
        self.file.len()
    }

    fn truncate(&mut self, new_len: u64) -> Result<(), HammersbaldError> {
        self.cache.write().unwrap().reset_len(new_len);
        self.file.truncate(new_len)
    }

    fn sync(&self) -> Result<(), HammersbaldError> {
        self.file.sync()
    }

    fn shutdown(&mut self) {
        self.file.shutdown()
    }

    fn append_page(&mut self, page: Page) -> Result<(), HammersbaldError> {
        let mut cache = self.cache.write().unwrap();
        cache.append(page.clone());
        self.file.append_page(page)

    }

    fn update_page(&mut self, page: Page) -> Result<u64, HammersbaldError> {
        let mut cache = self.cache.write().unwrap();
        cache.update(page.clone());
        self.file.update_page(page)
    }

    fn flush(&mut self) -> Result<(), HammersbaldError> {
        self.cache.write().unwrap().clear();
        self.file.flush()
    }
}


pub struct Cache {
    reads: HashMap<PRef, Arc<Page>>,
    age_desc: VecDeque<PRef>,
    len: u64,
    size:  usize
}

impl Cache {
    pub fn new (len: u64, size: usize) -> Cache {
        Cache { reads: HashMap::new(), age_desc: VecDeque::new(), len, size }
    }

    pub fn cache(&mut self, pref: PRef, page: Arc<Page>) {
        if self.reads.insert(pref, page).is_none() {
            self.age_desc.push_back(pref);
            if self.reads.len() > self.size {
                while let Some(old) = self.age_desc.pop_front() {
                    if self.reads.remove(&old).is_some() {
                        break;
                    }
                }
            }
        }
        else {
            if let Some(pos) = self.age_desc.iter().rposition(|o| *o == pref) {
                let last = self.age_desc.len() - 1;
                self.age_desc.swap(pos, last);
            }
        }
    }

    pub fn clear(&mut self) {
        self.reads.clear();
        self.age_desc.clear();
    }

    pub fn append (&mut self, page: Page) ->u64 {
        let pref = PRef::from(self.len);
        let page = Arc::new(page);
        self.cache(pref, page);
        self.len = max(self.len, pref.as_u64() + PAGE_SIZE as u64);
        self.len
    }

    pub fn update (&mut self, page: Page) ->u64 {
        let pref = page.pref();
        let page = Arc::new(page);
        self.cache(pref, page);
        self.len = max(self.len, pref.as_u64() + PAGE_SIZE as u64);
        self.len
    }

    pub fn get(&self, pref: PRef) -> Option<Page> {
        use std::ops::Deref;
        if let Some(content) = self.reads.get(&pref) {
            return Some(content.deref().clone())
        }
        None
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
            self.reads.remove(&PRef::from(o));
        }
    }
}