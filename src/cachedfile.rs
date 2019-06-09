//
// Copyright 2018-2019 Tamas Blummer
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

use lru_cache::LruCache;

use std::sync::{Arc, Mutex};
use std::cmp::max;

pub struct CachedFile {
    file: Box<PagedFile>,
    cache: Mutex<Cache>
}

impl CachedFile {
    /// create a read cached file with a page cache of given size
    pub fn new (file: Box<PagedFile>, pages: usize) -> Result<CachedFile, HammersbaldError> {
        let len = file.len()?;
        Ok(CachedFile{file, cache: Mutex::new(Cache::new(len, pages))})
    }
}

impl PagedFile for CachedFile {
    fn read_page(&self, pref: PRef) -> Result<Option<Page>, HammersbaldError> {
        let result = self.read_pages(pref, 1)?;
        if let Some (page) = result.first() {
            Ok(Some(page.clone()))
        }
        else {
            Ok(None)
        }
    }

    fn read_pages(&self, pref: PRef, n: usize) -> Result<Vec<Page>, HammersbaldError> {
        let mut result = Vec::new();
        let mut cache = self.cache.lock().unwrap();
        let mut from = pref;
        let until = pref + (n * PAGE_SIZE) as u64;
        while from < until {
            if let Some(page) = cache.get(from) {
                result.push(page);
                from += PAGE_SIZE as u64;
            }
            else {
                let mut n = 0;
                let mut next = from;
                while next < until && cache.get(next).is_none() {
                    n += 1;
                    next += PAGE_SIZE as u64;
                }
                result.extend(self.file.read_pages(from, n)?);
                from = next;
            }
        }
        Ok(result)
    }

    fn len(&self) -> Result<u64, HammersbaldError> {
        self.file.len()
    }

    fn truncate(&mut self, new_len: u64) -> Result<(), HammersbaldError> {
        self.cache.lock().unwrap().reset_len(new_len);
        self.file.truncate(new_len)
    }

    fn sync(&self) -> Result<(), HammersbaldError> {
        self.file.sync()
    }

    fn shutdown(&mut self) {
        self.file.shutdown()
    }

    fn append_pages(&mut self, pages: &Vec<Page>) -> Result<(), HammersbaldError> {
        let mut cache = self.cache.lock().unwrap();
        for p in pages {
            cache.append(p.clone());
        }
        self.file.append_pages(pages)
    }

    fn update_page(&mut self, page: Page) -> Result<u64, HammersbaldError> {
        let mut cache = self.cache.lock().unwrap();
        cache.update(page.clone());
        self.file.update_page(page)
    }

    fn flush(&mut self) -> Result<(), HammersbaldError> {
        self.cache.lock().unwrap().clear();
        self.file.flush()
    }
}


pub struct Cache {
    reads: LruCache<PRef, Arc<Page>>,
    len: u64
}

impl Cache {
    pub fn new (len: u64, size: usize) -> Cache {
        Cache { reads: LruCache::new(size), len }
    }

    pub fn cache(&mut self, pref: PRef, page: Arc<Page>) {
        self.reads.insert(pref, page);
    }

    pub fn clear(&mut self) {
        self.reads.clear();
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

    pub fn get(&mut self, pref: PRef) -> Option<Page> {
        use std::ops::Deref;
        if let Some(content) = self.reads.get_mut(&pref) {
            return Some(content.clone().deref().clone())
        }
        None
    }

    pub fn reset_len(&mut self, len: u64) {
        self.len = len;
        let to_delete: Vec<_> = self.reads.iter().filter_map(
            |(o, _)| {
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