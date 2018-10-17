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
use offset::Offset;
use error::BCDBError;

use std::collections::{HashMap,VecDeque};
use std::sync::{Arc, RwLock};
use std::cmp::max;


pub struct CachedFile {
    file: Box<PagedFile>,
    cache: RwLock<Cache>
}

impl CachedFile {
    /// create a read cached file with a page cache of given size
    pub fn new (file: Box<PagedFile>, pages: usize) -> Result<CachedFile, BCDBError> {
        let len = file.len()?;
        Ok(CachedFile{file, cache: RwLock::new(Cache::new(len, pages))})
    }
}

impl PagedFile for CachedFile {
    fn flush(&mut self) -> Result<(), BCDBError> {
        self.file.flush()
    }

    fn len(&self) -> Result<u64, BCDBError> {
        self.file.len()
    }

    fn truncate(&mut self, new_len: u64) -> Result<(), BCDBError> {
        self.cache.write().unwrap().reset_len(new_len);
        self.file.truncate(new_len)
    }

    fn sync(&self) -> Result<(), BCDBError> {
        self.file.sync()
    }

    fn read_page(&self, offset: Offset) -> Result<Option<Page>, BCDBError> {
        {
            let cache = self.cache.read().unwrap();
            if let Some(page) = cache.get(offset) {
                return Ok(Some(page));
            }
        }
        if let Some(page) = self.file.read_page (offset)? {
            let mut cache = self.cache.write().unwrap();
            cache.cache(offset, Arc::new(page.clone()));
            return Ok(Some(page));
        }
        Ok(None)
    }

    fn append_page(&mut self, page: Page) -> Result<(), BCDBError> {
        let mut cache = self.cache.write().unwrap();
        cache.append(page.clone());
        self.file.append_page(page)

    }

    fn write_page(&mut self, _: Offset, _: Page) -> Result<u64, BCDBError> {
        unimplemented!()
    }

    fn shutdown(&mut self) {
        self.file.shutdown()
    }
}


pub struct Cache {
    reads: HashMap<Offset, Arc<Page>>,
    age_desc: VecDeque<Offset>,
    len: u64,
    size:  usize
}

impl Cache {
    pub fn new (len: u64, size: usize) -> Cache {
        Cache { reads: HashMap::new(), age_desc: VecDeque::new(), len, size }
    }

    pub fn cache (&mut self, offset: Offset, page: Arc<Page>) {
        if self.reads.insert(offset, page).is_none() {
            self.age_desc.push_back(offset);
            if self.reads.len() > self.size {
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