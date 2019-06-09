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
//! # Asynchronous file
//! an append only file written in background
//!

use page::{Page, PAGE_SIZE};
use pagedfile::PagedFile;

use error::HammersbaldError;
use pref::PRef;

use std::sync::{Mutex, Arc, Condvar};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::cmp::min;

pub struct AsyncFile {
    inner: Arc<AsyncFileInner>
}

struct AsyncFileInner {
    file: Mutex<Box<PagedFile + Send + Sync>>,
    work: Condvar,
    flushed: Condvar,
    run: AtomicBool,
    queue: Mutex<Vec<Page>>
}

impl AsyncFileInner {
    pub fn new (file: Box<PagedFile + Send + Sync>) -> Result<AsyncFileInner, HammersbaldError> {
        Ok(AsyncFileInner { file: Mutex::new(file), flushed: Condvar::new(), work: Condvar::new(),
            run: AtomicBool::new(true),
            queue: Mutex::new(Vec::new())})
    }
}

impl AsyncFile {
    pub fn new (file: Box<PagedFile + Send + Sync>) -> Result<AsyncFile, HammersbaldError> {
        let inner = Arc::new(AsyncFileInner::new(file)?);
        let inner2 = inner.clone();
        thread::spawn(move || { AsyncFile::background(inner2) });
        Ok(AsyncFile { inner })
    }

    fn background (inner: Arc<AsyncFileInner>) {
        let mut queue = inner.queue.lock().expect("page queue lock poisoned");
        while inner.run.load(Ordering::Acquire) {
            while queue.is_empty() {
                queue = inner.work.wait(queue).expect("page queue lock poisoned");
            }
            let mut file = inner.file.lock().expect("file lock poisoned");
            file.append_pages(&queue).expect("can not write in background");
            queue.clear();
            inner.flushed.notify_all();
        }
    }

    fn read_in_queue (&self, pref: PRef) -> Result<Option<Page>, HammersbaldError> {
        let queue = self.inner.queue.lock().expect("page queue lock poisoned");
        if queue.len () > 0 {
            let file = self.inner.file.lock().expect("file lock poisoned");
            let len = file.len()?;
            if pref.as_u64() >= len {
                let index = ((pref.as_u64() - len) / PAGE_SIZE as u64) as usize;
                if index < queue.len() {
                    let page = queue[index].clone();
                    return Ok(Some(page));
                }
            }
        }
        Ok(None)
    }
}

impl PagedFile for AsyncFile {
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
        let mut need = n;
        let file_end ;
        {
            let file = self.inner.file.lock().expect("file lock poisoned");
            file_end = PRef::from(file.len()?);
            if pref < file_end {
                let np = min(need, ((file_end.as_u64() - pref.as_u64())/PAGE_SIZE as u64) as usize);
                need -= np;
                result.extend(file.read_pages(pref, np)?);
            }
        }
        if need > 0 {
            let mut next = file_end;
            while let Some(page) = self.read_in_queue(next)? {
                result.push(page);
                next += PAGE_SIZE as u64;
                need -= 1;
                if need == 0 {
                    break;
                }
            }
        }
        Ok(result)
    }

    fn len(&self) -> Result<u64, HammersbaldError> {
        self.inner.file.lock().unwrap().len()
    }

    fn truncate(&mut self, new_len: u64) -> Result<(), HammersbaldError> {
        self.inner.file.lock().unwrap().truncate(new_len)
    }

    fn sync(&self) -> Result<(), HammersbaldError> {
        self.inner.file.lock().unwrap().sync()
    }

    fn shutdown (&mut self) {
        let mut queue = self.inner.queue.lock().unwrap();
        self.inner.work.notify_one();
        while !queue.is_empty() {
            queue = self.inner.flushed.wait(queue).unwrap();
        }
        let mut file = self.inner.file.lock().unwrap();
        file.flush().unwrap();
        self.inner.run.store(false, Ordering::Release)
    }

    fn append_pages (&mut self, pages: &Vec<Page>) -> Result<(), HammersbaldError> {
        let mut queue = self.inner.queue.lock().unwrap();
        for page in pages {
            queue.push(page.clone());
        }
        self.inner.work.notify_one();
        Ok(())
    }

    fn update_page(&mut self, _: Page) -> Result<u64, HammersbaldError> {
        unimplemented!()
    }

    fn flush(&mut self) -> Result<(), HammersbaldError> {
        let mut queue = self.inner.queue.lock().unwrap();
        self.inner.work.notify_one();
        while !queue.is_empty() {
            queue = self.inner.flushed.wait(queue).unwrap();
        }
        let mut file = self.inner.file.lock().unwrap();
        file.flush()
    }
}
