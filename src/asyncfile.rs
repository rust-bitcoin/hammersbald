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
//! # Asynchronous file
//! an append only file written in background
//!

use page::Page;
use pagedfile::PagedFile;

use error::BCDBError;
use offset::Offset;

use std::sync::{Mutex, Arc, Condvar};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::collections::VecDeque;

pub struct AsyncFile {
    inner: Arc<AsyncFileInner>
}

struct AsyncFileInner {
    file: Mutex<Box<PagedFile>>,
    work: Condvar,
    flushed: Condvar,
    run: AtomicBool,
    queue: Mutex<VecDeque<Page>>
}

impl AsyncFileInner {
    pub fn new (file: Box<PagedFile>) -> Result<AsyncFileInner, BCDBError> {
        Ok(AsyncFileInner { file: Mutex::new(file), flushed: Condvar::new(), work: Condvar::new(),
            run: AtomicBool::new(true),
            queue: Mutex::new(VecDeque::new())})
    }
}

impl AsyncFile {
    pub fn new (file: Box<PagedFile>) -> Result<AsyncFile, BCDBError> {
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
            while let Some(page) = queue.pop_front() {
                file.append_page(page).expect("can not extend data file");
            }
            inner.flushed.notify_all();
        }
    }
}

impl PagedFile for AsyncFile {
    #[allow(unused_assignments)]
    fn flush(&mut self) -> Result<(), BCDBError> {
        let mut queue = self.inner.queue.lock().unwrap();
        self.inner.work.notify_one();
        while !queue.is_empty() {
            queue = self.inner.flushed.wait(queue).unwrap();
        }
        let mut file = self.inner.file.lock().unwrap();
        file.flush()
    }

    fn len(&self) -> Result<u64, BCDBError> {
        self.inner.file.lock().unwrap().len()
    }

    fn truncate(&mut self, new_len: u64) -> Result<(), BCDBError> {
        self.inner.file.lock().unwrap().truncate(new_len)
    }

    fn sync(&self) -> Result<(), BCDBError> {
        self.inner.file.lock().unwrap().sync()
    }

    fn read_page(&self, offset: Offset) -> Result<Option<Page>, BCDBError> {
        self.inner.file.lock().unwrap().read_page(offset)
    }

    fn append_page(&mut self, page: Page) -> Result<(), BCDBError> {
        let mut queue = self.inner.queue.lock().unwrap();
        queue.push_back(page);
        self.inner.work.notify_one();
        Ok(())
    }

    fn write_page(&mut self, _: Offset, _: Page) -> Result<u64, BCDBError> {
        unimplemented!()
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
}
