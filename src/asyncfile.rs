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

use cache::Cache;
use error::BCDBError;
use offset::Offset;

use std::sync::{Mutex, Arc, mpsc};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

pub struct AsyncFile {
    inner: Arc<AsyncFileInner>,
    sender: Mutex<mpsc::Sender<Option<Page>>>
}

struct AsyncFileInner {
    file: Mutex<Box<PagedFile>>,
    cache: Mutex<Cache>,
    run: AtomicBool
}

impl AsyncFileInner {
    pub fn new (file: Box<PagedFile>) -> Result<AsyncFileInner, BCDBError> {
        let len = file.len()?;
        Ok(AsyncFileInner { file: Mutex::new(file), cache: Mutex::new(Cache::new(len)), run: AtomicBool::new(true)})
    }
}

impl AsyncFile {
    pub fn new (file: Box<PagedFile>) -> Result<AsyncFile, BCDBError> {
        let (sender, receiver) = mpsc::channel();
        let inner = Arc::new(AsyncFileInner::new(file)?);
        let inner2 = inner.clone();
        thread::spawn(move || { AsyncFile::background(inner2, receiver) });
        Ok(AsyncFile { inner, sender: Mutex::new(sender) })
    }

    fn background (inner: Arc<AsyncFileInner>, receiver: mpsc::Receiver<Option<Page>>) {
        loop {
            if inner.run.load(Ordering::Acquire) == false {
                break;
            }
            if let Some(page) = receiver.recv().expect("call AsyncFile::shutdown () to avoid this error") {
                let mut file = inner.file.lock().expect("file lock poisoned");
                file.append_page(page).expect("can not extend data file");
            }
        }
    }

    fn read_page_from_store (&self, offset: Offset) -> Result<Option<Page>, BCDBError> {
        if offset != offset.this_page () {
            return Err(BCDBError::Corrupted(format!("data or link read is not page aligned {}", offset)))
        }
        self.inner.file.lock().unwrap().read_page(offset)
    }
}

impl PagedFile for AsyncFile {
    #[allow(unused_assignments)]
    fn flush(&mut self) -> Result<(), BCDBError> {
        self.sender.lock().unwrap().send(None)?;
        self.inner.file.lock().unwrap().flush()
    }

    fn len(&self) -> Result<u64, BCDBError> {
        self.inner.file.lock().unwrap().len()
    }

    fn truncate(&mut self, new_len: u64) -> Result<(), BCDBError> {
        let mut cache = self.inner.cache.lock().unwrap();
        cache.reset_len(new_len);
        self.inner.file.lock().unwrap().truncate(new_len)
    }

    fn sync(&self) -> Result<(), BCDBError> {
        self.inner.file.lock().unwrap().sync()
    }

    fn read_page(&self, offset: Offset) -> Result<Option<Page>, BCDBError> {
        let mut cache = self.inner.cache.lock().unwrap();
        if let Some(page) = cache.get(offset) {
            return Ok(Some(page));
        }
        if let Some(page) = self.read_page_from_store(offset)? {
            cache.cache(offset, Arc::new(page.clone()));
            return Ok(Some(page));
        }
        Ok(None)
    }

    fn append_page(&mut self, page: Page) -> Result<u64, BCDBError> {
        self.sender.lock().unwrap().send(Some(page.clone()))?;
        let mut cache = self.inner.cache.lock().unwrap();
        let len = cache.append(page);
        Ok(len)
    }

    fn write_page(&mut self, _: Offset, _: Page) -> Result<u64, BCDBError> {
        unimplemented!()
    }

    fn shutdown (&mut self) {
        self.sender.lock().unwrap().send(None).unwrap();
        let _cache = self.inner.cache.lock().unwrap();
        self.inner.run.store(false, Ordering::Release);
    }
}
