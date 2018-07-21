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
//! # The key file
//! Specific implementation details to key file
//!

use asyncfile::AsyncFile;
use logfile::LogFile;
use pagedb::{RW, DBFile, PageIterator, PageFile};
use page::Page;
use error::BCSError;
use types::Offset;

use std::sync::{Mutex, Arc};

/// The key file
pub struct KeyFile {
    async_file: AsyncFile
}

impl KeyFile {
    pub fn new(rw: Box<RW>, log_file: Arc<Mutex<LogFile>>) -> KeyFile {
        KeyFile{async_file: AsyncFile::new(rw, Some(log_file))}
    }

    pub fn write_page(&self, page: Arc<Page>) {
        self.async_file.write_page(page)
    }

    pub fn log_file (&self) -> Arc<Mutex<LogFile>> {
        self.async_file.log_file().unwrap()
    }

    pub fn append_page (&self, page: Arc<Page>) {
        self.async_file.append_page(page)
    }

    pub fn shutdown (&mut self) {
        self.async_file.shutdown()
    }
}

impl DBFile for KeyFile {
    fn flush(&mut self) -> Result<(), BCSError> {
        self.async_file.flush()
    }

    fn sync(&mut self) -> Result<(), BCSError> {
        self.async_file.sync()
    }

    fn truncate(&mut self, offset: Offset) -> Result<(), BCSError> {
        self.async_file.truncate(offset)
    }

    fn len(&mut self) -> Result<Offset, BCSError> {
        self.async_file.len()
    }
}

impl PageFile for KeyFile {
    fn read_page(&self, offset: Offset) -> Result<Arc<Page>, BCSError> {
        self.async_file.read_page(offset)
    }
}