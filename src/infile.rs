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
//! # in file store
//!
//! Implements persistent store

use error::BCDBError;
use logfile::LogFile;
use table::TableFile;
use datafile::{DataFile, LinkFile};
use bcdb::{BCDBFactory, BCDB};
use types::Offset;
use page::{PageFile,Page};
use rolled::RolledFile;

use std::sync::{Mutex,Arc};

const KEY_CHUNK_SIZE: u64 = 128*1024*1024;
const DATA_CHUNK_SIZE: u64 = 1024*1024*1024;
const LOG_CHUNK_SIZE: u64 = 1024*1024*1024;

/// Implements persistent storage
pub struct InFile {
    file: RolledFile
}

impl InFile {
    /// create a new DB in memory for tests
    pub fn new (file: RolledFile) -> InFile {
        InFile {file: file}
    }
}

impl BCDBFactory for InFile {
    fn new_db (name: &str) -> Result<BCDB, BCDBError> {
        let log = Arc::new(Mutex::new(LogFile::new(Box::new(
            RolledFile::new(name.to_string(), "lg".to_string(), true, LOG_CHUNK_SIZE)?))));
        let table = TableFile::new(Box::new(InFile::new(
            RolledFile::new(name.to_string(), "tb".to_string(), false, KEY_CHUNK_SIZE)?
        )), log)?;
        let link = LinkFile::new(Box::new(RolledFile::new(name.to_string(), "bl".to_string(), true, DATA_CHUNK_SIZE)?))?;
        let data = DataFile::new(Box::new(RolledFile::new(name.to_string(), "bc".to_string(), true, DATA_CHUNK_SIZE)?))?;

        BCDB::new(table, data, link)
    }
}

impl PageFile for InFile {
    fn flush(&mut self) -> Result<(), BCDBError> {
        self.file.flush()
    }

    fn len(&self) -> Result<u64, BCDBError> {
        self.file.len()
    }

    fn truncate(&mut self, new_len: u64) -> Result<(), BCDBError> {
        self.file.truncate(new_len)
    }

    fn sync(&self) -> Result<(), BCDBError> {
        self.file.sync()
    }

    fn read_page(&self, offset: Offset) -> Result<Option<Page>, BCDBError> {
        self.file.read_page(offset)
    }

    fn append_page(&mut self, page: Page) -> Result<(), BCDBError> {
        self.file.append_page(page)
    }

    fn write_page(&mut self, offset: Offset, page: Page) -> Result<(), BCDBError> {
        self.file.write_page(offset, page)
    }
}