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

use error::BCSError;
use bcdb::PageFile;
use logfile::LogFile;
use keyfile::KeyFile;
use datafile::DataFile;
use bcdb::{BCDBFactory, BCDB};
use types::Offset;
use page::{Page,PAGE_SIZE};


use std::io::Read;
use std::io::Write;
use std::io::Seek;
use std::io::SeekFrom;
use std::fs::{File, OpenOptions};
use std::sync::{Mutex,Arc};

/// in file store
pub struct InFile {
    data: Mutex<File>
}

impl InFile {
    /// create a new DB in memory for tests
    pub fn new (file: File) -> InFile {
        InFile {data: Mutex::new(file)}
    }
}

impl BCDBFactory for InFile {
    fn new_db (name: &str) -> Result<BCDB, BCSError> {
        let table_file = OpenOptions::new().read(true).write(true).create(true).open(name.to_owned() + ".tb")?;
        let data_file = OpenOptions::new().read(true).append(true).create(true).open(name.to_owned() + ".bc")?;
        let log_file = OpenOptions::new().read(true).append(true).create(true).open(name.to_owned() + ".lg")?;

        let log = Arc::new(Mutex::new(LogFile::new(Box::new(InFile::new(log_file)))));
        let table = KeyFile::new(Box::new(InFile::new(table_file)), log);
        let data = DataFile::new(Box::new(InFile::new(data_file)))?;

        BCDB::new(table, data)
    }
}

impl PageFile for InFile {
    fn flush(&mut self) -> Result<(), BCSError> {
        Ok(self.data.lock().unwrap().flush()?)
    }

    fn len(&self) -> Result<u64, BCSError> {
        Ok(self.data.lock().unwrap().seek(SeekFrom::End(0))?)
    }

    fn truncate(&mut self, len: u64) -> Result<(), BCSError> {
        Ok(self.data.lock().unwrap().set_len(len)?)
    }

    fn sync(&self) -> Result<(), BCSError> { Ok(self.data.lock().unwrap().sync_data()?) }

    fn read_page (&self, offset: Offset) -> Result<Page, BCSError> {
        let mut data = self.data.lock().unwrap();
        let mut buffer = [0u8; PAGE_SIZE];
        let len = data.seek(SeekFrom::End(0))?;
        if offset.as_u64() >= len {
            return Err(BCSError::InvalidOffset);
        }
        data.seek(SeekFrom::Start(offset.as_u64()))?;
        data.read(&mut buffer)?;
        let page = Page::from_buf(buffer);
        Ok(page)
    }

    fn append_page(&mut self, page: Page) -> Result<(), BCSError> {
        let mut data = self.data.lock().unwrap();
        data.write(&page.finish()[..])?;
        Ok(())
    }

    fn write_page(&mut self, page: Page) -> Result<(), BCSError> {
        let mut data = self.data.lock().unwrap();
        data.seek(SeekFrom::Start(page.offset.as_u64()))?;
        data.write(&page.finish()[..])?;
        Ok(())
    }
}
