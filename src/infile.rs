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
use std::io;
use std::fs::{File, OpenOptions};
use std::sync::{Mutex,Arc};

/// in file store
pub struct InFile {
    data: File
}

impl InFile {
    /// create a new DB in memory for tests
    pub fn new (file: File) -> InFile {
        InFile {data: file}
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
        Ok(self.data.flush()?)
    }

    fn len(&mut self) -> Result<u64, BCSError> {
        Ok(self.data.seek(SeekFrom::End(0))?)
    }

    fn truncate(&mut self, len: u64) -> Result<(), BCSError> {
        Ok(self.data.set_len(len)?)
    }

    fn sync(&self) -> Result<(), BCSError> { Ok(self.data.sync_data()?) }

    fn read_page (&mut self, offset: Offset) -> Result<Page, BCSError> {
        let mut buffer = [0u8; PAGE_SIZE];
        let len = self.len()?;
        if offset.as_u64() >= len {
            return Err(BCSError::InvalidOffset);
        }
        self.seek(SeekFrom::Start(offset.as_u64()))?;
        self.read(&mut buffer)?;
        let page = Page::from_buf(buffer);
        Ok(page)
    }

    fn append_page(&mut self, page: Page) -> Result<(), BCSError> {
        self.write(&page.finish()[..])?;
        Ok(())
    }

    fn write_page(&mut self, page: Page) -> Result<(), BCSError> {
        self.seek(SeekFrom::Start(page.offset.as_u64()))?;
        self.write(&page.finish()[..])?;
        Ok(())
    }
}

impl Read for InFile {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, io::Error> {
       self.data.read(buf)
    }
}

impl Write for InFile {
    fn write(&mut self, buf: &[u8]) -> Result<usize, io::Error> {
        self.data.write(buf)
    }

    fn flush(&mut self) -> Result<(), io::Error> {
        self.data.flush()
    }
}

impl Seek for InFile {
    fn seek(&mut self, pos: SeekFrom) -> Result<u64, io::Error> {
        self.data.seek(pos)
    }
}
