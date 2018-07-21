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
use bcdb::RW;
use logfile::LogFile;
use keyfile::KeyFile;
use datafile::DataFile;
use bcdb::{BCDBFactory, BCDB};


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
    pub fn new (file: File) -> InFile {
        InFile {data: file}
    }
}

impl BCDBFactory for InFile {
    fn new_db (name: &str) -> Result<BCDB, BCSError> {
        let table_file = OpenOptions::new().read(true).write(true).create(true).open(name.to_owned() + ".tbl")?;
        let data_file = OpenOptions::new().read(true).append(true).create(true).open(name.to_owned() + ".dat")?;
        let log_file = OpenOptions::new().read(true).append(true).create(true).open(name.to_owned() + ".log")?;

        let log = Arc::new(Mutex::new(LogFile::new(Box::new(InFile::new(log_file)))));
        let table = KeyFile::new(Box::new(InFile::new(table_file)), log);
        let data = DataFile::new(Box::new(InFile::new(data_file)));

        BCDB::new(table, data)
    }
}

impl RW for InFile {
    fn len(&mut self) -> Result<usize, BCSError> {
        Ok(self.data.seek(SeekFrom::End(0))? as usize)
    }

    fn truncate(&mut self, len: usize) -> Result<(), BCSError> {
        Ok(self.data.set_len(len as u64)?)
    }

    fn sync(&self) -> Result<(), BCSError> { Ok(self.data.sync_data()?) }
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
