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
//! # The data file
//! Specific implementation details to data file
//!

use page::{PAGE_PAYLOAD_SIZE, PAGE_SIZE};
use pagedfile::{PagedFile, PagedFileAppender, PagedFileWrite, PagedFileRead};
use format::{Envelope, Payload, Data, IndexedData};
use error::BCDBError;
use pref::PRef;

use byteorder::{ByteOrder, BigEndian};

use std::io::Cursor;

/// file storing indexed and referred data
pub struct DataFile {
    appender: PagedFileAppender
}

impl DataFile {
    /// create new file
    pub fn new(file: Box<PagedFile>) -> Result<DataFile, BCDBError> {
        let len = file.len()?;
        if len % PAGE_SIZE as u64 != 0 {
            return Err(BCDBError::Corrupted("data file does not end at page boundary".to_string()));
        }
        if len > 0 {
            if let Some(last) = file.read_page(PRef::from(len - PAGE_SIZE as u64))? {
                let lep = last.read_offset(PAGE_PAYLOAD_SIZE);
                return Ok(DataFile{appender: PagedFileAppender::new(file, PRef::from(len), lep)});
            }
            else {
                Err(BCDBError::Corrupted("missing first data page".to_string()))
            }
        }
        else {
            let appender = PagedFileAppender::new(file, PRef::from(0), PRef::from(0));
            return Ok(DataFile{appender})
        }
    }

    /// shutdown
    pub fn shutdown (&mut self) {
        self.appender.shutdown()
    }

    /// get a stored content at pref
    pub fn get_payload(&self, pref: PRef) -> Result<Payload, BCDBError> {
        let mut header = [0u8; 9];
        let pref = self.appender.read(pref, &mut header)?;
        let length = BigEndian::read_u24(&header[0..3]) as usize;
        let mut payload = vec!(0u8; length - 9);
        self.appender.read(pref, &mut payload)?;
        Payload::deserialize(&mut Cursor::new(payload))
    }

    /// append indexed data
    pub fn append_data (&mut self, key: &[u8], data: &[u8], referred: &Vec<PRef>) -> Result<PRef, BCDBError> {
        let indexed = IndexedData { key: key.to_vec(), data: Data{data: data.to_vec(), referred: referred.clone()} };
        let mut payload = vec!();
        Payload::Indexed(indexed).serialize(&mut payload);
        let mut envelope= vec!();
        Envelope{previous: self.appender.advance(), payload}.serialize(&mut envelope);
        let me = self.appender.position();
        self.appender.append(&envelope)?;
        Ok(me)
    }

    /// append referred data
    pub fn append_referred (&mut self, data: &[u8], referred: &Vec<PRef>) -> Result<PRef, BCDBError> {
        let referred = Data { data: data.to_vec(), referred: referred.clone() };
        let mut payload = vec!();
        Payload::Referred(referred).serialize(&mut payload);
        let mut envelope= vec!();
        Envelope{previous: self.appender.advance(), payload}.serialize(&mut envelope);
        let me = self.appender.position();
        self.appender.append(&envelope)?;
        Ok(me)
    }

    /// truncate file
    pub fn truncate(&mut self, pref: u64) -> Result<(), BCDBError> {
        self.appender.truncate (pref)
    }

    /// flush buffers
    pub fn flush (&mut self) -> Result<(), BCDBError> {
        self.appender.flush()
    }

    /// sync file on file system
    pub fn sync (&self) -> Result<(), BCDBError> {
        self.appender.sync()
    }

    /// get file length
    pub fn len (&self) -> Result<u64, BCDBError> {
        self.appender.len()
    }
}
