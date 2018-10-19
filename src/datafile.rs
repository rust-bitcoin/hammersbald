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

use pagedfile::{FileOps, PagedFile};
use format::{DataFormatter, Payload, Data, IndexedData};

use error::BCDBError;
use pref::PRef;

/// file storing indexed and referred data
pub struct DataFile {
    formatter: DataFormatter
}

impl DataFile {
    /// create new file
    pub fn new(file: Box<PagedFile>, previous: PRef) -> Result<DataFile, BCDBError> {
        let start = PRef::from(file.len()?);
        Ok(DataFile{ formatter: DataFormatter::new(file, start, previous)?})
    }

    /// initialize
    pub fn init(&mut self) -> Result<(), BCDBError> {
        self.formatter.append_slice (&[0xBC, 0xDA], PRef::from(2))
    }

    /// shutdown
    pub fn shutdown (&mut self) {
        self.formatter.shutdown()
    }

    /// get a stored content at pref
    pub fn get_payload(&self, pref: PRef) -> Result<Payload, BCDBError> {
        self.formatter.get_payload(pref)
    }

    /// append indexed data
    pub fn append_data (&mut self, key: &[u8], data: &[u8], referred: &Vec<PRef>) -> Result<PRef, BCDBError> {
        let indexed = IndexedData { key: key.to_vec(), data: Data{data: data.to_vec(), referred: referred.clone()} };
        self.formatter.append_payload(Payload::Indexed(indexed))
    }

    /// append referred data
    pub fn append_referred (&mut self, data: &[u8], referred: &Vec<PRef>) -> Result<PRef, BCDBError> {
        let data = Data{data: data.to_vec(), referred: referred.clone()};
        self.formatter.append_payload(Payload::Referred(data))
    }

    /// truncate file
    pub fn truncate(&mut self, pref: u64) -> Result<(), BCDBError> {
        self.formatter.truncate (pref)
    }

    /// flush buffers
    pub fn flush (&mut self) -> Result<(), BCDBError> {
        self.formatter.flush()
    }

    /// sync file on file system
    pub fn sync (&self) -> Result<(), BCDBError> {
        self.formatter.sync()
    }

    /// get file length
    pub fn len (&self) -> Result<u64, BCDBError> {
        self.formatter.len()
    }
}

