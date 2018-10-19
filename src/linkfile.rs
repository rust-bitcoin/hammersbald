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
//! # The link file
//! Specific implementation details to link file
//!

use pagedfile::{FileOps, PagedFile};
use error::BCDBError;
use pref::PRef;
use format::{Formatter, Link};

/// file storing data link chains from hash table to data
pub struct LinkFile {
    appender: Formatter
}

impl LinkFile {
    /// create new file
    pub fn new(file: Box<PagedFile>) -> Result<LinkFile, BCDBError> {
        let start = PRef::from(file.len()?);
        Ok(LinkFile{ appender: Formatter::new(file, start)? })
    }

    /// initialize
    pub fn init(&mut self) -> Result<(), BCDBError> {
        self.appender.append_slice(&[0xBC, 0xDB], PRef::from(2))
    }

    /// append data
    pub fn append_link (&mut self, link: Link) -> Result<(), BCDBError> {
        let me = self.appender.position();
        let mut ls = Vec::new();
        link.serialize(&mut ls);
        self.appender.append_slice(ls.as_slice(), me)
    }
}

impl FileOps for LinkFile {
    fn flush(&mut self) -> Result<(), BCDBError> {
        self.appender.flush()
    }

    fn len(&self) -> Result<u64, BCDBError> {
        self.appender.len()
    }

    fn truncate(&mut self, new_len: u64) -> Result<(), BCDBError> {
        self.appender.truncate (new_len)
    }

    fn sync(&self) -> Result<(), BCDBError> {
        self.appender.sync()
    }

    fn shutdown(&mut self) {
        self.appender.shutdown()
    }
}