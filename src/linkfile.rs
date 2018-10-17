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

use pagedfile::PagedFile;
use error::BCDBError;
use offset::Offset;
use content::{Link, SliceAppender};

/// file storing data link chains from hash table to data
pub struct LinkFile {
    file: Box<PagedFile>
}

impl LinkFile {
    /// create new file
    pub fn new(file: Box<PagedFile>) -> Result<LinkFile, BCDBError> {
        Ok(LinkFile{ file })
    }

    /// initialize
    pub fn init(&mut self) -> Result<(), BCDBError> {
        SliceAppender::new(self.file.as_mut(), Offset::from(0)).append_slice(&[0xBC, 0xDB])
    }

    /// shutdown
    pub fn shutdown (&mut self) {
        self.file.shutdown()
    }

    /// append data
    pub fn append_link (&mut self, link: Link, next: Offset) -> Result<(), BCDBError> {
        let mut ls = Vec::new();
        link.serialize(&mut ls);
        SliceAppender::new(self.file.as_mut(), next).append_slice(ls.as_slice())
    }

    /// truncate file
    pub fn truncate(&mut self, offset: u64) -> Result<(), BCDBError> {
        self.file.truncate (offset)
    }

    /// flush buffers
    pub fn flush (&mut self) -> Result<(), BCDBError> {
        self.file.flush()
    }

    /// sync file on file system
    pub fn sync (&self) -> Result<(), BCDBError> {
        self.file.sync()
    }

    /// get file length
    pub fn len (&self) -> Result<u64, BCDBError> {
        self.file.len()
    }
}

