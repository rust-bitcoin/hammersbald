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
use pagedfile::{PagedFile, PagedFileAppender};
use format::{Envelope, Payload, Data, IndexedData, Link};
use error::BCDBError;
use pref::PRef;

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
            let appender = PagedFileAppender::new(file, PRef::from(0), PRef::invalid());
            return Ok(DataFile{appender})
        }
    }

    /// return an iterator of all payloads
    pub fn payloads<'a>(&'a self) -> impl Iterator<Item=(PRef, Payload)> +'a {
        PayloadIterator::new(&self.appender, self.appender.lep())
    }

    /// shutdown
    pub fn shutdown (&mut self) {
        self.appender.shutdown()
    }

    /// get a stored content at pref
    pub fn get_payload(&self, pref: PRef) -> Result<Payload, BCDBError> {
        let envelope = self.appender.read_envelope(pref)?;
        Ok(envelope.payload)
    }

    /// append link
    pub fn append_link (&mut self, link: Link) -> Result<PRef, BCDBError> {
        let envelope = Envelope{payload: Payload::Link(link), previous: self.appender.lep()};
        let me = self.appender.position();
        let mut e = vec!();
        envelope.serialize(&mut e);
        self.appender.append(e.as_slice())?;
        self.appender.advance();
        Ok(me)
    }

    /// append indexed data
    pub fn append_data (&mut self, key: &[u8], data: &[u8], referred: &Vec<PRef>) -> Result<PRef, BCDBError> {
        let indexed = IndexedData { key: key.to_vec(), data: Data{data: data.to_vec(), referred: referred.clone()} };
        let envelope = Envelope {previous: self.appender.lep(), payload: Payload::Indexed(indexed)};
        let mut store = vec!();
        envelope.serialize(&mut store);
        let me = self.appender.position();
        self.appender.append(store.as_slice())?;
        self.appender.advance();
        Ok(me)
    }

    /// append referred data
    pub fn append_referred (&mut self, data: &[u8], referred: &Vec<PRef>) -> Result<PRef, BCDBError> {
        let data = Data{data: data.to_vec(), referred: referred.clone()};
        let envelope = Envelope {previous: self.appender.lep(), payload: Payload::Referred(data)};
        let mut store = vec!();
        envelope.serialize(&mut store);
        let me = self.appender.position();
        self.appender.append(store.as_slice())?;
        self.appender.advance();
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

/// Iterate data file content
pub struct PayloadIterator<'f> {
    file: &'f PagedFileAppender,
    pos: PRef
}

impl<'f> PayloadIterator<'f> {
    /// create a new iterator
    pub fn new (file: &'f PagedFileAppender, pos: PRef) -> PayloadIterator<'f> {
        PayloadIterator {file, pos}
    }
}

impl<'f> Iterator for PayloadIterator<'f> {
    type Item = (PRef, Payload);

    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        if self.pos.is_valid() {
            if let Ok(envelope) = self.file.read_envelope(self.pos) {
                let pos = self.pos;
                self.pos = envelope.previous;
                return Some((pos, envelope.payload))
            }
        }
        None
    }
}