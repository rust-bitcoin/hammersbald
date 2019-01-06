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
use error::HammersbaldError;
use pref::PRef;

use byteorder::{ByteOrder, BigEndian};

/// file storing indexed and referred data
pub struct DataFile {
    appender: PagedFileAppender
}

impl DataFile {
    /// create new file
    pub fn new(file: Box<PagedFile>) -> Result<DataFile, HammersbaldError> {
        let len = file.len()?;
        if len % PAGE_SIZE as u64 != 0 {
            return Err(HammersbaldError::Corrupted("data file does not end at page boundary".to_string()));
        }
        if len >= PAGE_SIZE as u64 {
            if let Some(last) = file.read_page(PRef::from(len - PAGE_SIZE as u64))? {
                let lep = last.read_pref(PAGE_PAYLOAD_SIZE);
                return Ok(DataFile{appender: PagedFileAppender::new(file, PRef::from(len), lep)});
            }
            else {
                Err(HammersbaldError::Corrupted("missing first data page".to_string()))
            }
        }
        else {
            let appender = PagedFileAppender::new(file, PRef::from(0), PRef::invalid());
            return Ok(DataFile{appender})
        }
    }

    /// return an iterator of all payloads
    pub fn envelopes<'a>(&'a self) -> EnvelopeIterator<'a> {
        EnvelopeIterator::new(&self.appender, self.appender.lep())
    }

    /// shutdown
    pub fn shutdown (&mut self) {
        self.appender.shutdown()
    }

    /// get a stored content at pref
    pub fn get_envelope(&self, mut pref: PRef) -> Result<Envelope, HammersbaldError> {
        let mut len = [0u8;3];
        pref = self.appender.read(pref, &mut len)?;
        let mut buf = vec!(0u8; BigEndian::read_u24(&len) as usize);
        self.appender.read(pref, &mut buf)?;
        Ok(Envelope::deseralize(buf))
    }

    /// append link
    pub fn append_link (&mut self, link: Link) -> Result<PRef, HammersbaldError> {
        let mut payload = vec!();
        Payload::Link(link).serialize(&mut payload);
        let envelope = Envelope::new(payload.as_slice(), self.appender.lep());
        let mut store = vec!();
        envelope.serialize(&mut store);
        let me = self.appender.position();
        self.appender.advance();
        self.appender.append(store.as_slice())?;
        Ok(me)
    }

    /// append indexed data
    pub fn append_data (&mut self, key: &[u8], data: &[u8]) -> Result<PRef, HammersbaldError> {
        let indexed = IndexedData::new(key, Data::new(data));
        let mut payload = vec!();
        Payload::Indexed(indexed).serialize(&mut payload);
        let envelope = Envelope::new(payload.as_slice(), self.appender.lep());
        let mut store = vec!();
        envelope.serialize(&mut store);
        let me = self.appender.position();
        self.appender.advance();
        self.appender.append(store.as_slice())?;
        Ok(me)
    }

    /// append referred data
    pub fn append_referred (&mut self, data: &[u8]) -> Result<PRef, HammersbaldError> {
         let data = Data::new(data);
        let mut payload = vec!();
        Payload::Referred(data).serialize(&mut payload);
        let envelope = Envelope::new(payload.as_slice(), self.appender.lep());
        let mut store = vec!();
        envelope.serialize(&mut store);
        let me = self.appender.position();
        self.appender.advance();
        self.appender.append(store.as_slice())?;
        Ok(me)
    }

    /// truncate file
    pub fn truncate(&mut self, pref: u64) -> Result<(), HammersbaldError> {
        self.appender.truncate (pref)
    }

    /// flush buffers
    pub fn flush (&mut self) -> Result<(), HammersbaldError> {
        self.appender.flush()
    }

    /// sync file on file system
    pub fn sync (&self) -> Result<(), HammersbaldError> {
        self.appender.sync()
    }

    /// get file length
    pub fn len (&self) -> Result<u64, HammersbaldError> {
        self.appender.len()
    }
}

/// Iterate data file content
pub struct EnvelopeIterator<'f> {
    file: &'f PagedFileAppender,
    pos: PRef
}

impl<'f> EnvelopeIterator<'f> {
    /// create a new iterator
    pub fn new (file: &'f PagedFileAppender, pos: PRef) -> EnvelopeIterator<'f> {
        EnvelopeIterator {file, pos}
    }
}

impl<'f> Iterator for EnvelopeIterator<'f> {
    type Item = (PRef, Envelope);

    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        if self.pos.is_valid() {
            let mut pos = self.pos;
            let start = pos;
            let mut len = [0u8;3];
            pos = self.file.read(pos, &mut len).unwrap();
            let mut buf = vec!(0u8; BigEndian::read_u24(&len) as usize);
            self.file.read(pos, &mut buf).unwrap();
            let envelope = Envelope::deseralize(buf);
            self.pos = envelope.previous();
            return Some((start, envelope))
        }
        None
    }
}
