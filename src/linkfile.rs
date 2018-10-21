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

use page::{Page, PAGE_SIZE, PAGE_PAYLOAD_SIZE};
use pagedfile::{PagedFile, PagedFileAppender};
use error::BCDBError;
use pref::PRef;
use format::{Link, Envelope, Payload};

/// file storing data link chains from hash table to data
pub struct LinkFile {
    appender: PagedFileAppender
}

impl LinkFile {
    /// create new file
    pub fn new(file: Box<PagedFile>) -> Result<LinkFile, BCDBError> {
        let len = file.len()?;
        if len % PAGE_SIZE as u64 != 0 {
            return Err(BCDBError::Corrupted("link file does not end at page boundary".to_string()));
        }
        if len > 0 {
            if let Some(last) = file.read_page(PRef::from(len - PAGE_SIZE as u64))? {
                let lep = last.read_offset(PAGE_PAYLOAD_SIZE);
                return Ok(LinkFile{appender: PagedFileAppender::new(file, PRef::from(len), lep)});
            }
            else {
                Err(BCDBError::Corrupted("missing first link page".to_string()))
            }
        }
        else {
            let appender = PagedFileAppender::new(file, PRef::from(0), PRef::from(0));
            return Ok(LinkFile{appender})
        }
    }

    /// append data
    pub fn append_link (&mut self, link: Link) -> Result<PRef, BCDBError> {
        let envelope = Envelope{payload: Payload::Link(link), previous: self.appender.advance()};
        let me = self.appender.position();
        let mut e = vec!();
        envelope.serialize(&mut e);
        self.appender.append(e.as_slice())?;
        Ok(me)
    }
}

impl PagedFile for LinkFile {
    fn read_page(&self, pref: PRef) -> Result<Option<Page>, BCDBError> {
        self.appender.read_page(pref)
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

    fn append_page(&mut self, page: Page) -> Result<(), BCDBError> {
        self.appender.append_page(page)
    }

    fn update_page(&mut self, _: Page) -> Result<u64, BCDBError> {
        unimplemented!()
    }

    fn flush(&mut self) -> Result<(), BCDBError> {
        self.appender.flush()
    }
}
