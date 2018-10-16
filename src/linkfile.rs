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
use datafile::{DataFileImpl, DataIterator, DataPageIterator, DataEntry, Content};

/// file storing data link chains from hash table to data
pub struct LinkFile {
    im: DataFileImpl
}

impl LinkFile {
    /// create new file
    pub fn new(rw: Box<PagedFile>) -> Result<LinkFile, BCDBError> {
        Ok(LinkFile{im: DataFileImpl::new(rw)?})
    }

    /// initialize
    pub fn init(&mut self) -> Result<(), BCDBError> {
        self.im.init ([0xBC, 0xDB])
    }

    /// shutdown
    pub fn shutdown (&mut self) {
        self.im.shutdown()
    }

    /// get an iterator of links
    pub fn iter<'a>(&'a self) -> impl Iterator<Item=(Offset, Vec<(u32, Offset)>, Offset)> + 'a {
        LinkFileIterator::new(DataIterator::new(
            DataPageIterator::new(&self.im, 0), 2))
    }


    /// get a stored content at offset
    pub fn get_content(&self, offset: Offset) -> Result<Option<Content>, BCDBError> {
        self.im.get_content(offset)
    }

    /// append data
    pub fn append_link (&mut self, links: Vec<(u32, Offset)>, next: Offset) -> Result<Offset, BCDBError> {
        self.im.append(DataEntry::new_link(links, next))
    }

    /// get a link
    pub fn get_link(&self, offset: Offset) -> Result<(Vec<(u32, Offset)>, Offset), BCDBError> {
        match self.im.get_content(offset)? {
            Some(Content::Link(current, next)) => Ok((current, next)),
            Some(_) | None => Err(BCDBError::Corrupted(format!("can not find link {}", offset)))
        }
    }

    /// truncate file
    pub fn truncate(&mut self, offset: u64) -> Result<(), BCDBError> {
        self.im.truncate (offset)
    }

    /// flush buffers
    pub fn flush (&mut self) -> Result<(), BCDBError> {
        self.im.flush()
    }

    /// sync file on file system
    pub fn sync (&self) -> Result<(), BCDBError> {
        self.im.sync()
    }

    /// get file length
    pub fn len (&self) -> Result<u64, BCDBError> {
        self.im.len()
    }
}

struct LinkFileIterator<'a> {
    inner: DataIterator<'a>
}

impl<'a> LinkFileIterator<'a> {
    fn new (inner: DataIterator) -> LinkFileIterator {
        LinkFileIterator{inner}
    }
}

impl<'a> Iterator for LinkFileIterator<'a> {
    type Item = (Offset, Vec<(u32, Offset)>, Offset);

    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        match self.inner.next() {
            Some((offset, Content::Link(current, next))) => {
                Some (
                    (offset, current.iter().fold(Vec::new(), |mut a, e| {a.push((e.0, e.1)); a}), next))
            },
            Some(_) => None,
            None => None
        }
    }
}
