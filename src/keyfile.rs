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
//! # The key file
//! Specific implementation details to link file
//!

use page::PageFile;
use error::BCDBError;
use types::Offset;
use datafile::{DataFileImpl, DataIterator, DataPageIterator, DataEntry, Content};

/// file storing data link chains from hash table to data
pub struct KeyFile {
    im: DataFileImpl
}

impl KeyFile {
    /// create new file
    pub fn new(rw: Box<PageFile>) -> Result<KeyFile, BCDBError> {
        Ok(KeyFile{im: DataFileImpl::new(rw, "key")?})
    }

    /// initialize
    pub fn init(&mut self) -> Result<(), BCDBError> {
        self.im.init ([0xBC, 0xDD])
    }

    /// shutdown
    pub fn shutdown (&mut self) {
        self.im.shutdown()
    }

    /// get an iterator of keys
    pub fn iter<'a>(&'a self) -> impl Iterator<Item=(Vec<u8>, Offset)> + 'a {
        KeyFileIterator::new(DataIterator::new(
            DataPageIterator::new(&self.im, 0), 2))
    }


    /// get a stored content at offset
    pub fn get_content(&self, offset: Offset) -> Result<Option<Content>, BCDBError> {
        self.im.get_content(offset)
    }

    /// append key
    pub fn append_key (&mut self, key: &[u8], data: Offset) -> Result<Offset, BCDBError> {
        self.im.append_byte_sized(DataEntry::new_key(key, data))
    }

    /// get a key
    pub fn get_key(&self, offset: Offset) -> Result<(Vec<u8>, Offset), BCDBError> {
        match self.im.get_content(offset)? {
            Some(Content::Key(key, data)) => Ok((key, data)),
            Some(_) | None => Err(BCDBError::Corrupted(format!("can not find link {}", offset)))
        }
    }

    /// clear cache
    pub fn clear_cache(&mut self, len: u64) {
        self.im.clear_cache(len);
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

struct KeyFileIterator<'a> {
    inner: DataIterator<'a>
}

impl<'a> KeyFileIterator<'a> {
    fn new (inner: DataIterator) -> KeyFileIterator {
        KeyFileIterator{inner}
    }
}

impl<'a> Iterator for KeyFileIterator<'a> {
    type Item = (Vec<u8>, Offset);

    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        match self.inner.next() {
            Some(Content::Key(key, offset)) => Some((key, offset)),
            Some(_) => None,
            None => None
        }
    }
}
