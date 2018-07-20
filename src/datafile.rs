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

use asyncfile::AsyncFile;
use blockdb::{RW,DBFile,BlockIterator,BlockFile};
use block::{Block, PAYLOAD_MAX};
use error::BCSError;
use types::Offset;

use std::sync::Arc;

/// The key file
pub struct DataFile {
    async_file: AsyncFile
}

impl DataFile {
    pub fn new(rw: Box<RW>) -> DataFile {
        DataFile{async_file: AsyncFile::new(rw)}
    }

    pub fn append_block (&self, block: Arc<Block>) {
        self.async_file.append_block(block)
    }

    pub fn shutdown (&mut self) {
        self.async_file.shutdown()
    }

    fn block_iter (&self) -> BlockIterator {
        BlockIterator{ blocknumber: 0, file: self }
    }
}

impl DBFile for DataFile {
    fn flush(&mut self) -> Result<(), BCSError> {
        self.async_file.flush()
    }

    fn sync(&mut self) -> Result<(), BCSError> {
        self.async_file.sync()
    }

    fn truncate(&mut self, offset: Offset) -> Result<(), BCSError> {
        self.async_file.truncate(offset)
    }

    fn len(&mut self) -> Result<Offset, BCSError> {
        self.async_file.len()
    }
}

impl BlockFile for DataFile {
    fn read_block(&self, offset: Offset) -> Result<Arc<Block>, BCSError> {
        self.async_file.read_block(offset)
    }
}

/// types of data stored in the data file
pub enum DataType {
    /// no data, just padding the storage blocks with zero bytes
    Padding,
    /// Transaction of application defined data associated with a block
    TransactionOrAppData,
    /// A header or a block of the blockchain
    HeaderOrBlock,
    /// Spillover bucket of the hash table
    TableSpillOver
}

pub struct DataEntry {
    pub data_type: DataType,
    pub content: Vec<u8>
}

pub struct DataIterator<'file> {
    offset: Offset,
    file: &'file DataFile
}

impl<'file> Iterator for DataIterator<'file> {
    type Item = DataEntry;

    fn next(&mut self) -> Option<Self::Item> {
        unimplemented!()
    }
}