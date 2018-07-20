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
//! # Three block pools that together implement the db
//!
use block::{Block, BLOCK_SIZE};
use types::Offset;
use asyncfile::AsyncFile;
use logfile::LogFile;
use error::BCSError;

use std::sync::Arc;
use std::io::{Read,Write,Seek};

pub trait BlockDBFactory {
    fn new_blockdb (name: &str) -> Result<BlockDB, BCSError>;
}

pub trait RW : Read + Write + Seek + Send {
    fn len (&mut self) -> Result<usize, BCSError>;
    fn truncate(&mut self, new_len: usize) -> Result<(), BCSError>;
    fn sync (&self) -> Result<(), BCSError>;
}

pub trait DBFile {
    fn flush(&mut self) -> Result<(), BCSError>;
    fn sync (&mut self) -> Result<(), BCSError>;
    fn truncate(&mut self, offset: Offset) -> Result<(), BCSError>;
    fn len(&mut self) -> Result<Offset, BCSError>;
    fn read_block (&self, offset: Offset) -> Result<Arc<Block>, BCSError>;
}

/// The database block layer
pub struct BlockDB {
    table: AsyncFile,
    data: AsyncFile,
    log: LogFile
}

impl BlockDB {
    pub fn new (mut table: AsyncFile, mut data: AsyncFile, mut log: LogFile) -> Result<BlockDB, BCSError> {
        BlockDB::check(&mut table, &[0xBC, 0xDB])?;
        BlockDB::check(&mut data, &[0xBC, 0xDA])?;
        BlockDB::check(&mut log, &[0xBC, 0x00])?;
        let mut blockdb = BlockDB{table, data, log};
        blockdb.recover()?;
        blockdb.batch()?;
        Ok(blockdb)
    }

    fn check(file: &mut DBFile, magic: &[u8]) -> Result<(), BCSError> {
        if file.len()?.as_usize() > 0 {
            let offset = Offset::new(0)?;
            let first = file.read_block(offset)?;
            if &first.payload [0..2] != magic {
                return Err(BCSError::BadMagic);
            }
        }
        Ok(())
    }

    fn recover(&mut self) -> Result<(), BCSError> {
        if self.log.len()?.as_usize() > 0 {
            let mut offset = Offset::new(0)?;
            let first = self.log.read_block(offset)?;

            let mut size = [0u8; 4];

            first.read(2, &mut size)?;
            let data_len = Offset::from_slice(&size)?;
            self.data.truncate(data_len)?;

            first.read(6, &mut size)?;
            let table_len = Offset::from_slice(&size)?;
            self.table.truncate(table_len)?;

            offset = Offset::new(BLOCK_SIZE)?;
            while let Ok(block) = self.log.read_block(offset) {
                if block.offset.as_usize() < table_len.as_usize() {
                    self.table.write_block(block);
                }
                offset = Offset::new(offset.as_usize() + BLOCK_SIZE)?;
            }
            self.log.truncate(Offset::new(0)?)?;
            self.log.sync()?;
        }
        Ok(())
    }

    pub fn batch (&mut self)  -> Result<(), BCSError> {
        self.data.flush()?;
        self.data.sync()?;
        self.table.flush()?;
        self.table.sync()?;
        self.log.truncate(Offset::new(0)?)?;
        self.log.reset();

        let data_len = self.data.len()?;
        let table_len = self.table.len()?;
        let mut first = Block::new(Offset::new(0)?);
        first.append(&[0xBC, 0x00])?;
        let mut size = [0u8; 6];
        data_len.serialize(&mut size);
        first.append(&size)?;
        table_len.serialize(&mut size);
        first.append(&size)?;


        self.log.append_block(Arc::new(first))?;
        self.log.sync()?;

        Ok(())
    }

    pub fn shutdown (&mut self) {
        self.data.shutdown();
        self.table.shutdown();
    }

    pub fn write_table_block(&mut self, block: Block) -> Result<(), BCSError> {
        let br = Arc::new(block);
        let prev = self.table.read_block(br.offset)?;
        self.log.append_block(prev)?;
        self.log.flush()?;
        self.log.sync()?;
        self.table.write_block(br);
        Ok(())
    }

    pub fn append_data_block(&self, block: Block) -> Result<(), BCSError> {
        let br = Arc::new(block);
        self.data.append_block(br);
        Ok(())
    }

    pub fn read_table_block (&self, offset: Offset) -> Result<Arc<Block>, BCSError> {
        self.table.read_block(offset)
    }

    pub fn read_data_block (&self, offset: Offset) -> Result<Arc<Block>, BCSError> {
        self.data.read_block(offset)
    }
}

#[cfg(test)]
mod test {
    extern crate hex;

    use inmemory::InMemory;

    use super::*;
    #[test]
    fn test () {
        let mut blockdb = InMemory::new_blockdb("").unwrap();
        blockdb.shutdown();
    }
}