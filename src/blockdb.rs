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
use keyfile::KeyFile;
use datafile::DataFile;
use error::BCSError;

use std::sync::{Mutex,Arc};
use std::io::{Read,Write,Seek};

pub trait BlockDBFactory {
    fn new_blockdb (name: &str) -> Result<BlockDB, BCSError>;
}

pub trait RW : Read + Write + Seek + Send {
    fn len (&mut self) -> Result<usize, BCSError>;
    fn truncate(&mut self, new_len: usize) -> Result<(), BCSError>;
    fn sync (&self) -> Result<(), BCSError>;
}

pub trait DBFile : BlockFile {
    fn flush(&mut self) -> Result<(), BCSError>;
    fn sync (&mut self) -> Result<(), BCSError>;
    fn truncate(&mut self, offset: Offset) -> Result<(), BCSError>;
    fn len(&mut self) -> Result<Offset, BCSError>;
}

pub trait BlockFile {
    fn read_block (&self, offset: Offset) -> Result<Arc<Block>, BCSError>;
}


/// The database block layer
pub struct BlockDB {
    table: KeyFile,
    data: DataFile,
    log: Arc<Mutex<LogFile>>
}

impl BlockDB {
    pub fn new (mut table: KeyFile, mut data: DataFile, log: Arc<Mutex<LogFile>>) -> Result<BlockDB, BCSError> {
        BlockDB::check(&mut table, &[0xBC, 0xDB])?;
        BlockDB::check(&mut data, &[0xBC, 0xDA])?;
        BlockDB::check_log(log.clone(), &[0xBC, 0x00])?;
        let mut blockdb = BlockDB{table, data, log};
        blockdb.recover()?;
        blockdb.batch()?;
        Ok(blockdb)
    }

    fn check_log(log: Arc<Mutex<LogFile>>, magic: &[u8]) -> Result<(), BCSError> {
        let mut file = log.lock().unwrap();
        if file.len()?.as_usize() > 0 {
            let offset = Offset::new(0)?;
            let first = file.read_block(offset)?;
            if &first.payload [0..2] != magic {
                return Err(BCSError::BadMagic);
            }
        }
        Ok(())
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
        let mut log = self.log.lock().unwrap();
        if log.len()?.as_usize() > 0 {
            let mut offset = Offset::new(0)?;
            let first = log.read_block(offset)?;

            let mut size = [0u8; 4];

            first.read(2, &mut size)?;
            let data_len = Offset::from_slice(&size)?;
            self.data.truncate(data_len)?;

            first.read(6, &mut size)?;
            let table_len = Offset::from_slice(&size)?;
            self.table.truncate(table_len)?;

            offset = Offset::new(BLOCK_SIZE)?;
            while let Ok(block) = log.read_block(offset) {
                if block.offset.as_usize() < table_len.as_usize() {
                    self.table.write_block(block);
                }
                offset = Offset::new(offset.as_usize() + BLOCK_SIZE)?;
            }
            log.truncate(Offset::new(0)?)?;
            log.sync()?;
        }
        Ok(())
    }

    pub fn batch (&mut self)  -> Result<(), BCSError> {
        self.data.flush()?;
        self.data.sync()?;
        self.table.flush()?;
        self.table.sync()?;
        let data_len = self.data.len()?;
        let table_len = self.table.len()?;

        let mut log = self.log.lock().unwrap();
        log.truncate(Offset::new(0)?)?;
        log.reset();

        let mut first = Block::new(Offset::new(0)?);
        first.append(&[0xBC, 0x00])?;
        let mut size = [0u8; 6];
        data_len.serialize(&mut size);
        first.append(&size)?;
        table_len.serialize(&mut size);
        first.append(&size)?;


        log.append_block(Arc::new(first))?;
        log.sync()?;

        Ok(())
    }

    pub fn shutdown (&mut self) {
        self.data.shutdown();
        self.table.shutdown();
    }

    pub fn write_table_block(&mut self, block: Block) -> Result<(), BCSError> {
        let br = Arc::new(block);
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

pub struct BlockIterator<'file> {
    blocknumber: usize,
    file: &'file BlockFile
}

impl<'file> BlockIterator<'file> {
    pub fn new (file: &'file BlockFile) -> BlockIterator {
        BlockIterator{blocknumber: 0, file}
    }
}

impl<'file> Iterator for BlockIterator<'file> {
    type Item = Arc<Block>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.blocknumber < (1 << 47) / BLOCK_SIZE {
            let offset = Offset::new(self.blocknumber*BLOCK_SIZE).unwrap();
            if let Ok(block) = self.file.read_block(offset) {
                self.blocknumber += 1;
                return Some(block);
            }
        }
        None
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