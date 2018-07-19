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
use blockpool::BlockPool;
use error::BCSError;

use std::sync::Arc;

pub trait BlockDBFactory {
    fn new_blockdb (name: String) -> Result<BlockDB, BCSError>;
}

/// The database block layer
pub struct BlockDB {
    table: BlockPool,
    data: BlockPool,
    log: BlockPool
}

impl BlockDB {
    pub fn new (mut table: BlockPool, mut data: BlockPool, mut log: BlockPool) -> Result<BlockDB, BCSError> {
        BlockDB::check(&mut table, &[0xBC, 0xDB])?;
        BlockDB::check(&mut data, &[0xBC, 0xDA])?;
        BlockDB::check(&mut log, &[0xBC, 0x00])?;
        if log.len()?.as_usize() > 0 {
            // TODO recover
            log.truncate(Offset::new(0)?)?;
        }
        let mut blockdb = BlockDB{table, data, log};
        blockdb.recover()?;
        blockdb.batch()?;
        Ok(blockdb)
    }

    fn check(file: &mut BlockPool, magic: &[u8]) -> Result<(), BCSError> {
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
                self.table.write_block(block);
                offset = Offset::new(offset.as_usize() + BLOCK_SIZE)?;
            }
        }
        Ok(())
    }

    pub fn batch (&mut self)  -> Result<(), BCSError> {
        self.data.sync()?;
        self.table.sync()?;
        self.log.truncate(Offset::new(0)?)?;

        let data_len = self.data.len()?;
        let table_len = self.table.len()?;
        let mut first = Block::new(Offset::new(0)?);
        first.append(&[0xBC, 0x00])?;
        let mut size = [0u8; 4];
        data_len.serialize(&mut size);
        first.append(&size)?;
        table_len.serialize(&mut size);
        first.append(&size)?;

        self.log.append_block(Arc::new(first));
        self.log.sync()
    }

    pub fn write_table_block(&self, block: Block) -> Result<(), BCSError> {
        let br = Arc::new(block);
        let prev = self.table.read_block(br.offset)?;
        self.log.append_block(prev);
        self.table.write_block(br);
        Ok(())
    }

    pub fn write_data_block(&self, block: Block) -> Result<(), BCSError> {
        let br = Arc::new(block);
        self.data.write_block(br);
        Ok(())
    }

    pub fn read_table_block (&self, offset: Offset) -> Result<Arc<Block>, BCSError> {
        self.table.read_block(offset)
    }

    pub fn read_data_block (&self, offset: Offset) -> Result<Arc<Block>, BCSError> {
        self.data.read_block(offset)
    }
}
