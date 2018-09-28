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
//! # Bitcoin specific use of this blockchain db
//!

use bcdb::BCDB;
use types::{U24, Offset};
use datafile::Content;
use error::BCDBError;

use bitcoin::blockdata::block::{BlockHeader, Block};
use bitcoin::blockdata::transaction::Transaction;
use bitcoin::network::serialize::BitcoinHash;
use bitcoin::network::encodable::{ConsensusDecodable, ConsensusEncodable};
use bitcoin::network::serialize::{RawDecoder, RawEncoder};
use bitcoin::network::serialize::serialize;
use bitcoin::util::hash::Sha256dHash;

use std::io::Cursor;

struct BitcoinAdapter<'db> {
    bcdb: &'db mut BCDB
}

impl<'db> BitcoinAdapter<'db> {
    pub fn new (bcdb: &mut BCDB) -> BitcoinAdapter {
        BitcoinAdapter {bcdb}
    }

    /// Insert a block header
    pub fn insert_header (&mut self, header: &BlockHeader, extension: &Vec<Vec<u8>>) -> Result<Offset, BCDBError> {
        let key = encode(&header.bitcoin_hash())?;
        let mut serialized_header = encode(header)?;

        let mut number_of_data = [0u8;3];
        U24::new(extension.len())?.serialize(&mut number_of_data);
        serialized_header.append(&mut number_of_data.to_vec());

        for d in extension {
            let offset = self.bcdb.put_content(Content::Extension(d.clone()))?;
            let mut did = [0u8;6];
            offset.serialize(&mut did);
            serialized_header.append(&mut did.to_vec());
        }

        let number_of_transactions = [0u8;3];
        serialized_header.append(&mut number_of_transactions.to_vec());

        self.bcdb.put(key.as_slice(), serialized_header.as_slice())
    }

    /// Fetch a header by its id
    pub fn fetch_header (&self, id: &Sha256dHash)  -> Result<Option<(BlockHeader, Vec<Vec<u8>>)>, BCDBError> {
        let key = encode(id)?;
        if let Some(stored) = self.bcdb.get(key.as_slice())? {
            let header = decode(stored.as_slice()[0..80].to_vec())?;
            let mut extension = Vec::new();
            let n_extensions = U24::from_slice(&stored.as_slice()[80..83])?.as_usize();
            for i in 0 .. n_extensions {
                let offset = Offset::from_slice(&stored.as_slice()[83+i*6 .. 83+(i+1)*6])?;
                if let Content::Extension(data) = self.bcdb.get_content(offset)? {
                    extension.push(data);
                }
                    else {
                        return Err(BCDBError::Corrupted(format!("can not find app data extension {}", offset.as_u64())));
                    }
            }

            Ok(Some((header, extension)))
        }
            else {
                Ok(None)
            }
    }

    /// insert a block
    pub fn insert_block(&mut self, block: &Block, extension: &Vec<Vec<u8>>) -> Result<Offset, BCDBError> {
        let key = encode(&block.bitcoin_hash())?;
        let mut serialized_block = encode(&block.header)?;

        let mut number_of_data = [0u8;3];
        U24::new(extension.len())?.serialize(&mut number_of_data);
        serialized_block.append(&mut number_of_data.to_vec());

        for d in extension {
            let offset = self.bcdb.put_content(Content::Extension(d.clone()))?;
            let mut did = [0u8;6];
            offset.serialize(&mut did);
            serialized_block.append(&mut did.to_vec());
        }

        let mut number_of_transactions = [0u8;3];
        U24::new(block.txdata.len())?.serialize(&mut number_of_transactions);
        serialized_block.append(&mut number_of_transactions.to_vec());

        for t in &block.txdata {
            let offset = self.bcdb.put(
                &encode(&t.txid())?.as_slice(), &encode(t)?.as_slice())?;
            let mut did = [0u8;6];
            offset.serialize(&mut did);
            serialized_block.append(&mut did.to_vec());
        }

        let offset = self.bcdb.put(key.as_slice(),serialized_block.as_slice())?;
        Ok(offset)
    }

    /// Fetch a block by its id
    pub fn fetch_block (&self, id: &Sha256dHash)  -> Result<Option<(Block, Vec<Vec<u8>>)>, BCDBError> {
        let key = encode(id)?;
        if let Some(stored) = self.bcdb.get(&key.as_slice())? {
            let header = decode(stored.as_slice()[0..80].to_vec())?;
            let mut extension = Vec::new();
            let n_extensions = U24::from_slice(&stored.as_slice()[80..83])?.as_usize();
            for i in 0 .. n_extensions {
                let offset = Offset::from_slice(&stored.as_slice()[83+i*6 .. 83+(i+1)*6])?;
                if let Content::Extension(data) = self.bcdb.get_content(offset)? {
                    extension.push(data);
                }
                    else {
                        return Err(BCDBError::Corrupted(format!("can not find app data extension {}", offset.as_u64())));
                    }
            }

            let n_transactions = U24::from_slice(&stored.as_slice()[83+n_extensions*6 .. 83+n_extensions*6+3])?.as_usize();
            let mut txdata: Vec<Transaction> = Vec::new();
            for i in 0 .. n_transactions {
                let offset = Offset::from_slice(&stored.as_slice()[83+n_extensions*6+3+i*6 .. 83+n_extensions*6+3+(i+1)*6])?;
                if let Content::Data (_, tx) = self.bcdb.get_content(offset)? {
                    txdata.push(decode(tx)?);
                }
                    else {
                        return Err(BCDBError::Corrupted(format!("can not find transaction of a block {}", offset.as_u64())));
                    }
            }

            Ok(Some((Block{header, txdata}, extension)))
        }
            else {
                Ok(None)
            }
    }
}


fn decode<T: ? Sized>(data: Vec<u8>) -> Result<T, BCDBError>
    where T: ConsensusDecodable<RawDecoder<Cursor<Vec<u8>>>> {
    let mut decoder: RawDecoder<Cursor<Vec<u8>>> = RawDecoder::new(Cursor::new(data));
    ConsensusDecodable::consensus_decode(&mut decoder).map_err(|e| { BCDBError::Util(e) })
}

fn encode<T: ? Sized>(data: &T) -> Result<Vec<u8>, BCDBError>
    where T: ConsensusEncodable<RawEncoder<Cursor<Vec<u8>>>> {
    serialize(data).map_err(|e| { BCDBError::Util(e) })
}
