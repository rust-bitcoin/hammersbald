//
// Copyright 2019 Tamas Blummer
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
//! # Hammersbald bitcoin support
//!

use PRef;
use HammersbaldAPI;
use HammersbaldDataReader;
use HammersbaldDataWriter;

use bitcoin::{
    BitcoinHash,
    blockdata::{
        block::{Block, BlockHeader},
        transaction::Transaction
    },
    util::hash::Sha256dHash,
    consensus::{Decodable, Encodable}
};

use std::{
    io::{Write, Cursor},
    error::Error
};

/// Store a bitcoin header with optional extra data
pub fn store_header(hammersbald: &mut HammersbaldAPI, header: &BlockHeader)  -> Result<PRef, Box<Error>> {
    Ok(hammersbald.put(&header.bitcoin_hash().as_bytes()[..], encode(header)?.as_slice())?)
}

/// Retrieve a bitcoin header
pub fn retrieve_header(hammersbald: &HammersbaldAPI, id: &Sha256dHash) -> Result<Option<(PRef, BlockHeader)>, Box<Error>> {
    if let Some((pref, data)) = hammersbald.get(&id.as_bytes()[..])? {
        return Ok(Some((pref, decode(&data.as_slice()[0..80])?)));
    }
    Ok(None)
}

/// Store a block with optional extra data
pub fn store_block(hammersbald: &mut HammersbaldAPI, block: &Block) -> Result<PRef, Box<Error>> {
    let mut writer = HammersbaldDataWriter::new();
    writer.write(encode(&block.header)?.as_slice())?;
    for tx in &block.txdata {
        writer.write_ref(hammersbald.put_referred(encode(tx)?.as_slice())?);
    }
    Ok(hammersbald.put(&block.bitcoin_hash().as_bytes()[..], writer.as_slice())?)
}

/// Retrieve a block
pub fn retrieve_block(hammersbald: &mut HammersbaldAPI, id: &Sha256dHash) -> Result<Option<(PRef, Block)>, Box<Error>> {
    if let Some((pref, data)) = hammersbald.get(&id.as_bytes()[..])? {
        let header: BlockHeader = decode(&data.as_slice()[0..80])?;
        let mut txdata: Vec<Transaction> = vec!();
        let mut reader = HammersbaldDataReader::new(&data.as_slice()[80..]);
        while let Ok(txref) = reader.read_ref() {
            let (_, data) = hammersbald.get_referred(txref)?;
            txdata.push(decode(data.as_slice())?);
        }
        return Ok(Some((pref, Block{header, txdata})));
    }
    Ok(None)
}

fn decode<'d, T: ? Sized>(data: &'d [u8]) -> Result<T, Box<Error>>
    where T: Decodable<Cursor<&'d [u8]>> {
    let mut decoder  = Cursor::new(data);
    Ok(Decodable::consensus_decode(&mut decoder)?)
}

fn encode<T: ? Sized>(data: &T) -> Result<Vec<u8>, Box<Error>>
    where T: Encodable<Vec<u8>> {
    let mut result = vec!();
    data.consensus_encode(&mut result)?;
    Ok(result)
}
