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
use HammersbaldError;
use HammersbaldIterator;

use bitcoin::{
    BitcoinHash,
    util::hash::Sha256dHash,
    consensus::{Decoder, Encoder, Decodable, Encodable, encode}
};

use std::{
    io::Cursor,
    error::Error
};

/// Bitcoin adaptor
pub struct BitcoinAdaptor {
    hammersbald: Box<HammersbaldAPI>
}

impl BitcoinAdaptor {
    /// Create a new Adaptor
    pub fn new (hammersbald: Box<HammersbaldAPI>) -> BitcoinAdaptor {
        BitcoinAdaptor { hammersbald }
    }

    /// Store some bitcoin object that has a bitcoin hash
    pub fn put_hash_keyed<T: ? Sized + BitcoinHash>(&mut self, encodable: &T) -> Result<PRef, Box<Error>>
        where T: Encodable<Vec<u8>> {
        Ok(self.hammersbald.put_keyed(&encodable.bitcoin_hash().as_bytes()[..], encode(encodable)?.as_slice())?)
    }

    /// Retrieve a bitcoin_object with its hash
    pub fn get_hash_keyed<T: ? Sized + BitcoinHash>(&self, id: &Sha256dHash) -> Result<Option<(PRef, T)>, Box<Error>>
        where T: Decodable<Cursor<Vec<u8>>>{
        if let Some((pref, data)) = self.hammersbald.get_keyed(&id.as_bytes()[..])? {
            return Ok(Some((pref, decode(data)?)))
        }
        Ok(None)
    }

    /// Store some bitcoin object
    pub fn put_encodable<T: ? Sized>(&mut self, encodable: &T) -> Result<PRef, Box<Error>>
        where T: Encodable<Vec<u8>> {
        Ok(self.hammersbald.put(encode(encodable)?.as_slice())?)
    }

    /// Retrieve some bitcoin object
    pub fn get_encodable<T: ? Sized + BitcoinHash>(&self, pref: PRef) -> Result<T, Box<Error>>
        where T: Decodable<Cursor<Vec<u8>>>{
        let (_, data) = self.hammersbald.get(pref)?;
        Ok(decode(data)?)
    }
}

impl HammersbaldAPI for BitcoinAdaptor {
    fn batch(&mut self) -> Result<(), HammersbaldError> {
        self.hammersbald.batch()
    }

    fn shutdown(&mut self) {
        self.hammersbald.shutdown()
    }

    fn put_keyed(&mut self, key: &[u8], data: &[u8]) -> Result<PRef, HammersbaldError> {
        self.hammersbald.put_keyed(key, data)
    }

    fn get_keyed(&self, key: &[u8]) -> Result<Option<(PRef, Vec<u8>)>, HammersbaldError> {
        self.hammersbald.get_keyed(key)
    }

    fn put(&mut self, data: &[u8]) -> Result<PRef, HammersbaldError> {
        self.hammersbald.put(data)
    }

    fn get(&self, pref: PRef) -> Result<(Vec<u8>, Vec<u8>), HammersbaldError> {
        self.hammersbald.get(pref)
    }

    fn forget(&mut self, key: &[u8]) -> Result<(), HammersbaldError> {
        self.hammersbald.forget(key)
    }

    fn iter(&self) -> HammersbaldIterator {
        self.hammersbald.iter()
    }
}

pub fn decode<T: ? Sized>(data: Vec<u8>) -> Result<T, Box<Error>>
    where T: Decodable<Cursor<Vec<u8>>> {
    let mut decoder  = Cursor::new(data);
    Ok(Decodable::consensus_decode(&mut decoder)?)
}

pub fn encode<T: ? Sized>(data: &T) -> Result<Vec<u8>, Box<Error>>
    where T: Encodable<Vec<u8>> {
    let mut result = vec!();
    data.consensus_encode(&mut result)?;
    Ok(result)
}

impl<D: Decoder> Decodable<D> for PRef {
    fn consensus_decode(d: &mut D) -> Result<PRef, encode::Error> {
        d.read_u64().map(u64::from_le).map(PRef::from)
    }
}

impl<S: Encoder> Encodable<S> for PRef {
    fn consensus_encode(&self, s: &mut S) -> Result<(), encode::Error> {
        s.emit_u64(self.as_u64().to_le())
    }
}

#[cfg(test)]
mod test {
    ///! Example use and test

    extern crate hex;

    use bitcoin::blockdata::{
        block::{Block, BlockHeader},
        transaction::Transaction,
        constants::genesis_block
    };
    use bitcoin::network::constants::Network;
    use transient;
    use super::*;

    // Example how to extend a bitcoin structure for storage
    #[derive(Eq, PartialEq, Debug)]
    struct LinkedBlock {
        block: Block,
        height: u32,
        previous: PRef
    }

    // need to implement if put_hash_keyed and get_hash_keyed should be used
    impl BitcoinHash for LinkedBlock {
        fn bitcoin_hash(&self) -> Sha256dHash {
            self.block.bitcoin_hash()
        }
    }

    // implement encoder. tedious just repeat the consensus_encode lines
    impl<S: Encoder> Encodable<S> for LinkedBlock {
        fn consensus_encode(&self, s: &mut S) -> Result<(), encode::Error> {
            self.block.consensus_encode(s)?;
            self.height.consensus_encode(s)?;
            self.previous.consensus_encode(s)?;
            Ok(())
        }
    }

    // implement decoder. tedious just repeat the consensus_encode lines
    impl<D: Decoder> Decodable<D> for LinkedBlock {
    fn consensus_decode(d: &mut D) -> Result<LinkedBlock, encode::Error> {
        Ok(LinkedBlock {
            block: Decodable::consensus_decode(d)?,
            height: Decodable::consensus_decode(d)?,
            previous: Decodable::consensus_decode(d)? })
        }
    }

    #[test]
    pub fn bitcoin_test () {
        // create a transient hammersbald
        let db = transient(1).unwrap();
        // promote to a bitcoin adapter
        let mut bdb = BitcoinAdaptor::new(db);

        // example transaction
        let tx = decode::<Transaction> (hex::decode("02000000000101ed30ca30ee83f13579da294e15c9d339b35d33c5e76d2fda68990107d30ff00700000000006db7b08002360b0000000000001600148154619cb0e7513fcdb1eb90cc9f86f3793b9d8ec382ff000000000022002027a5000c7917f785d8fc6e5a55adfca8717ecb973ebb7743849ff956d896a7ed04004730440220503890e657773607fb05c9ef4c4e73b0ab847497ee67b3b8cefb3688a73333180220066db0ca943a5932f309ac9d4f191300711a5fc206d7c3babd85f025eac30bca01473044022055f05c3072dfd389104af1f5ccd56fb5433efc602694f1f384aab703c77ac78002203c1133981d66dc48183e72a19cc0974b93002d35ad7d6ee4278d46b4e96f871a0147522102989711912d88acf5a4a18081104f99c2f8680a7de23f829f28db31fdb45b7a7a2102f0406fa1b49a9bb10c191fd83e2359867ecdace5ea990ce63d11478ed5877f1852ae81534220").unwrap()).unwrap();

        // store the transaction without associating a key
        let txref = bdb.put_encodable(&tx).unwrap();
        // retrieve by direct reference
        let tx2 = bdb.get_encodable::<Transaction>(txref).unwrap();
        assert_eq!(tx, tx2);

        // store the transaction with its hash as key
        let txref2 = bdb.put_hash_keyed(&tx).unwrap();
        // retrieve by hash
        if let Some((pref, tx3)) = bdb.get_hash_keyed::<Transaction>(&tx.bitcoin_hash()).unwrap() {
            assert_eq!(pref, txref2);
            assert_eq!(tx3, tx);
        }
        else {
            panic!("can not find tx");
        }

        let genesis = genesis_block(Network::Bitcoin);
        // store the genesist block
        let gref = bdb.put_hash_keyed(&genesis).unwrap();
        // find it
        if let Some((_, block)) = bdb.get_hash_keyed::<Block>(&genesis.bitcoin_hash()).unwrap() {
            assert_eq!(block, genesis);
        }
        else {
            panic!("can not find genesis block");
        }
        // find just the header
        if let Some((_, header)) = bdb.get_hash_keyed::<BlockHeader>(&genesis.bitcoin_hash()).unwrap() {
            assert_eq!(header, genesis.header);
        }
        else {
            panic!("can not find genesis header");
        }
        // block 1
        let block1 = decode::<Block>(hex::decode("010000006fe28c0ab6f1b372c1a6a246ae63f74f931e8365e15a089c68d6190000000000982051fd1e4ba744bbbe680e1fee14677ba1a3c3540bf7b1cdb606e857233e0e61bc6649ffff001d01e362990101000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0704ffff001d0104ffffffff0100f2052a0100000043410496b538e853519c726a2c91e61ec11600ae1390813a627c66fb8be7947be63c52da7589379515d4e0a604f8141781e62294721166bf621e73a82cbf2342c858eeac00000000").unwrap()).unwrap();

        // custom type that also stores hight and a reference to previous block
        let lb = LinkedBlock { block: block1.clone(), height: 1, previous: gref };

        // store custom type
        bdb.put_hash_keyed(&lb).unwrap();
        // find
        if let Some((_, block1test)) = bdb.get_hash_keyed(&block1.bitcoin_hash()).unwrap() {
            assert_eq!(lb, block1test);
            let gen = bdb.get_encodable(lb.previous).unwrap();
            assert_eq!(genesis, gen);
        }
        else {
            panic!("can not find block1");
        }
    }
}