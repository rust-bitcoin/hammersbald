//
// Copyright 2018-2019 Tamas Blummer
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

use bitcoin_hashes::sha256d;

use bitcoin::{
    BitcoinHash,
    consensus::{Decodable, Encodable}
};

use std::{
    error::Error
};
use bitcoin::consensus::{deserialize, serialize};

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
        where T: Encodable {
        Ok(self.hammersbald.put_keyed(&encodable.bitcoin_hash()[..], serialize(encodable).as_slice())?)
    }

    /// Retrieve a bitcoin_object with its hash
    pub fn get_hash_keyed<T: ? Sized + BitcoinHash>(&self, id: &sha256d::Hash) -> Result<Option<(PRef, T)>, Box<Error>>
        where T: Decodable{
        if let Some((pref, data)) = self.hammersbald.get_keyed(&id[..])? {
            return Ok(Some((pref, deserialize(data.as_slice())?)))
        }
        Ok(None)
    }

    /// Store some bitcoin object
    pub fn put_encodable<T: ? Sized>(&mut self, encodable: &T) -> Result<PRef, Box<Error>>
        where T: Encodable {
        Ok(self.hammersbald.put(serialize(encodable).as_slice())?)
    }

    /// Retrieve some bitcoin object
    pub fn get_decodable<T: ? Sized>(&self, pref: PRef) -> Result<(Vec<u8>, T), Box<Error>>
        where T: Decodable {
        let (key, data) = self.hammersbald.get(pref)?;
        Ok((key, deserialize(data.as_slice())?))
    }

    /// Store some bitcoin object with arbitary key
    pub fn put_keyed_encodable<T: ? Sized>( &mut self, key: &[u8], encodable: &T) -> Result<PRef, Box<Error>>
        where T: Encodable {
        Ok(self.hammersbald.put_keyed(key, serialize(encodable).as_slice())?)
    }

    /// Retrieve some bitcoin object with arbitary key
    pub fn get_keyed_decodable<T: ? Sized>(&self, key: &[u8]) -> Result<Option<(PRef, T)>, Box<Error>>
        where T: Decodable{
        if let Some((pref, data)) = self.hammersbald.get_keyed(key)? {
            return Ok(Some((pref, deserialize(data.as_slice())?)));
        }
        Ok(None)
    }

    /// quick check if the db contains a key. This might return false positive.
    pub fn may_have_hash_key (&self, key: &sha256d::Hash) -> Result<bool, Box<Error>> {
        Ok(self.hammersbald.may_have_key(&key[..])?)
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

    fn may_have_key (&self, key: &[u8]) -> Result<bool, HammersbaldError> {
        self.hammersbald.may_have_key(key)
    }

    fn forget(&mut self, key: &[u8]) -> Result<(), HammersbaldError> {
        self.hammersbald.forget(key)
    }

    fn iter(&self) -> HammersbaldIterator {
        self.hammersbald.iter()
    }
}

#[cfg(test)]
mod test {
    ///! Example use and test

    extern crate hex;

    use bitcoin::blockdata::{
        block::{Block},
        transaction::Transaction,
        constants::genesis_block
    };
    use bitcoin::network::constants::Network;
    use transient;
    use super::*;
    use bitcoin::consensus::deserialize;

    #[test]
    pub fn bitcoin_test () {
        // create a transient hammersbald
        let db = transient(1).unwrap();
        // promote to a bitcoin adapter
        let mut bdb = BitcoinAdaptor::new(db);

        // example transaction
        let tx = deserialize::<Transaction> (hex::decode("02000000000101ed30ca30ee83f13579da294e15c9d339b35d33c5e76d2fda68990107d30ff00700000000006db7b08002360b0000000000001600148154619cb0e7513fcdb1eb90cc9f86f3793b9d8ec382ff000000000022002027a5000c7917f785d8fc6e5a55adfca8717ecb973ebb7743849ff956d896a7ed04004730440220503890e657773607fb05c9ef4c4e73b0ab847497ee67b3b8cefb3688a73333180220066db0ca943a5932f309ac9d4f191300711a5fc206d7c3babd85f025eac30bca01473044022055f05c3072dfd389104af1f5ccd56fb5433efc602694f1f384aab703c77ac78002203c1133981d66dc48183e72a19cc0974b93002d35ad7d6ee4278d46b4e96f871a0147522102989711912d88acf5a4a18081104f99c2f8680a7de23f829f28db31fdb45b7a7a2102f0406fa1b49a9bb10c191fd83e2359867ecdace5ea990ce63d11478ed5877f1852ae81534220").unwrap().as_slice()).unwrap();

        // store the transaction without associating a key
        let txref = bdb.put_encodable(&tx).unwrap();
        // retrieve by direct reference
        let (_, tx2) = bdb.get_decodable::<Transaction>(txref).unwrap();
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
        bdb.put_hash_keyed(&genesis).unwrap();
        // find it
        if let Some((_, block)) = bdb.get_hash_keyed::<Block>(&genesis.bitcoin_hash()).unwrap() {
            assert_eq!(block, genesis);
        }
        else {
            panic!("can not find genesis block");
        }
    }
}