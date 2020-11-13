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

use std::marker::PhantomData;

use bitcoin_hashes::{sha256d, Hash};
use bitcoin::consensus::encode::{Encodable, Decodable, serialize, deserialize};

use Error;
use HammersbaldAPI;
use HammersbaldIterator;
use PRef;

/// Calculate the hash of a bitcoin-encodable object.
fn hash<T: Encodable>(object: &T) -> sha256d::Hash {
	let mut engine = sha256d::Hash::engine();
	object.consensus_encode(&mut engine).expect("engines don't error");
	sha256d::Hash::from_engine(engine)
}

/// Bitcoin adaptor
pub struct BitcoinAdaptor {
    hammersbald: Box<dyn HammersbaldAPI>
}

impl BitcoinAdaptor {
    /// Create a new Adaptor
    pub fn new(hammersbald: Box<dyn HammersbaldAPI>) -> BitcoinAdaptor {
        BitcoinAdaptor { hammersbald }
    }

    /// Store some bitcoin object that has a bitcoin hash
    pub fn put_hash_keyed<T>(&mut self, object: &T) -> Result<PRef, Error>
		where T: Encodable
	{
        Ok(self.hammersbald.put_keyed(&hash(object)[..], &serialize(object)[..])?)
    }

    /// Retrieve a bitcoin_object with its hash
    pub fn get_hash_keyed<T>(&self, id: sha256d::Hash) -> Result<Option<(PRef, T)>, Error>
        where T: Decodable
	{
        match self.hammersbald.get_keyed(&id[..])? {
            Some((pref, data)) => Ok(Some((pref, deserialize(&data[..])?))),
			None => Ok(None),
        }
    }

    /// Store some bitcoin object
    pub fn put_encodable<T>(&mut self, object: &T) -> Result<PRef, Error>
        where T: Encodable
	{
        self.hammersbald.put(&serialize(object))
    }

    /// Retrieve some bitcoin object
    pub fn get_decodable<T>(&self, pref: PRef) -> Result<(Vec<u8>, T), Error>
        where T: Decodable
	{
        let (key, data) = self.hammersbald.get(pref)?;
        Ok((key, deserialize(&data[..])?))
    }

    /// Store some bitcoin object with arbitary key.
    pub fn put_keyed_encodable<T>(&mut self, key: &[u8], object: &T) -> Result<PRef, Error>
        where T: Encodable
	{
        Ok(self.hammersbald.put_keyed(key, &serialize(object))?)
    }

    /// Retrieve some bitcoin object with arbitary key
    pub fn get_keyed_decodable<T>(&self, key: &[u8]) -> Result<Option<(PRef, T)>, Error>
        where T: Decodable
	{
        if let Some((pref, data)) = self.hammersbald.get_keyed(key)? {
            return Ok(Some((pref, deserialize(&data[..])?)));
        }
        Ok(None)
    }

    /// quick check if the db contains a key. This might return false positive.
    pub fn may_have_hash_key(&self, key: sha256d::Hash) -> Result<bool, Error> {
        Ok(self.hammersbald.may_have_key(&key[..])?)
    }

    /// iterate over all data, useful only if data is homogenous
    pub fn iter_decodable<T> (&self) -> HammersbaldDecodableIterator<T>
        where T: Decodable + ?Sized
	{
        HammersbaldDecodableIterator{
			inner: self.iter(),
			data: PhantomData,
		}
    }
}

/// An iterator over a stream of decodable data.
pub struct HammersbaldDecodableIterator<'a, T> {
    inner: HammersbaldIterator<'a>,
    data: PhantomData<T>
}

impl<'a, T: Decodable> Iterator for HammersbaldDecodableIterator<'a, T> {
    type Item = (PRef, T);

    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        while let Some((pref, _, data)) = self.inner.next() {
            if let Ok(d) = deserialize(&data[..]) {
                return Some((pref, d));
            }
        }
        None
    }
}

impl HammersbaldAPI for BitcoinAdaptor {
    fn batch(&mut self) -> Result<(), Error> {
        self.hammersbald.batch()
    }

    fn shutdown(&mut self) {
        self.hammersbald.shutdown()
    }

    fn put_keyed(&mut self, key: &[u8], data: &[u8]) -> Result<PRef, Error> {
        self.hammersbald.put_keyed(key, data)
    }

    fn get_keyed(&self, key: &[u8]) -> Result<Option<(PRef, Vec<u8>)>, Error> {
        self.hammersbald.get_keyed(key)
    }

    fn put(&mut self, data: &[u8]) -> Result<PRef, Error> {
        self.hammersbald.put(data)
    }

    fn get(&self, pref: PRef) -> Result<(Vec<u8>, Vec<u8>), Error> {
        self.hammersbald.get(pref)
    }

    fn may_have_key(&self, key: &[u8]) -> Result<bool, Error> {
        self.hammersbald.may_have_key(key)
    }

    fn forget(&mut self, key: &[u8]) -> Result<(), crate::error::Error> {
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

    use bitcoin::{Block, BlockHeader, Network, Transaction};
	use bitcoin::blockdata::constants::genesis_block;

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
        if let Some((pref, tx3)) = bdb.get_hash_keyed::<Transaction>(tx.wtxid().as_hash()).unwrap() {
            assert_eq!(pref, txref2);
            assert_eq!(tx3, tx);
        }
        else {
            panic!("can not find tx");
        }

        let genesis = genesis_block(Network::Bitcoin);
        // store the genesist block
        bdb.put_hash_keyed(&genesis.header).unwrap();
        // find it
        if let Some((_, block)) = bdb.get_hash_keyed::<BlockHeader>(genesis.block_hash().as_hash()).unwrap() {
            assert_eq!(block, genesis.header);
        }
        else {
            panic!("can not find genesis block");
        }
		//TODO(stevenroose) block
    }
}
