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

use api::{Hammersbald, HammersbaldAPI};
use pref::PRef;
use error::HammersbaldError;
use datafile::DagIterator;
use format::{Payload, Data};

use bitcoin::blockdata::block::{BlockHeader, Block};
use bitcoin::blockdata::transaction::Transaction;
use bitcoin::util::hash::BitcoinHash;
use bitcoin::consensus::{Decodable, Encodable};
use bitcoin::util::hash::Sha256dHash;
use bitcoin::blockdata::script::Script;

use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};

use std::io::Cursor;

/// Adapter for Hammersbald storing Bitcoin data
pub struct BitcoinAdapter {
    hammersbald: Hammersbald
}

/// Types of Bitcoin data
pub enum BitcoinData<'d> {
    /// Header or Block
    HeaderOrBlock(&'d [u8]),
    /// Transaction
    Transaction(&'d [u8]),
    /// Extension
    Extension(&'d [u8]),
}

impl<'d> BitcoinData<'d> {
    /// de-serialize stored Bitcoin data types
    pub fn deserialize (data: &'d [u8]) -> BitcoinData<'d> {
        match data [0] {
            0u8 => BitcoinData::HeaderOrBlock(&data [1..]),
            1u8 => BitcoinData::Transaction(&data[1..]),
            _ => BitcoinData::Extension(&data[1..])
        }
    }
}

impl BitcoinAdapter {
    /// create a new Bitcoin adapter wrapping Hammersbald
    pub fn new(hammersbald: Hammersbald) -> BitcoinAdapter {
        BitcoinAdapter { hammersbald }
    }

    /// Insert a Bitcoin header
    pub fn insert_header (&mut self, header: &BlockHeader, extension: &Vec<Vec<u8>>) -> Result<PRef, HammersbaldError> {
        let mut referred = vec!();
        if header.prev_blockhash != Sha256dHash::default() {
            if let Some((ph, _, _)) = self.hammersbald.get(&header.prev_blockhash.as_bytes()[..])? {
                referred.push(ph);
            }
            else {
                return Err(HammersbaldError::Corrupted("unconnected header".to_string()));
            }
        }
        let key = &header.bitcoin_hash().to_bytes()[..];
        let mut serialized_header = Vec::new();
        serialized_header.push(0u8);
        serialized_header.extend(encode(header)?);
        serialized_header.write_u48::<BigEndian>(PRef::invalid().as_u64())?; // no transactions
        serialized_header.write_u32::<BigEndian>(extension.len() as u32)?;
        for d in extension {
            let pref = self.hammersbald.put_referred(d.as_slice(), &vec!())?;
            serialized_header.write_u48::<BigEndian>(pref.as_u64())?;
            referred.push(pref);
        }
        self.hammersbald.put(&key[..], serialized_header.as_slice(), &referred)
    }

    /// Fetch a header by its id
    pub fn fetch_header (&self, id: &Sha256dHash)  -> Result<Option<(BlockHeader, Vec<Vec<u8>>)>, HammersbaldError> {
        let key = &id.to_bytes()[..];
        if let Some((_,stored,_)) = self.hammersbald.get(&key)? {
            return Self::parse_header(&self.hammersbald, stored);
        }
        Ok(None)
    }

    fn parse_header(hammersbald: &HammersbaldAPI, stored: Vec<u8>) -> Result<Option<(BlockHeader, Vec<Vec<u8>>)>, HammersbaldError> {
        if let BitcoinData::HeaderOrBlock(stored) = BitcoinData::deserialize(stored.as_slice()) {
            let header = decode(&stored[0..80])?;
            let mut data = Cursor::new(&stored[80..]);
            PRef::from(data.read_u48::<BigEndian>()?); // do not care of transactions
            let next = data.read_u32::<BigEndian>()?;
            let mut extension = Vec::new();
            for _ in 0..next {
                let pref = PRef::from(data.read_u48::<BigEndian>()?);
                let (_, e, _) = hammersbald.get_referred(pref)?;
                extension.push(e);
            }

            return Ok(Some((header, extension)))
        }
        Ok(None)
    }

    /// insert a block
    pub fn insert_block(&mut self, block: &Block, extension: &Vec<Vec<u8>>) -> Result<PRef, HammersbaldError> {
        let mut referred = vec!();
        if block.header.prev_blockhash != Sha256dHash::default() {
            if let Some((ph, _, _)) = self.hammersbald.get(&block.header.prev_blockhash.as_bytes()[..])? {
                referred.push(ph);
            }
            else {
                return Err(HammersbaldError::Corrupted("unconnected header".to_string()));
            }
        }
        let key = &block.bitcoin_hash().to_bytes()[..];
        let mut serialized_block = Vec::new();
        serialized_block.push(0u8);
        serialized_block.extend(encode(&block.header)?);
        let mut tx_prefs = Vec::new();
        for t in &block.txdata {
            let pref = self.hammersbald.put_referred(encode(t)?.as_slice(), &vec!())?;
            tx_prefs.push(pref);
            referred.push(pref);
        }
        let stored_tx_offsets = self.hammersbald.put_referred(&[], &tx_prefs)?;
        referred.push(stored_tx_offsets);
        serialized_block.write_u48::<BigEndian>(stored_tx_offsets.as_u64())?;
        serialized_block.write_u32::<BigEndian>(extension.len() as u32)?;
        for d in extension {
            let pref = self.hammersbald.put_referred(d.as_slice(), &vec!())?;
            serialized_block.write_u48::<BigEndian>(pref.as_u64())?;
            referred.push(pref);
        }
        self.hammersbald.put(&key[..], serialized_block.as_slice(), &referred)
    }

    /// Fetch a block by its id
    pub fn fetch_block (&self, id: &Sha256dHash)  -> Result<Option<(Block, Vec<Vec<u8>>)>, HammersbaldError> {
        let key = &id.as_bytes()[..];
        if let Some((_, stored, _)) = self.hammersbald.get(&key)? {
            if let BitcoinData::HeaderOrBlock(stored) = BitcoinData::deserialize(stored.as_slice()) {
                let header = decode(&stored[0..80])?;
                let mut data = Cursor::new(&stored[80..]);
                let txdata_offset = PRef::from(data.read_u48::<BigEndian>()?);
                let mut txdata: Vec<Transaction> = Vec::new();
                if txdata_offset.is_valid() {
                    let (_, _, txrefs) = self.hammersbald.get_referred(txdata_offset)?;
                    for txref in &txrefs {
                        let (_, tx, _) = self.hammersbald.get_referred(*txref)?;
                        txdata.push(decode(tx.as_slice())?);
                    }
                }
                let next = data.read_u32::<BigEndian>()?;
                let mut extension = Vec::new();
                for _ in 0..next {
                    let pref = PRef::from(data.read_u48::<BigEndian>()?);
                    let (_, e, _) = self.hammersbald.get_referred(pref)?;
                    extension.push(e);
                }

                return Ok(Some((Block { header, txdata }, extension)))
            }
        }
        Ok(None)
    }

    /// iterate over stored headers
    pub fn iter_headers<'s>(&'s self, tip: &Sha256dHash) -> Result<impl Iterator<Item=(BlockHeader, Vec<Vec<u8>>)> +'s, HammersbaldError> {
        if let Some((tipref, _, _)) = self.get(&tip.as_bytes()[..])? {
            return Ok(BitcoinHeaderScan { tip: tipref, hb: self })
        }
        return Err(HammersbaldError::Corrupted("Can not find root for scan".to_string()));
    }

    /// iterate over transactions that send to a script
    pub fn iter_send_to_script<'s> (&'s self, tip: &Sha256dHash, script: Script) -> Result<impl Iterator<Item=Transaction> +'s, HammersbaldError> {
        if let Some((tipref, _, _)) = self.get(&tip.as_bytes()[..])? {
            return Ok(BitcoinScriptScan { script, dag: self.dag(tipref) })
        }
        return Err(HammersbaldError::Corrupted("Can not find root for scan".to_string()));
    }
}

struct BitcoinScriptScan<'s> {
    script: Script,
    dag: DagIterator<'s>
}

impl<'s> BitcoinScriptScan<'s> {
    fn process(&self, data: Data) -> Option<Transaction> {
        if let BitcoinData::Transaction(transaction) = BitcoinData::deserialize(data.data) {
            let tx: Transaction = decode(transaction).expect("can not parse stored transaction");
            for output in &tx.output {
                if output.script_pubkey == self.script {
                    return Some(tx.clone());
                }
            }
        }
        None
    }
}

impl<'s> Iterator for BitcoinScriptScan<'s> {
    type Item = Transaction;

    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        while let Some((_, envelope)) = self.dag.next() {
            if let Some(transaction) = match Payload::deserialize(envelope.payload()) {
                Ok(Payload::Indexed(indexed)) => {
                    self.process(indexed.data)
                }
                Ok(Payload::Referred(data)) => {
                    self.process(data)
                }
                _ => None
            } {
                return Some(transaction)
            }
        }
        None
    }
}

struct BitcoinHeaderScan<'s> {
    tip: PRef,
    hb: &'s HammersbaldAPI
}

impl<'s> Iterator for BitcoinHeaderScan<'s> {
    type Item = (BlockHeader, Vec<Vec<u8>>);

    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        if self.tip.is_valid() {
            if let Ok((_,data,referred)) = self.hb.get_referred(self.tip) {
                if referred.len() > 0 {
                    self.tip = referred[0];
                }
                else {
                    self.tip = PRef::invalid();
                }
                if let Ok(Some(result)) = BitcoinAdapter::parse_header(self.hb, data) {
                    return Some(result)
                }
            }
            return None;
        }
        None
    }
}

impl HammersbaldAPI for BitcoinAdapter {
    fn init(&mut self) -> Result<(), HammersbaldError> {
        self.hammersbald.init()
    }

    fn batch(&mut self) -> Result<(), HammersbaldError> {
        self.hammersbald.batch()
    }

    fn shutdown(&mut self) {
        self.hammersbald.shutdown()
    }

    fn put(&mut self, key: &[u8], data: &[u8], referred: &Vec<PRef>) -> Result<PRef, HammersbaldError> {
        self.hammersbald.put(key, data, &referred)
    }

    fn get(&self, key: &[u8]) -> Result<Option<(PRef, Vec<u8>, Vec<PRef>)>, HammersbaldError> {
        self.hammersbald.get(key)
    }

    fn put_referred(&mut self, data: &[u8], referred: &Vec<PRef>) -> Result<PRef, HammersbaldError> {
        self.hammersbald.put_referred(data, referred)
    }

    fn get_referred(&self, pref: PRef) -> Result<(Vec<u8>, Vec<u8>, Vec<PRef>), HammersbaldError> {
        self.hammersbald.get_referred(pref)
    }

    fn dag(&self, root: PRef) -> DagIterator {
        self.hammersbald.dag(root)
    }
}

fn decode<'d, T: ? Sized>(data: &'d [u8]) -> Result<T, HammersbaldError>
    where T: Decodable<Cursor<&'d [u8]>> {
    let mut decoder  = Cursor::new(data);
    Decodable::consensus_decode(&mut decoder).map_err(|e| { HammersbaldError::BitcoinSerialize(e) })
}

fn encode<T: ? Sized>(data: &T) -> Result<Vec<u8>, HammersbaldError>
    where T: Encodable<Vec<u8>> {
    let mut result = vec!();
    data.consensus_encode(&mut result).map_err(|e| { HammersbaldError::BitcoinSerialize(e) })?;
    Ok(result)
}

#[cfg(test)]
mod test {
    extern crate rand;
    extern crate hex;

    use transient::Transient;

    use api::HammersbaldFactory;

    use super::*;

    #[test]
    fn hashtest() {
        let mut db = Transient::new_db("first", 1, 1).unwrap();
        db.init().unwrap();
        let data = encode(&Sha256dHash::default()).unwrap();
        let key = encode(&Sha256dHash::default()).unwrap();
        let pref = db.put(&key[..], data.as_slice(), &vec!()).unwrap();
        assert_eq!(db.get(&key[..]).unwrap(), Some((pref, data, vec!())));
        db.shutdown();
    }

    #[test]
    fn block_test() {
        let mut block: Block = decode(hex::decode("0000002060bbab0edbf3ef8a49608ee326f8fd75c473b7e3982095e2d100000000000000c30134f8c9b6d2470488d7a67a888f6fa12f8692e0c3411fbfb92f0f68f67eedae03ca57ef13021acc22dc4105010000000001010000000000000000000000000000000000000000000000000000000000000000ffffffff2f0315230e0004ae03ca57043e3d1e1d0c8796bf579aef0c0000000000122f4e696e6a61506f6f6c2f5345475749542fffffffff038427a112000000001976a914876fbb82ec05caa6af7a3b5e5a983aae6c6cc6d688ac0000000000000000266a24aa21a9ed5c748e121c0fe146d973a4ac26fa4a68b0549d46ee22d25f50a5e46fe1b377ee00000000000000002952534b424c4f434b3acd16772ad61a3c5f00287480b720f6035d5e54c9efc71be94bb5e3727f10909001200000000000000000000000000000000000000000000000000000000000000000000000000100000000010145310e878941a1b2bc2d33797ee4d89d95eaaf2e13488063a2aa9a74490f510a0100000023220020b6744de4f6ec63cc92f7c220cdefeeb1b1bed2b66c8e5706d80ec247d37e65a1ffffffff01002d3101000000001976a9143ebc40e411ed3c76f86711507ab952300890397288ac0400473044022001dd489a5d4e2fbd8a3ade27177f6b49296ba7695c40dbbe650ea83f106415fd02200b23a0602d8ff1bdf79dee118205fc7e9b40672bf31563e5741feb53fb86388501483045022100f88f040e90cc5dc6c6189d04718376ac19ed996bf9e4a3c29c3718d90ffd27180220761711f16c9e3a44f71aab55cbc0634907a1fa8bb635d971a9a01d368727bea10169522103b3623117e988b76aaabe3d63f56a4fc88b228a71e64c4cc551d1204822fe85cb2103dd823066e096f72ed617a41d3ca56717db335b1ea47a1b4c5c9dbdd0963acba621033d7c89bd9da29fa8d44db7906a9778b53121f72191184a9fee785c39180e4be153ae00000000010000000120925534261de4dcebb1ed5ab1b62bfe7a3ef968fb111dc2c910adfebc6e3bdf010000006b483045022100f50198f5ae66211a4f485190abe4dc7accdabe3bc214ebc9ea7069b97097d46e0220316a70a03014887086e335fc1b48358d46cd6bdc9af3b57c109c94af76fc915101210316cff587a01a2736d5e12e53551b18d73780b83c3bfb4fcf209c869b11b6415effffffff0220a10700000000001976a91450333046115eaa0ac9e0216565f945070e44573988ac2e7cd01a000000001976a914c01a7ca16b47be50cbdbc60724f701d52d75156688ac00000000010000000203a25f58630d7a1ea52550365fd2156683f56daf6ca73a4b4bbd097e66516322010000006a47304402204efc3d70e4ca3049c2a425025edf22d5ca355f9ec899dbfbbeeb2268533a0f2b02204780d3739653035af4814ea52e1396d021953f948c29754edd0ee537364603dc012103f7a897e4dbecab2264b21917f90664ea8256189ea725d28740cf7ba5d85b5763ffffffff03a25f58630d7a1ea52550365fd2156683f56daf6ca73a4b4bbd097e66516322000000006a47304402202d96defdc5b4af71d6ba28c9a6042c2d5ee7bc6de565d4db84ef517445626e03022022da80320e9e489c8f41b74833dfb6a54a4eb5087cdb46eb663eef0b25caa526012103f7a897e4dbecab2264b21917f90664ea8256189ea725d28740cf7ba5d85b5763ffffffff0200e1f5050000000017a914b7e6f7ff8658b2d1fb107e3d7be7af4742e6b1b3876f88fc00000000001976a914913bcc2be49cb534c20474c4dee1e9c4c317e7eb88ac0000000001000000043ffd60d3818431c495b89be84afac205d5d1ed663009291c560758bbd0a66df5010000006b483045022100f344607de9df42049688dcae8ff1db34c0c7cd25ec05516e30d2bc8f12ac9b2f022060b648f6a21745ea6d9782e17bcc4277b5808326488a1f40d41e125879723d3a012103f7a897e4dbecab2264b21917f90664ea8256189ea725d28740cf7ba5d85b5763ffffffffa5379401cce30f84731ef1ba65ce27edf2cc7ce57704507ebe8714aa16a96b92010000006a473044022020c37a63bf4d7f564c2192528709b6a38ab8271bd96898c6c2e335e5208661580220435c6f1ad4d9305d2c0a818b2feb5e45d443f2f162c0f61953a14d097fd07064012103f7a897e4dbecab2264b21917f90664ea8256189ea725d28740cf7ba5d85b5763ffffffff70e731e193235ff12c3184510895731a099112ffca4b00246c60003c40f843ce000000006a473044022053760f74c29a879e30a17b5f03a5bb057a5751a39f86fa6ecdedc36a1b7db04c022041d41c9b95f00d2d10a0373322a9025dba66c942196bc9d8adeb0e12d3024728012103f7a897e4dbecab2264b21917f90664ea8256189ea725d28740cf7ba5d85b5763ffffffff66b7a71b3e50379c8e85fc18fe3f1a408fc985f257036c34702ba205cef09f6f000000006a4730440220499bf9e2db3db6e930228d0661395f65431acae466634d098612fd80b08459ee022040e069fc9e3c60009f521cef54c38aadbd1251aee37940e6018aadb10f194d6a012103f7a897e4dbecab2264b21917f90664ea8256189ea725d28740cf7ba5d85b5763ffffffff0200e1f5050000000017a9148fc37ad460fdfbd2b44fe446f6e3071a4f64faa6878f447f0b000000001976a914913bcc2be49cb534c20474c4dee1e9c4c317e7eb88ac00000000").unwrap().as_slice()).unwrap();
        block.header.prev_blockhash = Sha256dHash::default();
        let mut db = BitcoinAdapter::new(Transient::new_db("first", 1, 1).unwrap());

        db.init().unwrap();

        let mut extra = Vec::new();
        extra.push([0u8; 2].to_vec());
        extra.push([2u8; 6].to_vec());

        db.insert_header(&block.header, &extra).unwrap();
        db.batch().unwrap();
        assert_eq!(db.fetch_header(&block.header.bitcoin_hash()).unwrap().unwrap(), (block.header, extra.clone()));

        db.insert_block(&block, &extra).unwrap();
        db.batch().unwrap();
        assert_eq!(db.fetch_block(&block.bitcoin_hash()).unwrap().unwrap(), (block, extra.clone()));
        db.shutdown();
    }
}