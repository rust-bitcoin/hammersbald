//
// Copyright 2018,2019 Tamas Blummer
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
//! # Hammersbald API
//!
use logfile::LogFile;
use tablefile::TableFile;
use datafile::{DataFile, DagIterator};
use memtable::MemTable;
use format::{Payload,Envelope};
use persistent::Persistent;
use transient::Transient;

pub use pref::PRef;
pub use error::HammersbaldError;

/// The blockchain db
pub struct Hammersbald {
    mem: MemTable
}

/// create or open a persistent db
pub fn persistent(name: &str, cached_data_pages: usize, bucket_fill_target: usize) -> Result<Box<HammersbaldAPI>, HammersbaldError> {
    Persistent::new_db(name, cached_data_pages,bucket_fill_target)
}

/// create a transient db
pub fn transient(bucket_fill_target: usize) -> Result<Box<HammersbaldAPI>, HammersbaldError> {
    Transient::new_db("",0,bucket_fill_target)
}

/// public API to Hammersbald
pub trait HammersbaldAPI {
    /// initialize a db
    fn init (&mut self) -> Result<(), HammersbaldError>;
    /// end current batch and start a new batch
    fn batch (&mut self)  -> Result<(), HammersbaldError>;

    /// stop background writer
    fn shutdown (&mut self);

    /// store data with a key
    /// storing with the same key makes previous data unaccessible
    /// returns the pref the data was stored
    fn put(&mut self, key: &[u8], data: &[u8], referred: &Vec<PRef>) -> Result<PRef, HammersbaldError>;

    /// retrieve single data by key
    /// returns (pref, data, referred)
    fn get(&self, key: &[u8]) -> Result<Option<(PRef, Vec<u8>, Vec<PRef>)>, HammersbaldError>;

    /// store referred data
    /// returns the pref the data was stored
    fn put_referred(&mut self, data: &[u8], referred: &Vec<PRef>) -> Result<PRef, HammersbaldError>;

    /// get data
    /// returns (key, data, referred)
    fn get_referred(&self, pref: PRef) -> Result<(Vec<u8>, Vec<u8>, Vec<PRef>), HammersbaldError>;

    /// iterator of data backward from tip following references
    fn iter<'a>(&'a self, tip: PRef) -> HammersbaldIterator<'a>;
}

impl Hammersbald {
    /// create a new db with key and data file
    pub fn new(log: LogFile, table: TableFile, data: DataFile, link: DataFile, bucket_fill_target :usize) -> Result<Box<HammersbaldAPI>, HammersbaldError> {
        let mem = MemTable::new(log, table, data, link, bucket_fill_target);
        let mut db = Box::new(Hammersbald { mem });
        db.recover()?;
        db.load()?;
        db.batch()?;
        Ok(db)
    }

    /// load memtable
    fn load(&mut self) -> Result<(), HammersbaldError> {
        self.mem.load()
    }

    fn recover(&mut self) -> Result<(), HammersbaldError> {
        self.mem.recover()
    }

    /// get hash table bucket iterator
    pub fn slots<'a> (&'a self) -> impl Iterator<Item=&'a Vec<(u32, PRef)>> +'a {
        self.mem.slots()
    }

    /// get hash table pointers
    pub fn buckets<'a> (&'a self) -> impl Iterator<Item=PRef> +'a {
        self.mem.buckets()
    }

    /// return an iterator of all payloads
    pub fn data_envelopes<'a>(&'a self) -> impl Iterator<Item=(PRef, Envelope)> +'a {
        self.mem.data_envelopes()
    }

    /// return an iterator of all links
    pub fn link_envelopes<'a>(&'a self) -> impl Iterator<Item=(PRef, Envelope)> +'a {
        self.mem.link_envelopes()
    }

    /// get db params
    pub fn params(&self) -> (usize, u32, usize, u64, u64, u64, u64, u64) {
        self.mem.params()
    }
}

impl HammersbaldAPI for Hammersbald {

    fn init (&mut self) -> Result<(), HammersbaldError> {
        self.mem.init()
    }

    fn batch (&mut self)  -> Result<(), HammersbaldError> {
        self.mem.batch()
    }

    fn shutdown (&mut self) {
        self.mem.shutdown()
    }

    fn put(&mut self, key: &[u8], data: &[u8], referred: &Vec<PRef>) -> Result<PRef, HammersbaldError> {
        #[cfg(debug_assertions)]
        {
            if key.len() > 255 || data.len() >= 1 << 23 {
                return Err(HammersbaldError::ForwardReference);
            }
        }
        let data_offset = self.mem.append_data(key, data, referred)?;
        #[cfg(debug_assertions)]
        {
            if referred.iter().any(|o| o.as_u64() >= data_offset.as_u64()) {
                return Err(HammersbaldError::ForwardReference);
            }
        }
        self.mem.put(key, data_offset)?;
        Ok(data_offset)
    }

    fn get(&self, key: &[u8]) -> Result<Option<(PRef, Vec<u8>, Vec<PRef>)>, HammersbaldError> {
        self.mem.get(key)
    }

    fn put_referred(&mut self, data: &[u8], referred: &Vec<PRef>) -> Result<PRef, HammersbaldError> {
        let data_offset = self.mem.append_referred(data, referred)?;
        #[cfg(debug_assertions)]
        {
            if referred.iter().any(|o| o.as_u64() >= data_offset.as_u64()) {
                return Err(HammersbaldError::ForwardReference);
            }
        }
        Ok(data_offset)
    }

    fn get_referred(&self, pref: PRef) -> Result<(Vec<u8>, Vec<u8>, Vec<PRef>), HammersbaldError> {
        let envelope = self.mem.get_envelope(pref)?;
        match Payload::deserialize(envelope.payload())? {
            Payload::Referred(referred) => return Ok((vec!(), referred.data.to_vec(), referred.referred())),
            Payload::Indexed(indexed) => return Ok((indexed.key.to_vec(), indexed.data.data.to_vec(), indexed.data.referred())),
            _ => Err(HammersbaldError::Corrupted("referred should point to data".to_string()))
        }
    }

    fn iter(&self, root: PRef) -> HammersbaldIterator {
        HammersbaldIterator{dagi: self.mem.dag(root)}
    }
}

/// iterate data content
pub struct HammersbaldIterator<'a> {
    dagi: DagIterator<'a>
}

impl<'a> Iterator for HammersbaldIterator<'a> {
    type Item = (PRef, Vec<u8>, Vec<u8>, Vec<PRef>);

    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        if let Some((pref, envelope)) = self.dagi.next() {
            match Payload::deserialize(envelope.payload()).unwrap() {
                Payload::Indexed(indexed) => {
                    return Some((pref, indexed.key.to_vec(), indexed.data.data.to_vec(), indexed.data.referred()))
                },
                Payload::Referred(referred) => {
                    return Some((pref, vec!(), referred.data.to_vec(), referred.referred()))
                },
                _ => return None
            }
        }
        None
    }
}

#[cfg(test)]
mod test {
    extern crate rand;
    extern crate hex;

    use transient::Transient;

    use self::rand::thread_rng;
    use std::collections::HashMap;
    use api::test::rand::RngCore;

    #[test]
    fn test_two_batches () {
        let mut db = Transient::new_db("first", 1, 1).unwrap();
        db.init().unwrap();

        let mut rng = thread_rng();

        let mut check = HashMap::new();
        let mut key = [0x0u8;32];
        let mut data = [0x0u8;40];

        for _ in 0 .. 10000 {
            rng.fill_bytes(&mut key);
            rng.fill_bytes(&mut data);
            let pref = db.put(&key, &data, &vec!()).unwrap();
            check.insert(key, (pref, data));
        }
        db.batch().unwrap();

        for (k, (o, v)) in check.iter() {
            assert_eq!(db.get(&k[..]).unwrap(), Some((*o, v.to_vec(), vec!())));
        }

        for _ in 0 .. 10000 {
            rng.fill_bytes(&mut key);
            rng.fill_bytes(&mut data);
            let pref = db.put(&key, &data, &vec!()).unwrap();
            check.insert(key, (pref, data));
        }
        db.batch().unwrap();

        for (k, (o, v)) in check.iter() {
            assert_eq!(db.get(&k[..]).unwrap(), Some((*o, v.to_vec(), vec!())));
        }
        db.shutdown();
    }
}