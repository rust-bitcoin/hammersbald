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
//! # Hammersbald API
//!
use logfile::LogFile;
use tablefile::TableFile;
use datafile::{DataFile, EnvelopeIterator};
use memtable::MemTable;
use format::{Payload,Envelope};
use persistent::Persistent;
use transient::Transient;
use pref::PRef;
use error::Error;

use byteorder::{WriteBytesExt, ReadBytesExt, BigEndian};

use std::{
    io,
    io::{Cursor, Read, Write}
};

/// Hammersbald
pub struct Hammersbald {
    mem: MemTable
}

/// create or open a persistent db
pub fn persistent(name: &str, cached_data_pages: usize, bucket_fill_target: usize) -> Result<Box<dyn HammersbaldAPI>, Error> {
    Persistent::new_db(name, cached_data_pages,bucket_fill_target)
}

/// create a transient db
pub fn transient(bucket_fill_target: usize) -> Result<Box<dyn HammersbaldAPI>, Error> {
    Transient::new_db("",0,bucket_fill_target)
}

/// public API to Hammersbald
pub trait HammersbaldAPI : Send + Sync {
    /// end current batch and start a new batch
    fn batch (&mut self)  -> Result<(), Error>;

    /// stop background writer
    fn shutdown (&mut self);

    /// store data accessible with key
    /// returns a persistent reference to stored data
    fn put_keyed(&mut self, key: &[u8], data: &[u8]) -> Result<PRef, Error>;

    /// retrieve data with key
    /// returns Some(persistent reference, data) or None
    fn get_keyed(&self, key: &[u8]) -> Result<Option<(PRef, Vec<u8>)>, Error>;

    /// store data
    /// returns a persistent reference
    fn put(&mut self, data: &[u8]) -> Result<PRef, Error>;

    /// retrieve data using a persistent reference
    /// returns (key, data)
    fn get(&self, pref: PRef) -> Result<(Vec<u8>, Vec<u8>), Error>;

    /// a quick (in-memory) check if the db may have the key
    /// this might return false positive, but if it is false key is definitely not used.
    fn may_have_key(&self, key: &[u8]) -> Result<bool, Error>;

    /// forget a key (if known)
    /// This is not a real delete as data will be still accessible through its PRef, but contains hash table growth
    fn forget(&mut self, key: &[u8]) -> Result<(), Error>;

    /// iterator of data
    fn iter(&self) -> HammersbaldIterator;
}

/// A helper to build Hammersbald data elements
pub struct HammersbaldDataWriter {
    data: Vec<u8>
}

impl HammersbaldDataWriter {
    /// create a new builder
    pub fn new () -> HammersbaldDataWriter {
        HammersbaldDataWriter { data: vec!() }
    }

    /// serialized data
    pub fn as_slice<'a> (&'a self) -> &'a [u8] {
        self.data.as_slice()
    }

    /// append a persistent reference
    pub fn write_ref(&mut self, pref: PRef) {
        self.data.write_u48::<BigEndian>(pref.as_u64()).unwrap();
    }

    /// return a reader
    pub fn reader<'a>(&'a self) -> Cursor<&'a [u8]> {
        Cursor::new(self.data.as_slice())
    }
}

impl Write for HammersbaldDataWriter {
    fn write(&mut self, buf: &[u8]) -> Result<usize, io::Error> {
        self.data.write(buf)
    }

    fn flush(&mut self) -> Result<(), io::Error> {
        Ok(())
    }
}

/// Helper to read Hammersbald data elements
pub struct HammersbaldDataReader<'a> {
    reader: Cursor<&'a [u8]>
}

impl<'a> HammersbaldDataReader<'a> {
    /// create a new reader
    pub fn new (data: &'a [u8]) -> HammersbaldDataReader<'a> {
        HammersbaldDataReader{ reader: Cursor::new(data) }
    }

    /// read a persistent reference
    pub fn read_ref (&mut self) -> Result<PRef, io::Error> {
        Ok(PRef::from(self.reader.read_u48::<BigEndian>()?))
    }
}

impl<'a> Read for HammersbaldDataReader<'a> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, io::Error> {
        self.reader.read(buf)
    }
}

impl Hammersbald {
    /// create a new db with key and data file
    pub fn new(log: LogFile, table: TableFile, data: DataFile, link: DataFile, bucket_fill_target :usize) -> Result<Hammersbald, Error> {
        let mem = MemTable::new(log, table, data, link, bucket_fill_target);
        let mut db = Hammersbald { mem };
        db.recover()?;
        db.load()?;
        db.batch()?;
        Ok(db)
    }

    /// load memtable
    fn load(&mut self) -> Result<(), Error> {
        self.mem.load()
    }

    fn recover(&mut self) -> Result<(), Error> {
        self.mem.recover()
    }

    /// get hash table bucket iterator
    pub fn slots<'a> (&'a self) -> impl Iterator<Item=Vec<(u32, PRef)>> +'a {
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

    fn batch (&mut self)  -> Result<(), Error> {
        self.mem.batch()
    }

    fn shutdown (&mut self) {
        self.mem.shutdown()
    }

    fn put_keyed(&mut self, key: &[u8], data: &[u8]) -> Result<PRef, Error> {
        #[cfg(debug_assertions)]
        {
            if key.len() > 255 || data.len() >= 1 << 23 {
                return Err(Error::KeyTooLong);
            }
        }
        let data_offset = self.mem.append_data(key, data)?;
        self.mem.put(key, data_offset)?;
        Ok(data_offset)
    }

    fn get_keyed(&self, key: &[u8]) -> Result<Option<(PRef, Vec<u8>)>, Error> {
        self.mem.get(key)
    }

    fn put(&mut self, data: &[u8]) -> Result<PRef, Error> {
        let data_offset = self.mem.append_referred(data)?;
        Ok(data_offset)
    }

    fn get(&self, pref: PRef) -> Result<(Vec<u8>, Vec<u8>), Error> {
        let envelope = self.mem.get_envelope(pref)?;
        match Payload::deserialize(envelope.payload())? {
            Payload::Referred(referred) => return Ok((vec!(), referred.data.to_vec())),
            Payload::Indexed(indexed) => return Ok((indexed.key.to_vec(), indexed.data.data.to_vec())),
            _ => Err(Error::Corrupted("referred should point to data".to_string()))
        }
    }

    fn may_have_key(&self, key: &[u8]) -> Result<bool, Error> {
        self.mem.may_have_key(key)
    }

    fn forget(&mut self, key: &[u8]) -> Result<(), Error> {
        self.mem.forget(key)
    }

    fn iter(&self) -> HammersbaldIterator {
        HammersbaldIterator{ ei: self.mem.data_envelopes()}
    }
}

/// iterate data content
pub struct HammersbaldIterator<'a> {
    ei: EnvelopeIterator<'a>
}

impl<'a> Iterator for HammersbaldIterator<'a> {
    type Item = (PRef, Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        if let Some((pref, envelope)) = self.ei.next() {
            match Payload::deserialize(envelope.payload()).unwrap() {
                Payload::Indexed(indexed) => {
                    return Some((pref, indexed.key.to_vec(), indexed.data.data.to_vec()))
                },
                Payload::Referred(referred) => {
                    return Some((pref, vec!(), referred.data.to_vec()))
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

        let mut rng = thread_rng();

        let mut check = HashMap::new();
        let mut key = [0x0u8;32];
        let mut data = [0x0u8;40];

        for _ in 0 .. 10000 {
            rng.fill_bytes(&mut key);
            rng.fill_bytes(&mut data);
            let pref = db.put_keyed(&key, &data).unwrap();
            check.insert(key, (pref, data));
        }
        db.batch().unwrap();

        for (k, (o, v)) in check.iter() {
            assert_eq!(db.get_keyed(&k[..]).unwrap(), Some((*o, v.to_vec())));
        }

        for _ in 0 .. 10000 {
            rng.fill_bytes(&mut key);
            rng.fill_bytes(&mut data);
            let pref = db.put_keyed(&key, &data).unwrap();
            check.insert(key, (pref, data));
        }
        db.batch().unwrap();

        for (k, (o, v)) in check.iter() {
            assert_eq!(db.get_keyed(&k[..]).unwrap(), Some((*o, v.to_vec())));
        }
        db.shutdown();
    }
}