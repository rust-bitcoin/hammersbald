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
//! # The blockchain db
//!
use pref::PRef;
use logfile::LogFile;
use tablefile::TableFile;
use datafile::DataFile;
use linkfile::LinkFile;
use memtable::MemTable;
use format::Payload;
use error::BCDBError;

/// a trait to create a new db
pub trait BCDBFactory {
    /// create a new db
    fn new_db (name: &str, cached_data_pages: usize) -> Result<BCDB, BCDBError>;
}

/// The blockchain db
pub struct BCDB {
    mem: MemTable,
    data: DataFile
}

/// public API to the blockchain db
pub trait BCDBAPI {
    /// initialize a db
    fn init (&mut self) -> Result<(), BCDBError>;
    /// end current batch and start a new batch
    fn batch (&mut self)  -> Result<(), BCDBError>;

    /// stop background writer
    fn shutdown (&mut self);

    /// store data with a key
    /// storing with the same key makes previous data unaccessible
    /// returns the pref the data was stored
    fn put(&mut self, key: &[u8], data: &[u8], referred: &Vec<PRef>) -> Result<PRef, BCDBError>;

    /// retrieve single data by key
    /// returns (pref, data, referred)
    fn get(&self, key: &[u8]) -> Result<Option<(PRef, Vec<u8>, Vec<PRef>)>, BCDBError>;

    /// store referred data
    /// returns the pref the data was stored
    fn put_referred(&mut self, data: &[u8], referred: &Vec<PRef>) -> Result<PRef, BCDBError>;

    /// get data
    /// returns (key, data, referred)
    fn get_referred(&self, pref: PRef) -> Result<(Vec<u8>, Vec<u8>, Vec<PRef>), BCDBError>;
}

impl BCDB {
    /// create a new db with key and data file
    pub fn new(log: LogFile, table: TableFile, data: DataFile, link: LinkFile) -> Result<BCDB, BCDBError> {
        let mut mem = MemTable::new(log, link, table);
        mem.load()?;
        let mut db = BCDB { mem, data };
        db.recover()?;
        db.batch()?;
        Ok(db)
    }

    fn recover(&mut self) -> Result<(), BCDBError> {
        let data_len = self.mem.recover()?;
        self.data.truncate(data_len)
    }

    /// get hash table bucket iterator
    pub fn bucket_iterator<'a> (&'a self) -> impl Iterator<Item=PRef> +'a {
        self.mem.iter()
    }
}

impl BCDBAPI for BCDB {
    /// initialize a db
    fn init (&mut self) -> Result<(), BCDBError> {
        self.mem.init()
    }


    /// end current batch and start a new batch
    fn batch (&mut self)  -> Result<(), BCDBError> {
        debug!("batch end");
        self.data.flush()?;
        self.data.sync()?;
        let data_len = self.data.len()?;
        debug!("data length {}", data_len);
        self.mem.batch(data_len)
    }

    /// stop background writer
    fn shutdown (&mut self) {
        self.data.shutdown();
        self.mem.shutdown()
    }

    /// store data with a key
    /// storing with the same key makes previous data unaddressable
    fn put(&mut self, key: &[u8], data: &[u8], referred: &Vec<PRef>) -> Result<PRef, BCDBError> {
        #[cfg(debug_assertions)]
        {
            if key.len() > 255 || data.len() >= 1 << 23 {
                return Err(BCDBError::ForwardReference);
            }
        }
        let data_offset = self.data.append_data(key, data, referred)?;
        #[cfg(debug_assertions)]
        {
            if referred.iter().any(|o| o.as_u64() >= data_offset.as_u64()) {
                return Err(BCDBError::ForwardReference);
            }
        }
        self.mem.put(key, data_offset)?;
        Ok(data_offset)
    }

    fn get(&self, key: &[u8]) -> Result<Option<(PRef, Vec<u8>, Vec<PRef>)>, BCDBError> {
        self.mem.get(key,  &self.data)
    }

    fn put_referred(&mut self, data: &[u8], referred: &Vec<PRef>) -> Result<PRef, BCDBError> {
        let data_offset = self.data.append_referred(data, referred)?;
        #[cfg(debug_assertions)]
        {
            if referred.iter().any(|o| o.as_u64() >= data_offset.as_u64()) {
                return Err(BCDBError::ForwardReference);
            }
        }
        Ok(data_offset)
    }

    fn get_referred(&self, pref: PRef) -> Result<(Vec<u8>, Vec<u8>, Vec<PRef>), BCDBError> {
        match self.data.get_payload(pref)? {
            Payload::Referred(referred) => return Ok((vec!(), referred.data, referred.referred)),
            Payload::Indexed(indexed) => return Ok((indexed.key, indexed.data.data, indexed.data.referred)),
            _ => return Err(BCDBError::Corrupted("pref should point to data".to_string()))
        }
    }
}

#[cfg(test)]
mod test {
    extern crate rand;
    extern crate hex;

    use transient::Transient;

    use super::*;
    use self::rand::thread_rng;
    use std::collections::HashMap;
    use api::test::rand::RngCore;

    #[test]
    fn test_two_batches () {
        let mut db = Transient::new_db("first", 1).unwrap();
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