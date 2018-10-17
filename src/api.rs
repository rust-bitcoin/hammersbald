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
use offset::Offset;
use logfile::LogFile;
use tablefile::TableFile;
use datafile::{DataFile, Content};
use linkfile::LinkFile;
use memtable::MemTable;
use error::{BCDBError};

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
    fn put(&mut self, key: &[u8], data: &[u8]) -> Result<Offset, BCDBError>;

    /// retrieve single data by key
    fn get_unique(&self, key: &[u8]) -> Result<Option<(Offset, Vec<u8>, Vec<u8>)>, BCDBError>;

    /// append some content without key
    /// only the returned offset can be used to retrieve
    fn put_content(&mut self, content: &[u8]) -> Result<Offset, BCDBError>;

    /// get some content at a known offset
    fn get_content(&self, offset: Offset) -> Result<(Vec<u8>, Vec<u8>), BCDBError>;
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

    /// get data iterator - this also includes no longer referenced data
    pub fn data_iterator<'a>(&'a self) -> impl Iterator<Item=(Offset, Vec<u8>, Vec<u8>)> + 'a {
        self.data.iter()
    }

    /// get link iterator - this also includes no longer used links
    pub fn link_iterator<'a>(&'a self) -> impl Iterator<Item=(Offset, Vec<(u32, Offset)>, Offset)> + 'a {
        self.mem.link_iterator()
    }

    /// get hash table bucket iterator
    pub fn bucket_iterator<'a> (&'a self) -> impl Iterator<Item=Offset> +'a {
        self.mem.iter()
    }
}

impl BCDBAPI for BCDB {
    /// initialize a db
    fn init (&mut self) -> Result<(), BCDBError> {
        self.data.init()?;
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
    fn put(&mut self, key: &[u8], data: &[u8]) -> Result<Offset, BCDBError> {
        #[cfg(debug_assertions)]
        {
            if key.len() > 255 || data.len() >= 1 << 23 {
                return Err(BCDBError::DoesNotFit);
            }
        }

        let data_offset = self.data.append_data(key, data)?;
        self.mem.put(key, data_offset)?;
        Ok(data_offset)
    }

    /// retrieve the single data associated with this key
    fn get_unique(&self, key: &[u8]) -> Result<Option<(Offset, Vec<u8>, Vec<u8>)>, BCDBError> {
        self.mem.get_unique(key,  &self.data)
    }

    /// append some content without key
    /// only the returned offset can be used to retrieve
    fn put_content(&mut self, data: &[u8]) -> Result<Offset, BCDBError> {
        self.data.append_data_extension(data)
    }

    /// get some content at a known offset
    fn get_content(&self, offset: Offset) -> Result<(Vec<u8>, Vec<u8>), BCDBError> {
        match self.data.get_content(offset)? {
            Some(Content::Extension(data)) => return Ok((Vec::new(), data)),
            Some(Content::Data(key, data)) => return Ok((key, data)),
            _ => return Err(BCDBError::Corrupted(format!("wrong offset {}", offset)))
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
            let offset = db.put(&key, &data).unwrap();
            check.insert(key, (offset, data));
        }
        db.batch().unwrap();

        for (k, (o, v)) in check.iter() {
            assert_eq!(db.get_unique(&k[..]).unwrap(), Some((*o, k.to_vec(), v.to_vec())));
        }

        for _ in 0 .. 10000 {
            rng.fill_bytes(&mut key);
            rng.fill_bytes(&mut data);
            let offset = db.put(&key, &data).unwrap();
            check.insert(key, (offset, data));
        }
        db.batch().unwrap();

        for (k, (o, v)) in check.iter() {
            assert_eq!(db.get_unique(&k[..]).unwrap(), Some((*o, k.to_vec(), v.to_vec())));
        }
        db.shutdown();
    }
}