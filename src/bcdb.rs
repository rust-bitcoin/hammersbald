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
use types::Offset;
use logfile::LogFile;
use keyfile::KeyFile;
use datafile::{DataFile, Content};
use page::{Page, PageFile};
use error::BCDBError;

use std::sync::{Mutex,Arc};

/// a trait to create a new db
pub trait BCDBFactory {
    /// create a new db
    fn new_db (name: &str) -> Result<BCDB, BCDBError>;
}

/// The blockchain db
pub struct BCDB {
    table: KeyFile,
    bucket: DataFile,
    data: DataFile,
    log: Arc<Mutex<LogFile>>
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
    fn put(&mut self, key: Vec<Vec<u8>>, data: &[u8]) -> Result<Offset, BCDBError>;

    /// retrieve data by key
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, BCDBError>;

    /// append some content without key
    /// only the returned offset can be used to retrieve
    fn put_content(&mut self, content: Content) -> Result<Offset, BCDBError>;

    /// get some content at a known offset
    fn get_content(&self, offset: Offset) -> Result<Content, BCDBError>;
}

impl BCDB {
    /// create a new db with key and data file
    pub fn new(table: KeyFile, data: DataFile, bucket: DataFile) -> Result<BCDB, BCDBError> {
        let log = table.log_file();
        let mut db = BCDB { table, bucket, data, log };
        db.recover()?;
        db.batch()?;
        Ok(db)
    }

    fn recover(&mut self) -> Result<(), BCDBError> {
        let log = self.log.lock().unwrap();
        let mut first = true;
        debug!("recover");
        for page in log.page_iter() {
            if !first {
                debug!("recover BCDB: patch page {}", page.offset.as_u64());
                self.table.patch_page(page)?;
            }
                else {
                    let mut size = [0u8; 6];
                    page.read(2, &mut size)?;
                    let data_len = Offset::from(&size[..]).as_u64();
                    self.data.truncate(data_len)?;

                    page.read(8, &mut size)?;
                    let table_len = Offset::from(&size[..]).as_u64();
                    self.table.truncate(table_len)?;

                    page.read(14, &mut size)?;
                    let bucket_len = Offset::from(&size[..]).as_u64();
                    self.bucket.truncate(bucket_len)?;
                    first = false;
                    debug!("recover BCDB: set lengths to table: {} data: {}", table_len, data_len);
                }
        }
        Ok(())
    }
}

impl BCDBAPI for BCDB {
    /// initialize a db
    fn init (&mut self) -> Result<(), BCDBError> {
        self.table.init()?;
        self.data.init()?;
        self.bucket.init()?;
        self.log.lock().unwrap().init()?;
        Ok(())
    }


    /// end current batch and start a new batch
    fn batch (&mut self)  -> Result<(), BCDBError> {
        debug!("batch end");
        self.data.flush()?;
        self.data.sync()?;
        self.data.clear_cache();
        self.bucket.flush()?;
        self.bucket.sync()?;
        self.bucket.clear_cache();
        self.table.flush()?;
        self.table.sync()?;
        self.table.clear_cache();
        let data_len = self.data.len()?;
        let table_len = self.table.len()?;
        let bucket_len = self.bucket.len()?;

        let mut log = self.log.lock().unwrap();
        log.clear_cache();
        log.truncate(0)?;

        let mut first = Page::new(Offset::from(0));
        first.write(0, &[0xBC, 0x00]).unwrap();
        first.write(2, Offset::from(data_len).to_vec().as_slice()).unwrap();
        first.write(8, Offset::from(table_len).to_vec().as_slice()).unwrap();
        first.write(14, Offset::from(bucket_len).to_vec().as_slice()).unwrap();

        log.tbl_len = table_len;
        log.append_page(first)?;
        log.flush()?;
        log.sync()?;
        log.clear_cache();
        debug!("batch start");

        Ok(())
    }

    /// stop background writer
    fn shutdown (&mut self) {
        self.data.shutdown();
        self.bucket.shutdown();
        self.table.shutdown();
    }

    /// store data with a key
    /// storing with the same key makes previous data unaccessible
    fn put(&mut self, keys: Vec<Vec<u8>>, data: &[u8]) -> Result<Offset, BCDBError> {
        let offset = self.data.append_content(Content::Data(keys.clone(), data.to_vec()))?;
        self.table.put(keys, offset, &mut self.bucket)?;
        Ok(offset)
    }

    /// retrieve data by key
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, BCDBError> {
        self.table.get(key, &self.data, &self.bucket)
    }

    /// append some content without key
    /// only the returned offset can be used to retrieve
    fn put_content(&mut self, content: Content) -> Result<Offset, BCDBError> {
        if let Content::Extension(data) = content {
            return self.data.append_content(Content::Extension(data));
        }
        return Err(BCDBError::DoesNotFit)
    }

    /// get some content at a known offset
    fn get_content(&self, offset: Offset) -> Result<Content, BCDBError> {
        self.data.get_content(offset)
    }
}

#[cfg(test)]
mod test {
    extern crate simple_logger;
    extern crate rand;
    extern crate hex;

    use inmemory::InMemory;
    use infile::InFile;
    use log;

    use super::*;
    use self::rand::thread_rng;
    use std::collections::HashMap;
    use bcdb::test::rand::RngCore;

    #[test]
    fn test () {
        simple_logger::init_with_level(log::Level::Debug).unwrap();
        let mut db = InMemory::new_db("first").unwrap();
        db.init().unwrap();

        let mut rng = thread_rng();

        let mut check = HashMap::new();
        let mut key = [0x0u8;32];
        let mut data = [0x0u8;40];

        for _ in 0 .. 100000 {
            rng.fill_bytes(&mut key);
            rng.fill_bytes(&mut data);
            check.insert(key, data);
            let mut k = Vec::new();
            k.push(key.to_vec());
            db.put(k, &data).unwrap();
            assert_eq!(db.get(&key).unwrap().unwrap(), data.to_vec());
        }
        db.batch().unwrap();

        for (k, v) in check.iter() {
            assert_eq!(db.get(k).unwrap(), Some(v.to_vec()));
        }
        db.shutdown();
    }
}