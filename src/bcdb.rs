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
use page::{Page, PAGE_SIZE};
use types::Offset;
use logfile::LogFile;
use keyfile::KeyFile;
use datafile::{DataFile, DataEntry};
use error::BCSError;

use hex;

use std::sync::{Mutex,Arc};
use std::io::{Read,Write,Seek};

/// fixed key length of 256 bits
pub const KEY_LEN : usize = 32;

/// a trait to create a new db
pub trait BCDBFactory {
    /// create a new db
    fn new_db (name: &str) -> Result<BCDB, BCSError>;
}

/// a read-write-seak-able storage with added methods
pub trait RW : Read + Write + Seek + Send {
    /// length of the storage
    fn len (&mut self) -> Result<usize, BCSError>;
    /// truncate storage
    fn truncate(&mut self, new_len: usize) -> Result<(), BCSError>;
    /// tell OS to flush buffers to disk
    fn sync (&self) -> Result<(), BCSError>;
}

/// a paged file with added features
pub trait DBFile : PageFile {
    /// flush buffered writes
    fn flush(&mut self) -> Result<(), BCSError>;
    /// tel OS to flush buffers to disk
    fn sync (&mut self) -> Result<(), BCSError>;
    /// truncate to a given length
    fn truncate(&mut self, offset: Offset) -> Result<(), BCSError>;
    /// return storage length
    fn len(&mut self) -> Result<Offset, BCSError>;
}

/// a paged storage
pub trait PageFile {
    /// read a page at given offset
    fn read_page (&self, offset: Offset) -> Result<Arc<Page>, BCSError>;
}


/// The blockchain db
pub struct BCDB {
    table: KeyFile,
    data: DataFile,
    log: Arc<Mutex<LogFile>>
}

impl BCDB {
    /// create a new db with key and data file
    pub fn new (table: KeyFile, data: DataFile) -> Result<BCDB, BCSError> {
        let log = table.log_file();
        Ok(BCDB {table, data, log})
    }

    /// initialize an empty db
    pub fn init (&mut self) -> Result<(), BCSError> {
        self.table.init()?;
        self.data.init()?;
        self.log.lock().unwrap().init()?;
        Ok(())
    }

    fn recover(&mut self) -> Result<(), BCSError> {
        let mut log = self.log.lock().unwrap();
        if log.len()?.as_u64() > 0 {
            {
                let mut log_pages = log.page_iter();
                if let Some(first) = log_pages.next() {
                    let mut size = [0u8; 6];

                    first.read(0, &mut size)?;
                    let data_len = Offset::from_slice(&size)?;
                    self.data.truncate(data_len)?;

                    first.read(6, &mut size)?;
                    let table_len = Offset::from_slice(&size)?;
                    self.table.truncate(table_len)?;

                    for page in log_pages {
                        if page.offset.as_u64() < table_len.as_u64() {
                            self.table.write_page(page);
                        }
                    }
                }
            }
            log.truncate(Offset::new(0)?)?;
            log.sync()?;
        }
        Ok(())
    }

    /// end current batch and start a new batch
    pub fn batch (&mut self)  -> Result<(), BCSError> {
        self.data.flush()?;
        self.data.sync()?;
        self.table.flush()?;
        self.table.sync()?;
        let data_len = self.data.len()?;
        let table_len = self.table.len()?;

        let mut log = self.log.lock().unwrap();
        log.truncate(Offset::new(0).unwrap())?;
        log.reset();

        let mut first = Page::new(Offset::new(0).unwrap());
        first.write(0, &[0xBC, 0x00]).unwrap();
        let mut size = [0u8; 6];
        data_len.serialize(&mut size);
        first.write(2, &size).unwrap();
        table_len.serialize(&mut size);
        first.write(8, &size).unwrap();


        log.append_page(Arc::new(first))?;
        log.flush()?;
        log.sync()?;

        Ok(())
    }

    /// stop background writer
    pub fn shutdown (&mut self) {
        self.data.shutdown();
        self.table.shutdown();
    }

    /// stora data with a key
    /// storing with the same key makes previous data unaccessible
    pub fn put(&mut self, key: &[u8], data: &[u8]) -> Result<Offset, BCSError> {
        trace!("put {} {}", hex::encode(key), hex::encode(data));
        if key.len() != KEY_LEN {
            return Err(BCSError::DoesNotFit);
        }
        let offset = self.data.append(DataEntry::new_data(key, data))?;
        self.table.put(key, offset, &mut self.data)?;
        Ok(offset)
    }

    /// retrieve data by key
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, BCSError> {
        if key.len() != KEY_LEN {
            return Err(BCSError::DoesNotFit);
        }
        trace!("get {}", hex::encode(key));
        self.table.get(key, &self.data)
    }
}

/// iterate through pages of a paged file
pub struct PageIterator<'file> {
    pagenumber: u64,
    file: &'file PageFile
}

/// page iterator
impl<'file> PageIterator<'file> {
    /// create a new iterator starting at given page
    pub fn new (file: &'file PageFile, pagenumber: u64) -> PageIterator {
        PageIterator{pagenumber, file}
    }
}

impl<'file> Iterator for PageIterator<'file> {
    type Item = Arc<Page>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pagenumber < (1 << 47) / PAGE_SIZE as u64 {
            let offset = Offset::new(self.pagenumber* PAGE_SIZE as u64).unwrap();
            if let Ok(page) = self.file.read_page(offset) {
                self.pagenumber += 1;
                return Some(page);
            }
        }
        None
    }
}

#[cfg(test)]
mod test {
    extern crate hex;
    extern crate simple_logger;

    use inmemory::InMemory;
    use log;

    use super::*;
    #[test]
    fn test () {
        simple_logger::init_with_level(log::Level::Trace).unwrap();

        let mut db = InMemory::new_db("").unwrap();
        db.init().unwrap();

        let key = [0xffu8;32];
        let data = [0xffu8;32];
        db.put(&key, &data).unwrap();

        assert_eq!(db.get(&key).unwrap(), Some(data[..].to_owned()));

        for _ in 1 .. 200000 {
            let data = [0xccu8; 32];
            db.put(&key, &data).unwrap();

            assert_eq!(db.get(&key).unwrap(), Some(data[..].to_owned()));
        }
        db.shutdown();
    }
}