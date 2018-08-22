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
use types::U24;
use datafile::DataType;

use bitcoin::blockdata::block::{BlockHeader, Block};
use bitcoin::blockdata::transaction::Transaction;
use bitcoin::network::serialize::BitcoinHash;
use bitcoin::network::encodable::{ConsensusDecodable, ConsensusEncodable};
use bitcoin::network::serialize::{RawDecoder, RawEncoder};
use bitcoin::network::serialize::serialize;
use bitcoin::util::hash::Sha256dHash;

use std::io::Cursor;
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
        let mut db = BCDB {table, data, log};
        db.recover()?;
        db.batch()?;
        Ok(db)
    }

    /// initialize an empty db
    pub fn init (&mut self) -> Result<(), BCSError> {
        self.table.init()?;
        self.data.init()?;
        self.log.lock().unwrap().init()?;
        Ok(())
    }

    fn recover(&mut self) -> Result<(), BCSError> {
        let log = self.log.lock().unwrap();
        let mut first = true;
        for page in log.page_iter() {
            if !first {
                trace!("patch page {}", page.offset.as_u64());
                self.table.patch_page(page);
            }
            else {
                let mut size = [0u8; 6];
                page.read(2, &mut size)?;
                let data_len = Offset::from_slice(&size)?;
                trace!("data len {}", data_len.as_u64());
                self.data.truncate(data_len)?;

                page.read(8, &mut size)?;
                let table_len = Offset::from_slice(&size)?;
                trace!("table len {}", table_len.as_u64());
                self.table.truncate(table_len)?;
                first = false;
            }
        }
        Ok(())
    }

    /// end current batch and start a new batch
    pub fn batch (&mut self)  -> Result<(), BCSError> {
        self.data.flush()?;
        self.data.sync()?;
        self.data.clear_cache();
        self.table.flush()?;
        self.table.sync()?;
        self.table.clear_cache();
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
        log.tbl_len = table_len.as_u64();


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

    /// store data with a key
    /// storing with the same key makes previous data unaccessible
    pub fn put(&mut self, key: &[u8], data: &[u8]) -> Result<Offset, BCSError> {
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
        self.table.get(key, &self.data)
    }

    /// Insert a block header
    pub fn insert_header (&mut self, header: &BlockHeader, extension: Vec<Vec<u8>>) -> Result<Offset, BCSError> {
        let key = encode(&header.bitcoin_hash())?;
        let mut serialized_header = encode(header)?;

        let mut number_of_data = [0u8;3];
        U24::new(extension.len())?.serialize(&mut number_of_data);
        serialized_header.append(&mut number_of_data.to_vec());

        for d in extension {
            let offset = self.data.append(DataEntry::new_data_extension(d.as_slice()))?;
            let mut did = [0u8;6];
            offset.serialize(&mut did);
            serialized_header.append(&mut did.to_vec());
        }

        let number_of_transactions = [0u8;3];
        serialized_header.append(&mut number_of_transactions.to_vec());

        let offset = self.data.append(
            DataEntry::new_data(key.as_slice(),
                                serialized_header.as_slice()))?;
        self.table.put(key.as_slice(), offset, &mut self.data)?;
        Ok(offset)
    }

    /// Fetch a header by its id
    pub fn fetch_header (&self, id: &Sha256dHash)  -> Result<Option<(BlockHeader, Vec<Vec<u8>>)>, BCSError> {
        let key = encode(id)?;
        if let Some(stored) = self.get(&key)? {
            let header = decode(stored.as_slice()[0..80].to_vec())?;
            let mut extension = Vec::new();
            let n_extensions = U24::from_slice(&stored.as_slice()[80..83])?.as_usize();
            for i in 0 .. n_extensions {
                let offset = Offset::from_slice(&stored.as_slice()[83+i*6 .. 83+(i+1)*6])?;
                if let Some(data) = self.data.get(offset)? {
                    if data.data_type == DataType::AppDataExtension {
                        extension.push(data.data);
                    }
                    else {
                        return Err(BCSError::Corrupted);
                    }
                }
                else {
                    return Err(BCSError::Corrupted);
                }
            }

            Ok(Some((header, extension)))
        }
        else {
            Ok(None)
        }
    }

    /// insert a block
    pub fn insert_block(&mut self, block: &Block, extension: Vec<Vec<u8>>) -> Result<Offset, BCSError> {
        let key = encode(&block.bitcoin_hash())?;
        let mut serialized_block = encode(block)?;

        let mut number_of_data = [0u8;3];
        U24::new(extension.len())?.serialize(&mut number_of_data);
        serialized_block.append(&mut number_of_data.to_vec());

        for d in extension {
            let offset = self.data.append(DataEntry::new_data_extension(d.as_slice()))?;
            let mut did = [0u8;6];
            offset.serialize(&mut did);
            serialized_block.append(&mut did.to_vec());
        }

        let mut number_of_transactions = [0u8;3];
        U24::new(block.txdata.len())?.serialize(&mut number_of_transactions);
        serialized_block.append(&mut number_of_transactions.to_vec());

        for t in &block.txdata {
            let offset = self.put(
                &encode(&t.txid())?.as_slice(), &encode(t)?.as_slice())?;
            let mut did = [0u8;6];
            offset.serialize(&mut did);
            serialized_block.append(&mut did.to_vec());
        }

        let offset = self.put(key.as_slice(),serialized_block.as_slice())?;
        Ok(offset)
    }

    /// Fetch a block by its id
    pub fn fetch_block (&self, id: &Sha256dHash)  -> Result<Option<(Block, Vec<Vec<u8>>)>, BCSError> {
        let key = encode(id)?;
        if let Some(stored) = self.get(&key)? {
            let header = decode(stored.as_slice()[0..80].to_vec())?;
            let mut extension = Vec::new();
            let n_extensions = U24::from_slice(&stored.as_slice()[80..83])?.as_usize();
            for i in 0 .. n_extensions {
                let offset = Offset::from_slice(&stored.as_slice()[83+i*6 .. 83+(i+1)*6])?;
                if let Some(data) = self.data.get(offset)? {
                    if data.data_type == DataType::AppDataExtension {
                        extension.push(data.data);
                    }
                    else {
                        return Err(BCSError::Corrupted);
                    }
                }
                else {
                    return Err(BCSError::Corrupted);
                }
            }

            let n_transactions = U24::from_slice(&stored.as_slice()[83+n_extensions*6 .. 83+n_extensions*6+3])?.as_usize();
            let mut txdata = Vec::new();
            for i in 0 .. n_transactions {
                let offset = Offset::from_slice(&stored.as_slice()[83+n_extensions*6+3+i*6 .. 83+n_extensions*6+3+(i+1)*6])?;
                if let Some (tx) = self.data.get(offset)? {
                    txdata.push(decode(tx.data)?);
                }
                else {
                    return Err(BCSError::Corrupted);
                }
            }

            Ok(Some((Block{header, txdata}, extension)))
        }
        else {
            Ok(None)
        }
    }

    /// fetch a transaction stored with a block
    pub fn fetch_transaction (&self, id: &Sha256dHash)  -> Result<Option<Transaction>, BCSError> {
        let key = encode(id)?;
        if let Some(stored) = self.get(&key)? {
            return Ok(decode(stored)?)
        }
        Err(BCSError::Corrupted)
    }
}

fn decode<T: ? Sized>(data: Vec<u8>) -> Result<T, BCSError>
    where T: ConsensusDecodable<RawDecoder<Cursor<Vec<u8>>>> {
    let mut decoder: RawDecoder<Cursor<Vec<u8>>> = RawDecoder::new(Cursor::new(data));
    ConsensusDecodable::consensus_decode(&mut decoder).map_err(|e| { BCSError::Util(e) })
}

fn encode<T: ? Sized>(data: &T) -> Result<Vec<u8>, BCSError>
    where T: ConsensusEncodable<RawEncoder<Cursor<Vec<u8>>>> {
    serialize(data).map_err(|e| { BCSError::Util(e) })
}


/// iterate through pages of a paged file
pub struct PageIterator<'file> {
    /// the current page of the iterator
    pub pagenumber: u64,
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
            let offset = Offset::new((self.pagenumber)* PAGE_SIZE as u64).unwrap();
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
    extern crate simple_logger;
    extern crate rand;

    use inmemory::InMemory;
    use infile::InFile;
    use log;

    use super::*;
    use self::rand::thread_rng;
    use std::collections::HashMap;
    use bcdb::test::rand::RngCore;

    #[test]
    fn test () {
        simple_logger::init_with_level(log::Level::Trace).unwrap();

        let mut db = InMemory::new_db("first").unwrap();
        db.init().unwrap();

        let mut rng = thread_rng();

        let mut check = HashMap::new();
        let mut key = [0x0u8;32];
        let mut data = [0x0u8;32];

        for _ in 1 .. 10000 {
            rng.fill_bytes(&mut key);
            rng.fill_bytes(&mut data);
            check.insert(key, data);
            db.put(&key, &data).unwrap();
            assert_eq!(db.get(&key).unwrap().unwrap(), data.to_owned());
        }
        db.batch().unwrap();

        for (k, v) in check.iter() {
            assert_eq!(db.get(k).unwrap(), Some(v.to_vec()));
        }

        for _ in 1 .. 10000 {
            rng.fill_bytes(&mut key);
            assert!(db.get(&key).unwrap().is_none());
        }

        db.shutdown();
    }
}