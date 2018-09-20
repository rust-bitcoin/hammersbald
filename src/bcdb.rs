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

/// fixed key length of 256 bits
pub const KEY_LEN : usize = 32;

/// a trait to create a new db
pub trait BCDBFactory {
    /// create a new db
    fn new_db (name: &str) -> Result<BCDB, BCSError>;
}

/// a read-write-seak-able storage with added methods
/// synchronized in its implementation
pub trait PageFile : Send + Sync {
    /// flush buffered writes
    fn flush(&mut self) -> Result<(), BCSError>;
    /// length of the storage
    fn len (&self) -> Result<u64, BCSError>;
    /// truncate storage
    fn truncate(&mut self, new_len: u64) -> Result<(), BCSError>;
    /// tell OS to flush buffers to disk
    fn sync (&self) -> Result<(), BCSError>;
    /// read a page at given offset
    fn read_page (&self, offset: Offset) -> Result<Page, BCSError>;
    /// append a page (ignore offset in the Page)
    fn append_page (&mut self, page: Page) -> Result<(), BCSError>;
    /// write a page at its position as specified in page.offset
    fn write_page (&mut self, page: Page) -> Result<(), BCSError>;
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
        debug!("recover");
        for page in log.page_iter() {
            if !first {
                debug!("recover BCDB: patch page {}", page.offset.as_u64());
                self.table.patch_page(page)?;
            }
            else {
                let mut size = [0u8; 6];
                page.read(2, &mut size)?;
                let data_len = Offset::from_slice(&size)?.as_u64();
                self.data.truncate(data_len)?;

                page.read(8, &mut size)?;
                let table_len = Offset::from_slice(&size)?.as_u64();
                self.table.truncate(table_len)?;
                first = false;
                debug!("recover BCDB: set lengths to table: {} data: {}", table_len, data_len);
            }
        }
        Ok(())
    }

    /// end current batch and start a new batch
    pub fn batch (&mut self)  -> Result<(), BCSError> {
        debug!("batch end");
        self.data.flush()?;
        self.data.sync()?;
        self.data.clear_cache();
        self.table.flush()?;
        self.table.sync()?;
        self.table.clear_cache();
        let data_len = self.data.len()?;
        let table_len = self.table.len()?;

        let mut log = self.log.lock().unwrap();
        log.clear_cache();
        log.truncate(0)?;

        let mut first = Page::new(Offset::new(0).unwrap());
        first.write(0, &[0xBC, 0x00]).unwrap();
        let mut size = [0u8; 6];
        Offset::new(data_len)?.serialize(&mut size);
        first.write(2, &size).unwrap();
        Offset::new(table_len)?.serialize(&mut size);
        first.write(8, &size).unwrap();
        log.tbl_len = table_len;


        log.append_page(first)?;
        log.flush()?;
        log.sync()?;
        log.clear_cache();
        debug!("batch start");

        Ok(())
    }

    /// stop background writer
    pub fn shutdown (&mut self) {
        self.data.shutdown();
        self.table.shutdown();
    }

    /// store data with a key
    /// storing with the same key makes previous data unaccessible
    pub fn put(&mut self, key: &[u8], data: &[u8]) -> Result<(Offset, i32), BCSError> {
        if key.len() != KEY_LEN {
            return Err(BCSError::DoesNotFit);
        }
        let offset = self.data.append(DataEntry::new_data(key, data))?;
        let spill = self.table.put(key, offset, &mut self.data)?;
        Ok((offset, spill))
    }

    /// retrieve data by key
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, BCSError> {
        if key.len() != KEY_LEN {
            return Err(BCSError::DoesNotFit);
        }
        self.table.get(key, &self.data)
    }

    /// Insert a block header
    pub fn insert_header (&mut self, header: &BlockHeader, extension: &Vec<Vec<u8>>) -> Result<Offset, BCSError> {
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

        Ok(self.put(key.as_slice(), serialized_header.as_slice())?.0)
    }

    /// Fetch a header by its id
    pub fn fetch_header (&self, id: &Sha256dHash)  -> Result<Option<(BlockHeader, Vec<Vec<u8>>)>, BCSError> {
        let key = encode(id)?;
        if let Some(stored) = self.get(key.as_slice())? {
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
                        return Err(BCSError::Corrupted(format!("expected app data extension {}", offset.as_u64())));
                    }
                }
                else {
                    return Err(BCSError::Corrupted(format!("can not find app data extension {}", offset.as_u64())));
                }
            }

            Ok(Some((header, extension)))
        }
        else {
            Ok(None)
        }
    }

    /// insert a block
    pub fn insert_block(&mut self, block: &Block, extension: &Vec<Vec<u8>>) -> Result<Offset, BCSError> {
        let key = encode(&block.bitcoin_hash())?;
        let mut serialized_block = encode(&block.header)?;

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
                &encode(&t.txid())?.as_slice(), &encode(t)?.as_slice())?.0;
            let mut did = [0u8;6];
            offset.serialize(&mut did);
            serialized_block.append(&mut did.to_vec());
        }

        let offset = self.put(key.as_slice(),serialized_block.as_slice())?.0;
        Ok(offset)
    }

    /// Fetch a block by its id
    pub fn fetch_block (&self, id: &Sha256dHash)  -> Result<Option<(Block, Vec<Vec<u8>>)>, BCSError> {
        let key = encode(id)?;
        if let Some(stored) = self.get(&key.as_slice())? {
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
                        return Err(BCSError::Corrupted(format!("expected app data extension {}", offset.as_u64())));
                    }
                }
                else {
                    return Err(BCSError::Corrupted(format!("can not find app data extension {}", offset.as_u64())));
                }
            }

            let n_transactions = U24::from_slice(&stored.as_slice()[83+n_extensions*6 .. 83+n_extensions*6+3])?.as_usize();
            let mut txdata: Vec<Transaction> = Vec::new();
            for i in 0 .. n_transactions {
                let offset = Offset::from_slice(&stored.as_slice()[83+n_extensions*6+3+i*6 .. 83+n_extensions*6+3+(i+1)*6])?;
                if let Some (tx) = self.data.get(offset)? {
                    txdata.push(decode(tx.data)?);
                }
                else {
                    return Err(BCSError::Corrupted(format!("can not find transaction of a block {}", offset.as_u64())));
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
            return Ok(Some(decode(stored)?));
        }
        Ok(None)
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
    type Item = Page;

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
        let mut data = [0x0u8;32];

        let mut spill = 0;
        for _ in 1 .. 100000 {
            rng.fill_bytes(&mut key);
            rng.fill_bytes(&mut data);
            check.insert(key, data);
            spill += db.put(&key, &data).unwrap().1;
            assert_eq!(db.get(&key).unwrap().unwrap(), data.to_owned());
        }
        db.batch().unwrap();
        println!("spillovers: {}", spill);

        for (k, v) in check.iter() {
            assert_eq!(db.get(k).unwrap(), Some(v.to_vec()));
        }


        db.shutdown();
    }

    #[test]
    fn hashtest() {
        let mut db = InMemory::new_db("first").unwrap();
        db.init().unwrap();
        db.put(encode(&Sha256dHash::default()).unwrap().as_slice(), encode(&Sha256dHash::default()).unwrap().as_slice()).unwrap();
        assert_eq!(Some(decode(db.get(encode(&Sha256dHash::default()).unwrap().as_slice()).unwrap().unwrap()).unwrap()), Some(Sha256dHash::default()));
    }

    #[test]
    fn block_test () {
        let block : Block = decode(hex::decode("0000002060bbab0edbf3ef8a49608ee326f8fd75c473b7e3982095e2d100000000000000c30134f8c9b6d2470488d7a67a888f6fa12f8692e0c3411fbfb92f0f68f67eedae03ca57ef13021acc22dc4105010000000001010000000000000000000000000000000000000000000000000000000000000000ffffffff2f0315230e0004ae03ca57043e3d1e1d0c8796bf579aef0c0000000000122f4e696e6a61506f6f6c2f5345475749542fffffffff038427a112000000001976a914876fbb82ec05caa6af7a3b5e5a983aae6c6cc6d688ac0000000000000000266a24aa21a9ed5c748e121c0fe146d973a4ac26fa4a68b0549d46ee22d25f50a5e46fe1b377ee00000000000000002952534b424c4f434b3acd16772ad61a3c5f00287480b720f6035d5e54c9efc71be94bb5e3727f10909001200000000000000000000000000000000000000000000000000000000000000000000000000100000000010145310e878941a1b2bc2d33797ee4d89d95eaaf2e13488063a2aa9a74490f510a0100000023220020b6744de4f6ec63cc92f7c220cdefeeb1b1bed2b66c8e5706d80ec247d37e65a1ffffffff01002d3101000000001976a9143ebc40e411ed3c76f86711507ab952300890397288ac0400473044022001dd489a5d4e2fbd8a3ade27177f6b49296ba7695c40dbbe650ea83f106415fd02200b23a0602d8ff1bdf79dee118205fc7e9b40672bf31563e5741feb53fb86388501483045022100f88f040e90cc5dc6c6189d04718376ac19ed996bf9e4a3c29c3718d90ffd27180220761711f16c9e3a44f71aab55cbc0634907a1fa8bb635d971a9a01d368727bea10169522103b3623117e988b76aaabe3d63f56a4fc88b228a71e64c4cc551d1204822fe85cb2103dd823066e096f72ed617a41d3ca56717db335b1ea47a1b4c5c9dbdd0963acba621033d7c89bd9da29fa8d44db7906a9778b53121f72191184a9fee785c39180e4be153ae00000000010000000120925534261de4dcebb1ed5ab1b62bfe7a3ef968fb111dc2c910adfebc6e3bdf010000006b483045022100f50198f5ae66211a4f485190abe4dc7accdabe3bc214ebc9ea7069b97097d46e0220316a70a03014887086e335fc1b48358d46cd6bdc9af3b57c109c94af76fc915101210316cff587a01a2736d5e12e53551b18d73780b83c3bfb4fcf209c869b11b6415effffffff0220a10700000000001976a91450333046115eaa0ac9e0216565f945070e44573988ac2e7cd01a000000001976a914c01a7ca16b47be50cbdbc60724f701d52d75156688ac00000000010000000203a25f58630d7a1ea52550365fd2156683f56daf6ca73a4b4bbd097e66516322010000006a47304402204efc3d70e4ca3049c2a425025edf22d5ca355f9ec899dbfbbeeb2268533a0f2b02204780d3739653035af4814ea52e1396d021953f948c29754edd0ee537364603dc012103f7a897e4dbecab2264b21917f90664ea8256189ea725d28740cf7ba5d85b5763ffffffff03a25f58630d7a1ea52550365fd2156683f56daf6ca73a4b4bbd097e66516322000000006a47304402202d96defdc5b4af71d6ba28c9a6042c2d5ee7bc6de565d4db84ef517445626e03022022da80320e9e489c8f41b74833dfb6a54a4eb5087cdb46eb663eef0b25caa526012103f7a897e4dbecab2264b21917f90664ea8256189ea725d28740cf7ba5d85b5763ffffffff0200e1f5050000000017a914b7e6f7ff8658b2d1fb107e3d7be7af4742e6b1b3876f88fc00000000001976a914913bcc2be49cb534c20474c4dee1e9c4c317e7eb88ac0000000001000000043ffd60d3818431c495b89be84afac205d5d1ed663009291c560758bbd0a66df5010000006b483045022100f344607de9df42049688dcae8ff1db34c0c7cd25ec05516e30d2bc8f12ac9b2f022060b648f6a21745ea6d9782e17bcc4277b5808326488a1f40d41e125879723d3a012103f7a897e4dbecab2264b21917f90664ea8256189ea725d28740cf7ba5d85b5763ffffffffa5379401cce30f84731ef1ba65ce27edf2cc7ce57704507ebe8714aa16a96b92010000006a473044022020c37a63bf4d7f564c2192528709b6a38ab8271bd96898c6c2e335e5208661580220435c6f1ad4d9305d2c0a818b2feb5e45d443f2f162c0f61953a14d097fd07064012103f7a897e4dbecab2264b21917f90664ea8256189ea725d28740cf7ba5d85b5763ffffffff70e731e193235ff12c3184510895731a099112ffca4b00246c60003c40f843ce000000006a473044022053760f74c29a879e30a17b5f03a5bb057a5751a39f86fa6ecdedc36a1b7db04c022041d41c9b95f00d2d10a0373322a9025dba66c942196bc9d8adeb0e12d3024728012103f7a897e4dbecab2264b21917f90664ea8256189ea725d28740cf7ba5d85b5763ffffffff66b7a71b3e50379c8e85fc18fe3f1a408fc985f257036c34702ba205cef09f6f000000006a4730440220499bf9e2db3db6e930228d0661395f65431acae466634d098612fd80b08459ee022040e069fc9e3c60009f521cef54c38aadbd1251aee37940e6018aadb10f194d6a012103f7a897e4dbecab2264b21917f90664ea8256189ea725d28740cf7ba5d85b5763ffffffff0200e1f5050000000017a9148fc37ad460fdfbd2b44fe446f6e3071a4f64faa6878f447f0b000000001976a914913bcc2be49cb534c20474c4dee1e9c4c317e7eb88ac00000000").unwrap()).unwrap();
        let mut db = InMemory::new_db("first").unwrap();

        db.init().unwrap();

        let mut extra = Vec::new();
        extra.push([0u8;2].to_vec());
        extra.push([2u8;6].to_vec());

        db.insert_header(&block.header, &extra).unwrap();
        db.batch().unwrap();
        assert_eq!(db.fetch_header(&block.header.bitcoin_hash()).unwrap().unwrap(), (block.header, extra.clone()));

        db.insert_block(&block, &extra).unwrap();
        db.batch().unwrap();
        assert_eq!(db.fetch_block(&block.bitcoin_hash()).unwrap().unwrap(), (block, extra.clone()));
        db.fetch_transaction(&Sha256dHash::from_hex("2b9baddbd2861c663978a98c6c3c7648e1cd5c41b451f4a35b7851dd4786d9d3").unwrap()).unwrap().unwrap();
        db.fetch_transaction(&Sha256dHash::from_hex("d06d86bacf88f1f316d4470080b7869f1c298b850e7b219124ae131c0475abb0").unwrap()).unwrap().unwrap();
        db.fetch_transaction(&Sha256dHash::from_hex("06eee51317a76a76c67499c8f782819745b58d28cdb4d8357ef7f7e6d79cc513").unwrap()).unwrap().unwrap();
        db.fetch_transaction(&Sha256dHash::from_hex("f56da6d0bb5807561c29093066edd1d505c2fa4ae89bb895c4318481d360fd3f").unwrap()).unwrap().unwrap();
        db.fetch_transaction(&Sha256dHash::from_hex("32a52be869fc148b6104244859c879f1319cfd86e89e6f7fc1ffaaf518fa14be").unwrap()).unwrap().unwrap();

    }
}