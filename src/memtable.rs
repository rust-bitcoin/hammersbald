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
//! # The memtable
//! Specific implementation details to in-memory index of the db
//!
//!
use error::BCDBError;
use offset::Offset;
use datafile::{DataFile, Content};
use tablefile::{TableFile, FIRST_PAGE_HEAD, BUCKETS_FIRST_PAGE, BUCKETS_PER_PAGE, BUCKET_SIZE};
use linkfile::LinkFile;
use logfile::LogFile;
use page::{PAGE_SIZE};
use pagedfile::PagedFile;
use tablefile::TablePage;

use siphasher::sip::SipHasher;
use rand::{thread_rng, RngCore};

use std::hash::Hasher;
use std::collections::HashMap;
use std::fmt;
use std::cmp::min;

const BUCKET_FILL_TARGET: u32 = 128;
const INIT_BUCKETS: usize = 512;
const INIT_LOGMOD :usize = 8;

pub struct MemTable {
    step: usize,
    log_mod: u32,
    sip0: u64,
    sip1: u64,
    buckets: Vec<Bucket>,
    dirty: Dirty,
    log_file: LogFile,
    link_file: LinkFile,
    table_file: TableFile,
}

impl MemTable {
    pub fn new (log_file: LogFile, link_file: LinkFile, table_file: TableFile) -> MemTable {
        let mut rng = thread_rng();

        MemTable {log_mod: INIT_LOGMOD as u32, step: 0,
            sip0: rng.next_u64(),
            sip1: rng.next_u64(),
            buckets: vec!(Bucket::default(); INIT_BUCKETS),
            dirty: Dirty::new(INIT_BUCKETS), log_file, link_file, table_file}
    }

    pub fn init (&mut self) -> Result<(), BCDBError> {
        self.link_file.init()?;
        self.log_file.init(0, 0, 0)?;
        Ok(())
    }

    pub fn load (&mut self) -> Result<(), BCDBError>{
        if let Some(first) = self.table_file.read_table_page(Offset::from(0))? {
            let n_buckets = first.read_offset(0)?.as_u64() as u32;
            self.step = first.read_offset(6)?.as_u64() as usize;
            self.log_mod = (32 - n_buckets.leading_zeros()) as u32 - 2;
            self.sip0 = first.read_u64(12)?;
            self.sip1 = first.read_u64(20)?;

            let mut bucket_to_link = HashMap::with_capacity(n_buckets as usize);
            for (n, bucket) in self.table_file.iter().enumerate() {
                if bucket.is_valid() {
                    bucket_to_link.insert(bucket, n);
                }
            }
            let mut buckets = vec!(Bucket::default(); n_buckets as usize);
            for (self_offset, mut links, mut next) in self.link_file.iter() {
                if let Some(bucket_index) = bucket_to_link.get(&self_offset) {
                    let bucket = &mut buckets[*bucket_index];
                    bucket.link = self_offset;
                    loop {

                        let mut hashes = links.iter().fold(Vec::new(), |mut a, e| {
                            a.push(e.0);
                            a
                        });
                        hashes.extend(bucket.hashes.iter());
                        bucket.hashes = hashes;

                        let mut offsets = links.iter().fold(Vec::new(), |mut a, e| {
                            a.push(e.1.as_u64());
                            a
                        });
                        offsets.extend(bucket.offsets.iter());
                        bucket.offsets = offsets;

                        if !next.is_valid() {
                            break;
                        }

                        let (l, n) = self.link_file.get_link(next)?;
                        links = l;
                        next = n;
                    }

                }
            }
        }
        Ok(())
    }

    /// end current batch and start a new batch
    pub fn batch (&mut self, data_len: u64)  -> Result<(), BCDBError> {

        self.log_file.flush()?;
        self.log_file.sync()?;

        self.flush()?;
        self.dirty.clear();

        self.link_file.sync()?;
        let link_len = self.link_file.len()?;
        debug!("link length {}", link_len);

        self.table_file.sync()?;
        let table_len = self.table_file.len()?;
        debug!("table length {}", table_len);

        self.log_file.reset(table_len);
        self.log_file.init(data_len, table_len, link_len)?;
        self.log_file.flush()?;
        self.log_file.sync()?;
        debug!("batch start");

        Ok(())
    }

    /// stop background writer
    pub fn shutdown (&mut self) {
        self.table_file.shutdown();
        self.link_file.shutdown();
        self.log_file.shutdown();
    }

    pub fn recover(&mut self) -> Result<u64, BCDBError> {
        debug!("recover");
        let mut data_len = 0;
        let mut table_len = 0;
        let mut link_len = 0;
        if let Some(page) = self.log_file.read_page(Offset::from(0))? {
            let mut size = [0u8; 6];
            page.read(2, &mut size)?;
            data_len = Offset::from(&size[..]).as_u64();

            page.read(8, &mut size)?;
            table_len = Offset::from(&size[..]).as_u64();
            self.table_file.truncate(table_len)?;

            page.read(14, &mut size)?;
            link_len = Offset::from(&size[..]).as_u64();
            self.link_file.truncate(link_len)?;
        }

        if self.log_file.len()? > PAGE_SIZE as u64 {
            for page in self.log_file.page_iter().skip(1) {
                let table_page = TablePage::from(page);
                self.table_file.write_page(table_page.offset, table_page.page)?;
            }
            self.table_file.flush()?;

            self.table_file.sync()?;
            self.link_file.sync()?;
            self.log_file.reset(table_len);
            self.log_file.init(data_len, table_len, link_len)?;
            self.log_file.flush()?;
            self.log_file.sync()?;
        }

        Ok(data_len)
    }

    /// get link iterator - this also includes no longer used links
    pub fn link_iterator<'a>(&'a self) -> impl Iterator<Item=(Offset, Vec<(u32, Offset)>, Offset)> + 'a {
        self.link_file.iter()
    }

    pub fn flush (&mut self) -> Result<(), BCDBError> {
        if self.dirty.is_dirty() {
            // first page
            let mut page = TablePage::new(Offset::from(0));
            page.write_offset(0, Offset::from(self.buckets.len() as u64))?;
            page.write_offset(6, Offset::from(self.step as u64))?;
            page.write_u64(12, self.sip0)?;
            page.write_u64(20, self.sip1)?;
            for b in 0 .. min(self.buckets.len(), BUCKETS_FIRST_PAGE) {
                self.write_offset_to_page(b, &mut page, b, FIRST_PAGE_HEAD)?;
            }
            self.table_file.write_table_page(page)?;

            // other pages
            let flags : Vec<bool> = self.dirty.page_flags().collect();
            for (pn_1 /* page number - 1 */, dirty) in flags.iter().skip(1).enumerate() {
                if *dirty {
                    page = TablePage::new(Offset::from((pn_1+1) as u64 * PAGE_SIZE as u64));
                    let start = BUCKETS_PER_PAGE*pn_1 + BUCKETS_FIRST_PAGE;
                    let end = min(self.buckets.len(), (pn_1+1)*BUCKETS_PER_PAGE + BUCKETS_FIRST_PAGE);
                    for (n, b) in (start .. end).enumerate() {
                        self.write_offset_to_page(b, &mut page, n, 0)?;
                    }
                    self.table_file.write_table_page(page)?;
                }
            }

            self.link_file.flush()?;
            self.table_file.flush()?;
        }
        Ok(())
    }

    fn write_offset_to_page(&mut self, bucket: usize, page: &mut TablePage, i: usize, head: usize) -> Result<(), BCDBError> {
        let ref mut bucket = self.buckets[bucket];
        let mut link = Offset::invalid();
        let links = bucket.hashes.iter().zip(bucket.offsets.iter())
            .fold(Vec::new(), |mut a, e|
                {
                    a.push((*e.0, Offset::from(*e.1)));
                    a
                });
        for chunk in links.chunks(255) {
            link = self.link_file.append_link(chunk.to_vec(), link)?;
        }
        page.write_offset(i * BUCKET_SIZE + head, link)
    }

    pub fn iter<'a>(&'a self) -> impl Iterator<Item=Offset> +'a {
        BucketIterator{file: self, n:0}
    }

    pub fn put (&mut self,  key: &[u8], data_offset: Offset) -> Result<(), BCDBError>{
        let hash = self.hash(key);
        let bucket = self.bucket_for_hash(hash);
        self.store_to_bucket(bucket, hash, data_offset)?;

        if thread_rng().next_u32() % BUCKET_FILL_TARGET == 0 && self.step < (1 << 31) {
            if self.step < (1 << self.log_mod) {
                let step = self.step;
                self.rehash_bucket(step)?;
            }

            self.step += 1;
            if self.step > (1 << (self.log_mod + 1)) {
                self.log_mod += 1;
                self.step = 0;
            }

            self.buckets.push(Bucket::default());
            self.dirty.append();
        }
        Ok(())
    }

    fn store_to_bucket(&mut self, bucket: usize, hash: u32, offset: Offset) -> Result<(), BCDBError> {
        if let Some(bucket) = self.buckets.get_mut(bucket as usize) {
            bucket.hashes.push(hash);
            bucket.offsets.push(offset.as_u64());
        } else {
            return Err(BCDBError::Corrupted(format!("memtable does not have the bucket {}", bucket).to_string()))
        }
        self.modify_bucket(bucket)?;
        Ok(())
    }

    fn rehash_bucket(&mut self, bucket: usize) -> Result<(), BCDBError> {
        let mut rewrite = false;
        let mut new_bucket_store = Bucket::default();
        let mut moves = HashMap::new();
        if let Some(b) = self.buckets.get(bucket as usize) {
            for (hash, offset) in b.hashes.iter().zip(b.offsets.iter()) {
                let new_bucket = (hash & (!0u32 >> (32 - self.log_mod - 1))) as usize; // hash % 2^(log_mod + 1)
                if new_bucket != bucket {
                    moves.entry(new_bucket).or_insert(Vec::new()).push((*hash, Offset::from(*offset)));
                    rewrite = true;
                } else {
                    new_bucket_store.hashes.push(*hash);
                    new_bucket_store.offsets.push(*offset);
                }
            }
        }
        if rewrite {
            for (bucket, added) in moves {
                for (hash, offset) in added {
                    self.store_to_bucket(bucket, hash, offset)?;
                }
            }
            self.buckets[bucket] = new_bucket_store;
            self.modify_bucket(bucket)?;
        }
        Ok(())
    }

    fn modify_bucket(&mut self, bucket: usize) -> Result<(), BCDBError> {
        self.dirty.set(bucket);
        let bucket_page = if bucket < BUCKETS_FIRST_PAGE { 
            Offset::from(0) 
        } else {
            Offset::from(((bucket - BUCKETS_FIRST_PAGE)/BUCKETS_PER_PAGE + 1) as u64 * PAGE_SIZE as u64)
        };
        self.log_file.log_page(bucket_page, &self.table_file)
    }

    // get the data last associated with the key
    pub fn get_unique(&self, key: &[u8], data_file: &DataFile) -> Result<Option<(Offset, Vec<u8>, Vec<u8>)>, BCDBError> {
        let hash = self.hash(key);
        let bucket_number = self.bucket_for_hash(hash);
        if let Some(bucket) = self.buckets.get(bucket_number) {
            for (n, h) in bucket.hashes.iter().enumerate().rev() {
                if *h == hash {
                    let data_offset = Offset::from(*bucket.offsets.get(n).unwrap());
                    if let Some(Content::Data(data_key, data)) = data_file.get_content(data_offset)? {
                        if data_key.as_slice() == key {
                            return Ok(Some((data_offset, data_key, data)));
                        }
                    }
                }
            }
        }
        Ok(None)
    }

    fn bucket_for_hash(&self, hash: u32) -> usize {
        let mut bucket = (hash & (!0u32 >> (32 - self.log_mod))) as usize; // hash % 2^(log_mod)
        if bucket < self.step {
            bucket = (hash & (!0u32 >> (32 - self.log_mod - 1))) as usize; // hash % 2^(log_mod + 1)
        }
        bucket
    }

    fn hash (&self, key: &[u8]) -> u32 {
        let mut hasher = SipHasher::new_with_keys(self.sip0, self.sip1);
        hasher.write(key);
        hasher.finish() as u32
    }
}

struct Dirty {
    bits: Vec<u64>,
    used: usize
}

impl fmt::Debug for Dirty {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        for b in &self.bits {
            write!(f, "{:064b}", b)?;
        }
        Ok(())
    }
}

impl Dirty {
    pub fn new (n: usize) -> Dirty {
        Dirty{bits: vec!(0u64; (n >> 6) + 1), used: n}
    }

    pub fn set(&mut self, n: usize) {
        self.bits[n >> 6] |= 1 << (n & 0x3f);
    }

    pub fn get(&self, n: usize) -> bool {
        (self.bits[n >> 6] & (1 << (n & 0x3f))) != 0
    }

    pub fn clear(&mut self) {
        for s in &mut self.bits {
            *s = 0;
        }
    }

    pub fn is_dirty (&self) -> bool {
        self.bits.iter().any(|n| *n != 0)
    }

    pub fn page_flags<'m>(&'m self) -> impl Iterator<Item=bool> + 'm {
        PageIterator::new(&self)
    }

    pub fn append(&mut self) {
        self.used += 1;
        if self.used >= (self.bits.len() << 6) {
            self.bits.push(1);
        }
        else {
            let next = self.used;
            self.set(next);
        }
    }
}

struct BucketIterator<'a> {
    file: &'a MemTable,
    n: u32
}

impl<'a> Iterator for BucketIterator<'a> {
    type Item = Offset;

    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        match self.file.buckets.get(self.n as usize) {
            Some(n) => { self.n += 1; Some(n.link) },
            None => None
        }
    }
}

struct PageIterator<'b> {
    bits: &'b Dirty,
    page: usize
}

impl<'b> PageIterator<'b> {
    pub fn new(bits: &'b Dirty) -> PageIterator<'b> {
        PageIterator {bits, page: 0}
    }
}

impl<'b> Iterator for PageIterator<'b> {
    type Item = bool;

    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        if self.page == 0 {
            self.page += 1;
            for i in 0 .. BUCKETS_FIRST_PAGE {
                if self.bits.get(i) {
                    return Some(true);
                }
            }
            return Some(false);
        }
        else {
            let start = BUCKETS_FIRST_PAGE + (self.page - 1) * BUCKETS_PER_PAGE;
            self.page += 1;
            if start < self.bits.used {
                for i in start .. start + BUCKETS_PER_PAGE {
                    if self.bits.get(i) {
                        return Some(true);
                    }
                }
                return Some(false);
            }
        }
        return None;
    }
}

#[derive(Clone, Default)]
pub struct Bucket {
    link: Offset,
    hashes: Vec<u32>,
    offsets: Vec<u64>
}

#[cfg(test)]
mod test {
    extern crate rand;

    use transient::Transient;
    use api::BCDBFactory;
    use api::BCDBAPI;

    use super::*;
    use self::rand::thread_rng;
    use std::collections::HashMap;
    use self::rand::RngCore;

    #[test]
    fn test_dirty() {
        let mut dirty = Dirty::new(63);
        assert_eq!(format!("{:?}", dirty), "0000000000000000000000000000000000000000000000000000000000000000");
        dirty.set(0);
        assert!(dirty.get(0));
        assert_eq!(format!("{:?}", dirty), "0000000000000000000000000000000000000000000000000000000000000001");
        dirty.set(3);
        assert_eq!(format!("{:?}", dirty), "0000000000000000000000000000000000000000000000000000000000001001");
        dirty.append();
        assert_eq!(format!("{:?}", dirty), "00000000000000000000000000000000000000000000000000000000000010010000000000000000000000000000000000000000000000000000000000000001");
        dirty.append();
        assert_eq!(format!("{:?}", dirty), "00000000000000000000000000000000000000000000000000000000000010010000000000000000000000000000000000000000000000000000000000000011");
        assert!(dirty.get(65));
    }

        #[test]
    fn test() {
        let mut db = Transient::new_db("first").unwrap();
        db.init().unwrap();

        let mut rng = thread_rng();
        let mut key = [0x0u8;32];
        let mut data = [0x0u8;40];
        let mut check = HashMap::new();

        for _ in 0 .. 10000 {
            rng.fill_bytes(&mut key);
            rng.fill_bytes(&mut data);
            let o = db.put(&key, &data).unwrap();
            check.insert(key, (o, data.to_vec()));
        }
        db.batch().unwrap();

        for (k, (o, data)) in check {
            assert_eq!(db.get_unique(&k[..]).unwrap().unwrap(), (o, k.to_vec(), data));
        }
        db.shutdown();
    }
}

