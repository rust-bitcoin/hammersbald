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
use error::{BCDBError, MayFailIterator};
use bcdb::BCDB;
use offset::Offset;
use datafile::{DataFile, Content};
use table::TableFile;
use linkfile::LinkFile;
use page::{PAGE_SIZE, TablePage};

use siphasher::sip::SipHasher;
use rand::{thread_rng, RngCore};

use std::hash::Hasher;
use std::collections::HashMap;
use std::fmt;

const BUCKET_FILL_TARGET: u32 = 128;

const FIRST_PAGE_HEAD:usize = 28;
const INIT_BUCKETS: usize = 512;
const INIT_LOGMOD :usize = 8;
const BUCKETS_FIRST_PAGE:usize = 677;
const BUCKETS_PER_PAGE:usize = 681;
const BUCKET_SIZE: usize = 6;

pub struct MemTable {
    step: usize,
    log_mod: u32,
    sip0: u64,
    sip1: u64,
    buckets: Vec<Bucket>,
    dirty: Dirty
}

impl MemTable {
    pub fn new () -> MemTable {
        let mut rng = thread_rng();

        MemTable {log_mod: INIT_LOGMOD as u32, step: 0,
            sip0: rng.next_u64(),
            sip1: rng.next_u64(),
            buckets: vec!(Bucket::default(); INIT_BUCKETS),
            dirty: Dirty::new(INIT_BUCKETS)}
    }

    pub fn load (table_file: &TableFile, link_file: &LinkFile) -> Result<MemTable, BCDBError>{
        if let Some(first) = table_file.read_page(Offset::from(0))? {
            let n_buckets = first.read_offset(0)?.as_u64() as u32;
            let step = first.read_offset(6)?.as_u64() as usize;
            let log_mod = (32 - n_buckets.leading_zeros()) as u32 - 2;
            let sip0 = first.read_u64(12)?;
            let sip1 = first.read_u64(20)?;

            let mut bucket_to_link = HashMap::with_capacity(n_buckets as usize);
            for (n, bucket) in table_file.iter().enumerate() {
                if bucket.is_valid() {
                    // TODO: follow link next
                    bucket_to_link.insert(bucket, n);
                }
            }
            let mut buckets = vec!(Bucket::default(); n_buckets as usize);
            for (self_offset, links, _) in link_file.iter() {
                if let Some(bucket_index) = bucket_to_link.get(&self_offset) {
                    let bucket = &mut buckets[*bucket_index];
                    bucket.link = self_offset;
                    // note that order is reverse of the link database
                    let mut hashes = links.iter().fold(Vec::new(), |mut a, e| {
                        a.push(e.0);
                        a
                    });
                    let mut offsets = links.iter().fold(Vec::new(), |mut a, e| {
                        a.push(e.1.as_u64());
                        a
                    });
                    bucket.hashes.extend(hashes.iter().rev());
                    bucket.offsets.extend(offsets.iter().rev());
                }
            }
            Ok(MemTable {log_mod, step, sip0, sip1, buckets, dirty: Dirty::new(n_buckets as usize) })
        }
        else {
            Ok(MemTable::new())
        }
    }

    pub fn flush (&mut self, table_file: &mut TableFile, link_file: &mut LinkFile) -> Result<(), BCDBError> {
        if self.dirty.is_dirty() {
            // first page
            let mut page = TablePage::new(Offset::from(0));
            page.write_offset(0, Offset::from(self.buckets.len() as u64))?;
            page.write_offset(6, Offset::from(self.step as u64))?;
            page.write_u64(12, self.sip0)?;
            page.write_u64(20, self.sip1)?;
            for b in 0 .. BUCKETS_FIRST_PAGE {
                Self::write_offset_to_page(&mut self.buckets[b], link_file, &mut page, b)?;
            }
            table_file.write_page(page)?;

            // other pages
            for (pn_1 /* page number - 1 */, dirty) in self.dirty.page_flags().skip(1).enumerate() {
                if dirty {
                    page = TablePage::new(Offset::from((pn_1+1) as u64 * PAGE_SIZE as u64));
                    for (n, b) in (BUCKETS_PER_PAGE*pn_1 + BUCKETS_FIRST_PAGE .. (pn_1+1)*BUCKETS_PER_PAGE + BUCKETS_FIRST_PAGE).enumerate() {
                        Self::write_offset_to_page(&mut self.buckets[b], link_file, &mut page, n)?;
                    }
                    table_file.write_page(page)?;
                }
            }

            link_file.flush()?;
            table_file.flush()?;
        }
        Ok(())
    }

    fn write_offset_to_page(bucket: &mut Bucket, link_file: &mut LinkFile, page: &mut TablePage, i: usize) -> Result<(), BCDBError> {
        let mut link = Offset::invalid();
        let links = bucket.hashes.iter().zip(bucket.offsets.iter())
            .fold(Vec::new(), |mut a, e|
                {
                    a.push((*e.0, Offset::from(*e.1)));
                    a
                });
        for chunk in links.chunks(255) {
            let mut links = chunk.to_vec();
            links.reverse();
            link = link_file.append_link(links, link)?;
        }
        page.write_offset(i * BUCKET_SIZE + FIRST_PAGE_HEAD, link)
    }

    pub fn put (&mut self,  keys: Vec<Vec<u8>>, data_offset: Offset) -> Result<(), BCDBError>{
        for key in keys {
            let hash = self.hash(key.as_slice());
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
        self.dirty.set(bucket);
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
            self.dirty.set(bucket);
        }
        Ok(())
    }


    /// retrieve data offsets by key
    pub fn get<'a>(&'a self, key: &[u8], data_file: &'a DataFile) -> impl MayFailIterator<(Offset, Vec<Vec<u8>>, Vec<u8>)> + 'a {
        let hash = self.hash(key);
        let bucket_number = self.bucket_for_hash(hash);
        let mut offsets = Vec::new();

        if let Some(bucket) = self.buckets.get(bucket_number) {
            for (n, h) in bucket.hashes.iter().enumerate().rev() {
                if *h == hash {
                    offsets.push(Offset::from(*bucket.offsets.get(n).unwrap()));
                }
            }
        }
        GetIterator::new(key.to_vec(), offsets, data_file)
    }

    // get the data last associated with the key
    pub fn get_unique(&self, key: &[u8], data_file: &DataFile) -> Result<Option<(Offset, Vec<Vec<u8>>, Vec<u8>)>, BCDBError> {
        let hash = self.hash(key);
        let bucket_number = self.bucket_for_hash(hash);
        if let Some(bucket) = self.buckets.get(bucket_number) {
            for (n, h) in bucket.hashes.iter().enumerate().rev() {
                if *h == hash {
                    let data_offset = Offset::from(*bucket.offsets.get(n).unwrap());
                    if let Some(Content::Data(keys, data)) = data_file.get_content(data_offset)? {
                        if keys.iter().any(|k| *k == key) {
                            return Ok(Some((data_offset, keys, data)));
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
            for i in 0 .. BUCKETS_FIRST_PAGE {
                if self.bits.get(i) {
                    return Some(true);
                }
            }
            return Some(false);
        }
        else {
            let start = BUCKETS_FIRST_PAGE + (self.page - 1) * BUCKETS_PER_PAGE;
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

struct GetIterator<'data> {
    inner: GetIteratorInner<'data>
}

struct GetIteratorInner<'data> {
    key: Vec<u8>,
    pos: usize,
    offsets: Vec<Offset>,
    data_file: &'data DataFile,
    error: Option<String>
}

impl<'data> GetIterator<'data> {
    pub fn new (key: Vec<u8>, offsets: Vec<Offset>, data_file: &'data DataFile) -> GetIterator<'data> {
        GetIterator{inner: GetIteratorInner{key, offsets, data_file, pos: 0, error: None}}
    }
}

impl<'data> MayFailIterator<(Offset, Vec<Vec<u8>>, Vec<u8>)> for GetIterator<'data> {
    fn next(&mut self) -> Result<Option<(Offset, Vec<Vec<u8>>, Vec<u8>)>, BCDBError> {
        match self.inner.next() {
            Some(n) => Ok(Some(n)),
            None => if let Some(ref error) = self.inner.error {
                Err(BCDBError::Corrupted(error.clone()))
            }
            else {
                Ok(None)
            }
        }
    }
}

impl<'data> IntoIterator for GetIterator<'data> {
    type Item = (Offset, Vec<Vec<u8>>, Vec<u8>);
    type IntoIter = GetIteratorInner<'data>;

    fn into_iter(self) -> <Self as IntoIterator>::IntoIter {
        self.inner
    }
}

impl<'data> Iterator for GetIteratorInner<'data> {
    type Item = (Offset, Vec<Vec<u8>>, Vec<u8>);

    fn next(&mut self) -> Option<(Offset, Vec<Vec<u8>>, Vec<u8>)> {
        use std::error::Error;

        while self.pos < self.offsets.len() {
            let data_offset = self.offsets[self.pos];
            self.pos += 1;
            match self.data_file.get_content(data_offset) {
                Ok(Some(Content::Data(keys, data))) => if keys.iter().any(|key| *key == self.key) {
                    return Some((data_offset, keys, data));
                }
                Ok(_) => { self.error = Some(format!("offset {} should point to data", data_offset)); return None }
                Err(ref error) => { self.error = Some(error.description().to_string()); return None }
            }
        }
        None
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

    use inmemory::InMemory;
    use bcdb::BCDBFactory;
    use bcdb::BCDBAPI;

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
        let mut db = InMemory::new_db("first").unwrap();
        db.init().unwrap();

        let mut rng = thread_rng();
        let mut key = [0x0u8;32];
        let mut data = [0x0u8;40];
        let mut check = HashMap::new();

        for _ in 0 .. 10000{
            rng.fill_bytes(&mut key);
            rng.fill_bytes(&mut data);
            let mut k = Vec::new();
            k.push(key.to_vec());
            let o = db.put(k.clone(), &data).unwrap();
            check.insert(key, (o, data.to_vec()));
        }
        db.batch().unwrap();

        let mut memtable = MemTable::load(&db.table, &db.link).unwrap();

        for (k, (o, data)) in &check {
            assert_eq!(memtable.get(&k[..], &db.data).next().unwrap().unwrap(), (*o, vec!(k.to_vec()), data.clone()));
        }

        check.clear();
        for _ in 0 .. 10000 {
            rng.fill_bytes(&mut key);
            rng.fill_bytes(&mut data);
            let mut k = Vec::new();
            k.push(key.to_vec());
            let o = db.data.append_data(k.clone(), &data).unwrap();
            memtable.put(k.clone(), o).unwrap();
            check.insert(key, (o, data.to_vec()));
        }
        for (k, (o, data)) in check {
            assert_eq!(memtable.get_unique(&k[..], &db.data).unwrap().unwrap(), (o, vec!(k.to_vec()), data));
        }
    }
}

