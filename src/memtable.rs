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

use siphasher::sip::SipHasher;
use rand::{thread_rng, RngCore};

use std::hash::Hasher;
use std::collections::HashMap;
use std::fmt;

const BUCKET_FILL_TARGET: u32 = 128;

pub struct MemTable {
    step: usize,
    log_mod: u32,
    sip0: u64,
    sip1: u64,
    buckets: Vec<Option<Bucket>>,
    dirty: Dirty
}

impl MemTable {
    pub fn new (step: usize, buckets: usize, log_mod: u32, sip0: u64, sip1: u64) -> MemTable {
        MemTable {log_mod, step, sip0, sip1, buckets: vec!(None; buckets), dirty: Dirty::new(buckets)}
    }

    pub fn load (&mut self, bcdb: &mut BCDB) -> Result<(), BCDBError>{
        let mut offset_to_bucket = HashMap::with_capacity(self.buckets.len());
        for (n, bucket) in bcdb.bucket_iterator().enumerate() {
            if bucket.is_valid() {
                offset_to_bucket.insert(bucket, n);
            }
        }
        for (self_offset, links, _) in bcdb.link_iterator() {
            if let Some(bucket_index) = offset_to_bucket.get(&self_offset) {
                let bucket = self.buckets.get_mut(*bucket_index).unwrap();
                if bucket.is_none() {
                    *bucket = Some(Bucket::default());
                }
                if let Some(ref mut b) = bucket {
                    // note that order is reverse of the link database
                    let mut hashes = links.iter().fold(Vec::new(), |mut a, e| { a.push (e.0); a});
                    let mut offsets = links.iter().fold(Vec::new(), |mut a, e| { a.push (e.1.as_u64()); a});
                    b.hashes.extend(hashes.iter().rev());
                    b.offsets.extend(offsets.iter().rev());
                }
            }
        }
        Ok(())
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

                self.buckets.push(None);
                self.dirty.append();
            }
        }
        Ok(())
    }

    fn store_to_bucket(&mut self, bucket: usize, hash: u32, offset: Offset) -> Result<(), BCDBError> {
        if let Some(some) = self.buckets.get_mut(bucket as usize) {
            if let Some(bucket) = some {
                bucket.hashes.push(hash);
                bucket.offsets.push(offset.as_u64());
            } else {
                *some = Some(Bucket{hashes: vec!(hash), offsets: vec!(offset.as_u64())});
            }
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
        if let Some(Some(b)) = self.buckets.get(bucket as usize) {
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
            self.buckets[bucket] = Some(new_bucket_store);
            self.dirty.set(bucket);
        }
        Ok(())
    }

    pub fn flush (table_file: &mut TableFile) {
        unimplemented!()
    }

    /// retrieve data offsets by key
    pub fn get<'a>(&'a self, key: &[u8], data_file: &'a DataFile) -> impl MayFailIterator<(Offset, Vec<Vec<u8>>, Vec<u8>)> + 'a {
        let hash = self.hash(key);
        let bucket_number = self.bucket_for_hash(hash);
        let mut offsets = Vec::new();

        if let Some(Some(bucket)) = self.buckets.get(bucket_number) {
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
        if let Some(Some(bucket)) = self.buckets.get(bucket_number) {
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

        let (step, buckets, log_mod, sip0, sip1) = db.get_parameters();
        let mut memtable = MemTable::new(step as usize, buckets as usize, log_mod, sip0, sip1);
        memtable.load(&mut db).unwrap();

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

