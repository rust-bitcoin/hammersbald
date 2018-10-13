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
use bcdb::BCDB;
use offset::Offset;
use datafile::{DataFile, Content};

use siphasher::sip::SipHasher;
use rand::{thread_rng, RngCore};

use std::hash::Hasher;
use std::collections::HashMap;

const BUCKET_FILL_TARGET: u32 = 2;

pub struct MemTable {
    step: u32,
    log_mod: u32,
    sip0: u64,
    sip1: u64,
    buckets: Vec<Option<Bucket>>
}

impl MemTable {
    pub fn new (step: u32, buckets: u32, log_mod: u32, sip0: u64, sip1: u64) -> MemTable {
        MemTable {log_mod, step, sip0, sip1, buckets: vec!(None; buckets as usize)}
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
            let mut bucket = hash & (!0u32 >> (32 - self.log_mod)); // hash % 2^(log_mod)
            if bucket < self.step {
                bucket = hash & (!0u32 >> (32 - self.log_mod - 1)); // hash % 2^(log_mod + 1)
            }
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
            }
        }
        Ok(())
    }

    fn store_to_bucket(&mut self, bucket: u32, hash: u32, offset: Offset) -> Result<(), BCDBError> {
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
        Ok(())
    }

    fn rehash_bucket(&mut self, bucket: u32) -> Result<(), BCDBError> {
        let mut rewrite = false;
        let mut new_bucket_store = Bucket::default();
        let mut moves = HashMap::new();
        if let Some(Some(b)) = self.buckets.get_mut(bucket as usize) {
            for (hash, offset) in b.hashes.iter().zip(b.offsets.iter()) {
                let new_bucket = hash & (!0u32 >> (32 - self.log_mod - 1)); // hash % 2^(log_mod + 1)
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
            self.buckets[bucket as usize] = Some(new_bucket_store);
        }
        Ok(())
    }

    /// retrieve data offsets by key
    pub fn get(&self, key: &[u8], data_file: &DataFile) -> Result<Vec<(Offset, Vec<Vec<u8>>, Vec<u8>)>, BCDBError> {
        let hash = self.hash(key);
        let mut bucket_number = (hash & (!0u32 >> (32 - self.log_mod))) as usize; // hash % 2^(log_mod)
        if bucket_number < self.step as usize {
            bucket_number = (hash & (!0u32 >> (32 - self.log_mod - 1))) as usize; // hash % 2^(log_mod + 1)
        }
        let mut result = Vec::new();

        if let Some(Some(bucket)) = self.buckets.get(bucket_number) {
            let mut fih = false;
            for (n, h) in bucket.hashes.iter().enumerate().rev() {
                if *h == hash {
                    fih = true;
                    let data_offset = Offset::from(*bucket.offsets.get(n).unwrap());
                    if let Some(Content::Data(keys, data)) = data_file.get_content(data_offset)? {
                        if keys.iter().any(|k| k.as_slice() == key) {
                            result.push((data_offset, keys, data));
                        }
                    } else {
                        return Err(BCDBError::Corrupted("bucket should point to data".to_string()))
                    }
                }
            }
            if !fih {
                println!("not found in hash");
            }
        }
        Ok(result)
    }

    fn hash (&self, key: &[u8]) -> u32 {
        let mut hasher = SipHasher::new_with_keys(self.sip0, self.sip1);
        hasher.write(key);
        hasher.finish() as u32
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
        let mut memtable = MemTable::new(step, buckets, log_mod, sip0, sip1);
        memtable.load(&mut db).unwrap();

        for (k, (o, data)) in &check {
            assert_eq!(memtable.get(&k[..], &db.data).unwrap(), vec!((*o, vec!(k.to_vec()), data.clone())));
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
            assert_eq!(memtable.get(&k[..], &db.data).unwrap(), vec!((o, vec!(k.to_vec()), data)));
        }
    }
}

