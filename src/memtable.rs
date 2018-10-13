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
use types::Offset;

use siphasher::sip::SipHasher;
use std::hash::Hasher;

pub struct MemTable {
    step: u32,
    log_mod: u32,
    sip0: u64,
    sip1: u64,
    buckets: Vec<Option<Bucket>>
}

impl MemTable {
    pub fn new (step: u32, buckets: u32, log_mod: u32, sip0: u64, sip1: u64) -> MemTable {
        MemTable {log_mod, step, sip0, sip1, buckets: Vec::with_capacity(buckets as usize)}
    }

    pub fn load (&mut self, bcdb: &mut BCDB) -> Result<(), BCDBError>{
        for bucket in bcdb.bucket_iterator() {
            if bucket.is_valid() {
                let mut next = bucket;
                let mut hashes = Vec::new();
                let mut offsets = Vec::new();
                loop {
                    let (links, n) = bcdb.get_link(next)?;
                    hashes.extend(links.iter().fold(Vec::new(), |mut a, e| { a.push (e.0); a}));
                    offsets.extend(links.iter().fold(Vec::new(), |mut a, e| { a.push (e.1.as_u64()); a}));
                    next = n;
                    if !next.is_valid() {
                        break;
                    }
                }
                self.buckets.push(Some(Bucket{hashes, offsets}));
            }
            else {
                self.buckets.push(None);
            }
        }
        Ok(())
    }

    /// retrieve data offsets by key
    fn get(&mut self, key: &[u8]) -> Result<Vec<Offset>, BCDBError> {
        let hash = self.hash(key);
        let mut bucket_number = (hash & (!0u32 >> (32 - self.log_mod))) as usize; // hash % 2^(log_mod)
        if bucket_number < self.step as usize {
            bucket_number = (hash & (!0u32 >> (32 - self.log_mod - 1))) as usize; // hash % 2^(log_mod + 1)
        }
        let mut result = Vec::new();

        if let Some(Some(bucket)) = self.buckets.get_mut(bucket_number) {
            for (n, h) in bucket.hashes.iter().enumerate() {
                if *h == hash {
                    result.push(Offset::from(*bucket.offsets.get(n).unwrap()));
                }
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

#[derive(Clone)]
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
        let data = [0x0u8;40];
        let mut check = HashMap::new();

        for _ in 0 .. 2 {
            rng.fill_bytes(&mut key);
            let mut k = Vec::new();
            k.push(key.to_vec());
            let o = db.put(k.clone(), &data).unwrap();
            check.insert(key, o);
        }
        db.batch().unwrap();

        let (step, buckets, log_mod, sip0, sip1) = db.get_parameters();
        let mut memtable = MemTable::new(step, buckets, log_mod, sip0, sip1);
        memtable.load(&mut db).unwrap();

        for (k, o) in check {
            assert_eq!(memtable.get(&k[..]).unwrap(), vec!(o));
        }
    }
}

