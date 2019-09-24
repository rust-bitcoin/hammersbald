//
// Copyright 2018-2019 Tamas Blummer
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
use error::Error;
use pref::PRef;
use datafile::{DataFile, EnvelopeIterator};
use tablefile::{TableFile, FIRST_PAGE_HEAD, BUCKETS_FIRST_PAGE, BUCKETS_PER_PAGE, BUCKET_SIZE};
use logfile::LogFile;
use page::PAGE_SIZE;
use pagedfile::PagedFile;
use format::{Link, Payload, Envelope};
use page::Page;

use bitcoin_hashes::siphash24;
use rand::{thread_rng, RngCore};

use std::collections::HashMap;
use std::fmt;
use std::cmp::{min, max};
use std::sync::RwLock;

const INIT_BUCKETS: usize = 512;
const INIT_LOGMOD :usize = 8;

pub struct MemTable {
    step: usize,
    forget: usize,
    log_mod: u32,
    sip0: u64,
    sip1: u64,
    buckets: RwLock<Vec<Bucket>>,
    dirty: Dirty,
    log_file: LogFile,
    data_file: DataFile,
    table_file: TableFile,
    link_file: DataFile,
    bucket_fill_target: usize
}

impl MemTable {
    pub fn new (log_file: LogFile, table_file: TableFile, data_file: DataFile, link_file: DataFile, bucket_fill_target: usize) -> MemTable {
        let mut rng = thread_rng();

        MemTable {log_mod: INIT_LOGMOD as u32, step: 0, forget: 0,
            sip0: rng.next_u64(),
            sip1: rng.next_u64(),
            buckets: RwLock::new(vec!(Bucket::default(); INIT_BUCKETS)),
            dirty: Dirty::new(INIT_BUCKETS), log_file, table_file, data_file, link_file,
            bucket_fill_target: max(min(bucket_fill_target, 128), 1)}
    }

    pub fn params(&self) -> (usize, u32, usize, u64, u64, u64, u64, u64) {
        (self.step, self.log_mod, self.buckets.read().unwrap().len(), self.table_file.len().unwrap(), self.data_file.len().unwrap(), self.link_file.len().unwrap(),
        self.sip0, self.sip1)
    }

    /// end current batch and start a new batch
    pub fn batch (&mut self)  -> Result<(), Error> {
        self.log_file.flush()?;
        self.log_file.sync()?;

        self.flush()?;
        self.dirty.clear();

        self.table_file.sync()?;
        let table_len = self.table_file.len()?;

        self.link_file.sync()?;
        let link_len = self.link_file.len()?;

        self.data_file.flush()?;
        self.data_file.sync()?;
        let data_len = self.data_file.len()?;

        self.log_file.reset(table_len);
        self.log_file.init(data_len, table_len, link_len)?;
        self.log_file.flush()?;
        self.log_file.sync()?;

        Ok(())
    }

    /// stop background writer
    pub fn shutdown (&mut self) {
        self.data_file.shutdown();
        self.link_file.shutdown();
        self.table_file.shutdown();
        self.log_file.shutdown();
    }

    pub fn recover(&mut self) -> Result<(), Error> {
        let mut data_len = 0;
        let mut table_len = 0;
        let mut link_len = 0;
        if let Some(page) = self.log_file.read_page(PRef::from(0))? {
            data_len = page.read_pref(0).as_u64();
            table_len = page.read_pref(6).as_u64();
            link_len = page.read_pref(12).as_u64();

            self.table_file.truncate(table_len)?;
            self.data_file.truncate(data_len)?;
            self.link_file.truncate(link_len)?;
        }

        if self.log_file.len()? > PAGE_SIZE as u64 {
            for page in self.log_file.page_iter().skip(1) {
                self.table_file.update_page(page)?;
            }
            self.table_file.flush()?;
            self.table_file.sync()?;

            self.log_file.init(data_len, table_len, link_len)?;
            self.log_file.flush()?;
            self.log_file.sync()?;
        }

        Ok(())
    }

    pub fn load (&mut self) -> Result<(), Error>{
        if let Some(first) = self.table_file.read_page(PRef::from(0))? {
            let n_buckets = first.read_pref(0).as_u64() as u32;
            self.buckets = RwLock::new(vec![Bucket::default(); n_buckets as usize]);
            self.dirty = Dirty::new(n_buckets as usize);
            self.step = first.read_pref(6).as_u64() as usize;
            self.log_mod = (32 - n_buckets.leading_zeros()) as u32 - 2;
            self.sip0 = first.read_u64(12);
            self.sip1 = first.read_u64(20);
        }

        let mut buckets = self.buckets.write().unwrap();

        for (i, link) in self.table_file.iter().enumerate() {
            if i < buckets.len() {
                buckets[i].stored = link;
            }
            else {
                break;
            }
        }

        Ok(())
    }

    fn resolve_bucket(&self, bucket_number: usize) -> Result<(), Error> {
        if let Some(bucket) = self.buckets.write().unwrap().get_mut(bucket_number) {
            if bucket.slots.is_none () {
                if bucket.stored.is_valid() {
                    if let Ok(Payload::Link(link)) = Payload::deserialize(self.link_file.get_envelope(bucket.stored)?.payload()) {
                        bucket.slots = Some(link.slots());
                    }
                }
            }
        }
        Ok(())
    }

    pub fn flush (&mut self) -> Result<(), Error> {
        {
            // first page
            let fp = PRef::from(0);
            let mut page = self.table_file.read_page(fp)?.unwrap_or(Self::invalid_offsets_page(fp));
            page.write_pref(0, PRef::from(self.buckets.read().unwrap().len() as u64));
            page.write_pref(6, PRef::from(self.step as u64));
            page.write_u64(12, self.sip0);
            page.write_u64(20, self.sip1);
            self.table_file.update_page(page)?;
        }
        if self.dirty.is_dirty() {
            let dirty_iterator = DirtyIterator::new(&self.dirty);
            for (bucket_number, _) in dirty_iterator.enumerate().filter(|a| a.1) {
                let bucket_pref= TableFile::table_offset(bucket_number);
                if let Some(mut bucket) = self.buckets.write().unwrap().get_mut(bucket_number) {
                    let mut page = self.table_file.read_page(bucket_pref.this_page())?.unwrap_or(Self::invalid_offsets_page(bucket_pref.this_page()));
                    if let Some (ref slots) = bucket.slots {
                        let link = if slots.len() > 0 {
                            let slots = Link::from_slots(slots.as_slice());
                            self.link_file.append_link(Link::deserialize(slots.as_slice()))?
                        } else {
                            PRef::invalid()
                        };
                        bucket.stored = link;
                        page.write_pref(bucket_pref.in_page_pos(), link);
                        self.table_file.update_page(page)?;
                    }
                }
            }
        }
        self.dirty.clear();
        self.link_file.flush()?;
        self.table_file.flush()?;
        Ok(())
    }

    pub fn invalid_offsets_page(pos: PRef) -> Page {
        let mut page = Page::new_table_page(pos);
        if pos.as_u64() == 0 {
            for o in 0 .. BUCKETS_FIRST_PAGE {
                page.write_pref(FIRST_PAGE_HEAD + o*BUCKET_SIZE, PRef::invalid());
            }
        }
        else {
            for o in 0 .. BUCKETS_PER_PAGE {
                page.write_pref(o*BUCKET_SIZE, PRef::invalid());
            }
        }
        page
    }

    pub fn slots<'a>(&'a self) -> impl Iterator<Item=Vec<(u32, PRef)>> +'a {
        BucketIterator{file: self, n:0}
    }

    pub fn buckets<'a>(&'a self) -> impl Iterator<Item=PRef> +'a {
        self.table_file.iter()
    }

    pub fn data_envelopes<'a>(&'a self) -> EnvelopeIterator<'a> {
        self.data_file.envelopes()
    }

    pub fn link_envelopes<'a>(&'a self) -> impl Iterator<Item=(PRef, Envelope)> +'a {
        self.link_file.envelopes()
    }

    pub fn append_data (&mut self, key: &[u8], data: &[u8]) -> Result<PRef, Error> {
        self.data_file.append_data(key, data)
    }

    pub fn append_referred (&mut self, data: &[u8]) -> Result<PRef, Error> {
        self.data_file.append_referred(data)
    }

    pub fn get_envelope(&self, pref: PRef) -> Result<Envelope, Error> {
        self.data_file.get_envelope(pref)
    }

    pub fn put (&mut self, key: &[u8], data_offset: PRef) -> Result<(), Error>{
        let hash = self.hash(key);
        let bucket = self.bucket_for_hash(hash);

        self.remove_duplicate(key, hash, bucket)?;

        self.store_to_bucket(bucket, hash, data_offset)?;

        if self.forget == 0 {
            if hash % self.bucket_fill_target as u32 == 0 && self.step < (1 << 31) {
                if self.step < (1 << self.log_mod) {
                    let step = self.step;
                    self.rehash_bucket(step)?;
                }

                self.step += 1;
                if self.step > (1 << (self.log_mod + 1)) {
                    self.log_mod += 1;
                    self.step = 0;
                }

                self.buckets.write().unwrap().push(Bucket::default());
                self.dirty.append();
            }
        }
        else {
            self.forget -= 1;
        }
        Ok(())
    }

    pub fn forget(&mut self, key: &[u8]) -> Result<(), Error> {
        let hash = self.hash(key);
        let bucket = self.bucket_for_hash(hash);
        if self.remove_duplicate(key, hash, bucket)? {
            self.forget += 1;
        }
        Ok(())
    }

    fn remove_duplicate(&mut self, key: &[u8], hash: u32, bucket_number: usize) -> Result<bool, Error> {
        let mut remove = None;
        self.resolve_bucket(bucket_number)?;
        if let Some(bucket) = self.buckets.write().unwrap().get_mut(bucket_number) {
            if let Some(ref mut slots) = bucket.slots {
                for (n, (_, pref)) in slots.iter().enumerate()
                    .filter(|s| (s.1).0 == hash) {
                    let envelope = self.data_file.get_envelope(*pref)?;
                    if let Payload::Indexed(indexed) = Payload::deserialize(envelope.payload())? {
                        if indexed.key == key {
                            remove = Some(n);
                            break;
                        }
                    }
                }
                if let Some(r) = remove {
                    slots.remove(r);
                }
            }
        }
        if remove.is_some() {
            self.modify_bucket(bucket_number)?;
        }
        Ok(remove.is_some())
    }

    fn store_to_bucket(&mut self, bucket: usize, hash: u32, pref: PRef) -> Result<(), Error> {
        self.resolve_bucket(bucket)?;
        if let Some(bucket) = self.buckets.write().unwrap().get_mut(bucket as usize) {
            if let Some(ref mut slots) = bucket.slots {
                slots.push((hash, pref));
            }
            else {
                bucket.slots = Some(vec!((hash, pref)));
            }
        } else {
            return Err(Error::Corrupted(format!("memtable does not have the bucket {}", bucket).to_string()))
        }
        self.modify_bucket(bucket)?;
        Ok(())
    }

    fn rehash_bucket(&mut self, bucket: usize) -> Result<(), Error> {
        let mut rewrite = false;
        let mut new_bucket_store = Bucket::default();
        let mut moves = HashMap::new();
        self.resolve_bucket(bucket)?;
        if let Some(b) = self.buckets.read().unwrap().get(bucket as usize) {
            if let Some(ref slots) = b.slots {
                for (hash, pref) in slots {
                    let new_bucket = (hash & (!0u32 >> (32 - self.log_mod - 1))) as usize; // hash % 2^(log_mod + 1)
                    if new_bucket != bucket {
                        moves.entry(new_bucket).or_insert(Vec::new()).push((*hash, *pref));
                        rewrite = true;
                    } else {
                        if let Some(ref mut slots) = new_bucket_store.slots {
                            slots.push((*hash, *pref));
                        }
                        else {
                            new_bucket_store.slots = Some(vec!((*hash, *pref)));
                        }
                    }
                }
            }
        }
        else {
            return Err(Error::Corrupted(format!("does not have bucket {} for rehash", bucket)));
        }
        if rewrite {
            for (bucket, added) in moves {
                for (hash, pref) in added {
                    self.store_to_bucket(bucket, hash, pref)?;
                }
            }
            self.buckets.write().unwrap()[bucket] = new_bucket_store;
            self.modify_bucket(bucket)?;
        }
        Ok(())
    }

    fn modify_bucket(&mut self, bucket: usize) -> Result<(), Error> {
        self.dirty.set(bucket);
        let bucket_page = if bucket < BUCKETS_FIRST_PAGE { 
            PRef::from(0)
        } else {
            PRef::from(((bucket - BUCKETS_FIRST_PAGE)/BUCKETS_PER_PAGE + 1) as u64 * PAGE_SIZE as u64)
        };
        self.log_file.log_page(bucket_page, &self.table_file)
    }

    pub fn may_have_key(&self, key: &[u8]) -> Result<bool, Error> {
        let hash = self.hash(key);
        let bucket_number = self.bucket_for_hash(hash);
        self.resolve_bucket(bucket_number)?;
        if let Some(bucket) = self.buckets.read().unwrap().get(bucket_number) {
            if let Some (ref slots) = bucket.slots {
                if slots.iter().any(|(h, _)| *h == hash) {
                    return Ok(true);
                }
            }
        } else {
            return Err(Error::Corrupted(format!("bucket {} should exist", bucket_number)));
        }
        Ok(false)
    }

    // get the data last associated with the key
    pub fn get(&self, key: &[u8]) -> Result<Option<(PRef, Vec<u8>)>, Error> {
        let hash = self.hash(key);
        let bucket_number = self.bucket_for_hash(hash);
        self.resolve_bucket(bucket_number)?;
        if let Some(ref bucket) = self.buckets.read().unwrap().get(bucket_number) {
            if let Some(ref slots) = bucket.slots {
                for (h, data) in slots {
                    if *h == hash {
                        let envelope = self.data_file.get_envelope(*data)?;
                        if let Payload::Indexed(indexed) = Payload::deserialize(envelope.payload())? {
                            if indexed.key == key {
                                return Ok(Some((*data, indexed.data.data.to_vec())));
                            }
                        } else {
                            return Err(Error::Corrupted("pref should point to indexed data".to_string()));
                        }
                    }
                }
            }
        }
        else {
            return Err(Error::Corrupted(format!("bucket {} should exist", bucket_number)));
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
        siphash24::Hash::hash_to_u64_with_keys(self.sip0, self.sip1, key) as u32
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
    n: usize
}

impl<'a> Iterator for BucketIterator<'a> {
    type Item = Vec<(u32, PRef)>;

    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        self.file.resolve_bucket(self.n).unwrap();
        if let Some(bucket) = self.file.buckets.read().unwrap().get(self.n) {
            self.n += 1;
            if let Some(ref slots) = bucket.slots {
                return Some(slots.clone());
            }
            else {
                return Some(vec!());
            }
        }
        None
    }
}

struct DirtyIterator<'b> {
    bits: &'b Dirty,
    pos: usize
}

impl<'b> DirtyIterator<'b> {
    pub fn new(bits: &'b Dirty) -> DirtyIterator<'b> {
        DirtyIterator {bits, pos: 0}
    }
}

impl<'b> Iterator for DirtyIterator<'b> {
    type Item = bool;

    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        if self.pos < self.bits.used {
            let pos = self.pos;
            self.pos += 1;
            return Some(self.bits.get(pos));
        }
        return None;
    }
}

#[derive(Clone, Default)]
pub struct Bucket {
    stored: PRef,
    slots: Option<Vec<(u32, PRef)>>
}


#[cfg(test)]
mod test {
    extern crate rand;

    use transient::Transient;

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
        let mut db = Transient::new_db("first", 1, 1).unwrap();

        let mut rng = thread_rng();
        let mut key = [0x0u8;32];
        let mut data = [0x0u8;40];
        let mut check = HashMap::new();

        for _ in 0 .. 10000 {
            rng.fill_bytes(&mut key);
            rng.fill_bytes(&mut data);
            let o = db.put_keyed(&key, &data).unwrap();
            check.insert(key, (o, data.to_vec()));
        }
        db.batch().unwrap();

        for (k, (o, data)) in &check {
            assert_eq!(db.get_keyed(&k[..]).unwrap().unwrap(), (*o, data.clone()));
        }

        for (k, (_, _)) in &check {
            db.forget(k).unwrap();
            assert!(db.get_keyed(&k[..]).unwrap().is_none());
        }

        db.shutdown();
    }
}

