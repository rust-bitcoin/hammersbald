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
//! # Statistics for a Hammersbald db
//!
//!
use api::Hammersbald;
use format::Payload;

use bitcoin_hashes::siphash24;

use std::collections::{HashMap, HashSet};

/// print some statistics on a db
#[allow(unused)]
fn stats(db: &Hammersbald) {
    let (step, log_mod, blen, tlen, dlen, llen, sip0, sip1) = db.params();
    println!("File sizes: table: {}, data: {}, links: {}\nHash table: buckets: {}, log_mod: {}, step: {}", tlen, dlen, llen, blen, log_mod, step);

    let mut pointer = HashSet::new();
    for bucket in db.buckets() {
        if bucket.is_valid() {
            pointer.insert(bucket);
        }
    }

    let mut n_links = 0;
    for (pos, envelope) in db.link_envelopes() {
        match Payload::deserialize(envelope.payload()).unwrap() {
            Payload::Link(_) => {
                n_links += 1;
                pointer.remove (&pos);
            },
            _ => panic!("Unexpected payload type link at {}", pos)
        }
    }
    if !pointer.is_empty() {
        panic!("{} roots point to non-existent links", pointer.len());
    }


    let mut roots = HashMap::new();
    let mut ndata = 0;
    let mut used_buckets = 0;
    for slots in db.slots() {
        ndata += slots.len();
        if slots.len() > 0 {
            used_buckets += 1;
        }
        for slot in slots.iter() {
            roots.entry(slot.1).or_insert(Vec::new()).push(slot.0);
        }
    }
    println!("Used buckets: {} {:.1} % avg. slots per bucket: {:.1}", used_buckets, 100.0*(used_buckets as f32/blen as f32), ndata as f32/used_buckets as f32);
    println!("Data: indexed: {}, hash collisions {:.2} %", ndata, (1.0-(roots.len() as f32)/(ndata as f32))*100.0);

    let mut indexed_garbage = 0;
    let mut referred_garbage = 0;
    let mut referred = 0;
    for (pos, envelope) in db.data_envelopes() {
        match Payload::deserialize(envelope.payload()).unwrap() {
            Payload::Indexed(indexed) => {
                if let Some(root) = roots.remove(&pos) {
                    let h = hash(indexed.key, sip0, sip1);
                    if root.iter().any(|hash| *hash == h) == false {
                        panic!("ERROR root {} points data with different key hash", pos);
                    }
                } else {
                    indexed_garbage += 1;
                }
            },
            Payload::Referred(data) => {
                referred += 1;
            },
            _ => panic!("Unexpected payload type in data at {}", pos)
        }
    }
    if !roots.is_empty() {
        panic!("ERROR {} roots point to non-existent data", roots.len());
    }
    println!("Referred: {}", referred);
    println!("Garbage: indexed: {}, referred: {}, links: {}", indexed_garbage, referred_garbage, n_links - used_buckets);
}


fn hash (key: &[u8], sip0: u64, sip1: u64) -> u32 {
    siphash24::Hash::hash_to_u64_with_keys(sip0, sip1, key) as u32
}