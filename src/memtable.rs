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
use table::TableFile;
use linkfile::LinkFile;

pub struct Table {
    step: u32,
    log_mod: u32,
    sip0: u64,
    sip1: u64,
    buckets: Vec<Option<Bucket>>
}

impl Table {
    pub fn new (log_mod: u32, step: u32, sip0: u64, sip1: u64) -> Table {
        assert!(log_mod < 32);
        assert!(step < (1<<log_mod));
        Table{log_mod, step, sip0, sip1, buckets: Vec::with_capacity(1<<log_mod as usize)}
    }

    pub fn load (table: TableFile, link: LinkFile) {

    }
}

#[derive(Clone)]
pub struct Bucket {
    hashes: Vec<u32>,
    offsets: Vec<u64>
}



