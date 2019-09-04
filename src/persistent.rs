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
//! # persistent store
//!
//! Implements persistent store

use api::{Hammersbald, HammersbaldAPI};
use asyncfile::AsyncFile;
use cachedfile::CachedFile;
use datafile::DataFile;
use error::Error;
use logfile::LogFile;
use rolledfile::RolledFile;
use tablefile::TableFile;

const TABLE_CHUNK_SIZE: u64 = 1024 * 1024 * 1024;
const DATA_CHUNK_SIZE: u64 = 1024 * 1024 * 1024;
const LOG_CHUNK_SIZE: u64 = 1024 * 1024 * 1024;

/// Implements persistent storage
pub struct Persistent {}

impl Persistent {
    /// create a new db
    pub fn new_db(name: &str, cached_data_pages: usize, bucket_fill_target: usize) -> Result<Box<dyn HammersbaldAPI>, Error> {
        let data = DataFile::new(
            Box::new(CachedFile::new(
                Box::new(AsyncFile::new(
                    Box::new(RolledFile::new(
                        name, "bc", true, DATA_CHUNK_SIZE)?))?), cached_data_pages)?))?;

        let link = DataFile::new(
            Box::new(CachedFile::new(
                Box::new(AsyncFile::new(
                    Box::new(RolledFile::new(
                        name, "bl", true, DATA_CHUNK_SIZE)?))?), cached_data_pages)?))?;

        let log = LogFile::new(
            Box::new(AsyncFile::new(
                Box::new(RolledFile::new(name, "lg", true, LOG_CHUNK_SIZE)?))?));

        let table = TableFile::new(
            Box::new(CachedFile::new(
            Box::new(RolledFile::new(name, "tb", false, TABLE_CHUNK_SIZE)?), cached_data_pages)?))?;

        Ok(Box::new(Hammersbald::new(log, table, data, link, bucket_fill_target)?))
    }
}
