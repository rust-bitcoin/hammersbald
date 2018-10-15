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
//! # Blockchain store
//!
//! A very fast persistent blockchain store and a convenience library for blockchain in-memory cache.
//!

#![deny(non_upper_case_globals)]
#![deny(non_camel_case_types)]
#![deny(non_snake_case)]
#![deny(unused_mut)]
#![deny(missing_docs)]
#![deny(unused_must_use)]

#[macro_use]
extern crate log;
#[cfg(feature="bitcoin_support")]
extern crate bitcoin;
extern crate siphasher;
extern crate rand;
extern crate byteorder;

mod page;
mod logfile;
mod table;
mod cache;
mod rolled;
mod asyncfile;
mod linkfile;
mod memtable;
pub mod bcdb;
pub mod datafile;
pub mod error;
pub mod offset;
pub mod inmemory;
pub mod infile;
#[cfg(feature="bitcoin_support")]
pub mod bitcoin_adapter;