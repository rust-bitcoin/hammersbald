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
//! # Hammersbald Blockchain store
//!
//! A very fast persistent blockchain store
//!

#![deny(non_upper_case_globals)]
#![deny(non_camel_case_types)]
#![deny(non_snake_case)]
#![deny(unused_mut)]
#![deny(missing_docs)]
#![deny(unused_must_use)]

#[cfg(feature="bitcoin_support")]
extern crate bitcoin;
extern crate siphasher;
extern crate rand;
extern crate byteorder;
extern crate lru_cache;

mod page;
mod pagedfile;
mod logfile;
mod tablefile;
mod cachedfile;
mod singlefile;
mod rolledfile;
mod asyncfile;
mod memtable;
pub mod format;
pub mod api;
pub mod datafile;
pub mod error;
pub mod pref;
pub mod transient;
pub mod persistent;
#[cfg(feature="bitcoin_support")]
pub mod bitcoin_support;