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
//! # Error type
//!
//!
#[cfg(feature="bitcoin_support")]
use bitcoin::consensus::encode;

use std::convert;
use std::error::Error;
use std::fmt;
use std::io;
use std::sync;

/// Errors returned by this library
pub enum HammersbaldError {
    /// pref is invalid (> 2^48)
    InvalidOffset,
    /// corrupted data
    Corrupted(String),
    /// key too long
    KeyTooLong,
    /// wrapped IO error
    IO(io::Error),
    /// Wrapped bitcoin util error
    #[cfg(feature="bitcoin_support")]
    BitcoinSerialize(encode::Error),
    /// Lock poisoned
    Poisoned(String),
    /// Queue error
    Queue(String)
}

impl Error for HammersbaldError {
    fn description(&self) -> &str {
        match *self {
            HammersbaldError::InvalidOffset => "invalid pref",
            HammersbaldError::KeyTooLong => "key too long",
            HammersbaldError::Corrupted (ref s) => s.as_str(),
            HammersbaldError::IO(_) => "IO Error",
            #[cfg(feature="bitcoin_support")]
            HammersbaldError::BitcoinSerialize(_) => "Bitcoin Serialize Error",
            HammersbaldError::Poisoned(ref s) => s.as_str(),
            HammersbaldError::Queue(ref s) => s.as_str()
        }
    }

    fn cause(&self) -> Option<&Error> {
        match *self {
            HammersbaldError::InvalidOffset => None,
            HammersbaldError::KeyTooLong => None,
            HammersbaldError::Corrupted (_) => None,
            HammersbaldError::IO(ref e) => Some(e),
            #[cfg(feature="bitcoin_support")]
            HammersbaldError::BitcoinSerialize(ref e) => Some(e),
            HammersbaldError::Poisoned(_) => None,
            HammersbaldError::Queue(_) => None
        }
    }
}

impl fmt::Display for HammersbaldError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Hammersbald error: {} cause: {:?}", self.description(), self.cause())
    }
}

impl fmt::Debug for HammersbaldError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        (self as &fmt::Display).fmt(f)
    }
}

impl convert::From<io::Error> for HammersbaldError {
    fn from(err: io::Error) -> HammersbaldError {
        HammersbaldError::IO(err)
    }
}

impl convert::From<HammersbaldError> for io::Error {
    fn from(_: HammersbaldError) -> io::Error {
        io::Error::from(io::ErrorKind::UnexpectedEof)
    }
}

impl<T> convert::From<sync::PoisonError<T>> for HammersbaldError {
    fn from(err: sync::PoisonError<T>) -> HammersbaldError {
        HammersbaldError::Poisoned(err.to_string())
    }
}

impl<T> convert::From<sync::mpsc::SendError<T>> for HammersbaldError {
    fn from(err: sync::mpsc::SendError<T>) -> HammersbaldError {
        HammersbaldError::Queue(err.to_string())
    }
}
