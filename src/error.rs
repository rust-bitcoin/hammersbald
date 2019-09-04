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
//! # Error type
//!
//!
#[cfg(feature="bitcoin_support")]
use bitcoin::consensus::encode;

use std::convert;
use std::fmt;
use std::io;
use std::sync;

/// Errors returned by this library
pub enum Error {
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

impl std::error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::InvalidOffset => "invalid pref",
            Error::KeyTooLong => "key too long",
            Error::Corrupted (ref s) => s.as_str(),
            Error::IO(_) => "IO Error",
            #[cfg(feature="bitcoin_support")]
            Error::BitcoinSerialize(_) => "Bitcoin Serialize Error",
            Error::Poisoned(ref s) => s.as_str(),
            Error::Queue(ref s) => s.as_str()
        }
    }

    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match *self {
            Error::InvalidOffset => None,
            Error::KeyTooLong => None,
            Error::Corrupted (_) => None,
            Error::IO(ref e) => Some(e),
            #[cfg(feature="bitcoin_support")]
            Error::BitcoinSerialize(ref e) => Some(e),
            Error::Poisoned(_) => None,
            Error::Queue(_) => None
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use std::error::Error;
        write!(f, "Hammersbald error: {} cause: {:?}", self.description(), self.source())
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        (self as &dyn fmt::Display).fmt(f)
    }
}

impl convert::From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::IO(err)
    }
}

impl convert::From<Error> for io::Error {
    fn from(_: Error) -> io::Error {
        io::Error::from(io::ErrorKind::UnexpectedEof)
    }
}

impl<T> convert::From<sync::PoisonError<T>> for Error {
    fn from(err: sync::PoisonError<T>) -> Error {
        Error::Poisoned(err.to_string())
    }
}

impl<T> convert::From<sync::mpsc::SendError<T>> for Error {
    fn from(err: sync::mpsc::SendError<T>) -> Error {
        Error::Queue(err.to_string())
    }
}
