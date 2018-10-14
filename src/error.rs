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
use bitcoin::util;

use std::convert;
use std::error::Error;
use std::fmt;
use std::io;
use std::sync;

/// Errors returned by this library
pub enum BCDBError {
    /// offset is invalid (> 2^48)
    InvalidOffset,
    /// corrupted data
    Corrupted(String),
    /// Data does not fit into the block
    DoesNotFit,
    /// wrapped IO error
    IO(io::Error),
    /// Wrapped bitcoin util error
    #[cfg(feature="bitcoin_support")]
    Util(util::Error),
    /// Lock poisoned
    Poisoned(String)
}

impl Error for BCDBError {
    fn description(&self) -> &str {
        match *self {
            BCDBError::InvalidOffset => "invalid offset",
            BCDBError::DoesNotFit => "data does not fit into the page",
            BCDBError::Corrupted (ref s) => s.as_str(),
            BCDBError::IO(_) => "IO Error",
            #[cfg(feature="bitcoin_support")]
            BCDBError::Util(_) => "Bitcoin Util Error",
            BCDBError::Poisoned(ref s) => s.as_str()
        }
    }

    fn cause(&self) -> Option<&Error> {
        match *self {
            BCDBError::InvalidOffset => None,
            BCDBError::DoesNotFit => None,
            BCDBError::Corrupted (_) => None,
            BCDBError::IO(ref e) => Some(e),
            #[cfg(feature="bitcoin_support")]
            BCDBError::Util(ref e) => Some(e),
            BCDBError::Poisoned(_) => None
        }
    }
}

impl fmt::Display for BCDBError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "BCSError: {} cause: {:?}", self.description(), self.cause())
    }
}

impl fmt::Debug for BCDBError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        (self as &fmt::Display).fmt(f)
    }
}

impl convert::From<io::Error> for BCDBError {
    fn from(err: io::Error) -> BCDBError {
        BCDBError::IO(err)
    }
}

impl<T> convert::From<sync::PoisonError<T>> for BCDBError {
    fn from(err: sync::PoisonError<T>) -> BCDBError {
        BCDBError::Poisoned(err.to_string())
    }
}

/// an iterator that may fail on next() with a BCDBError
pub trait MayFailIterator<I> : IntoIterator<Item=I> {
    /// get next item.
    /// Error is returned only on data corruption.
    fn next(&mut self) -> Result<Option<I>, BCDBError>;
}