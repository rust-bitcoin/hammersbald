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
use std::convert;
use std::error::Error;
use std::fmt;
use std::io;

/// Errors returned by this library
pub enum BCSError {
    /// offset is invalid (> 2^48)
    InvalidOffset,
    /// corrupted data
    Corrupted,
    /// Data does not fit into the block
    DoesNotFit,
    /// wrapped IO error
    IO(io::Error)
}

impl Error for BCSError {
    fn description(&self) -> &str {
        match *self {
            BCSError::InvalidOffset => "invalid offset",
            BCSError::DoesNotFit => "data does not fit into the block",
            BCSError::Corrupted => "corrupted",
            BCSError::IO(_) => "IO Error",
        }
    }

    fn cause(&self) -> Option<&Error> {
        match *self {
            BCSError::InvalidOffset => None,
            BCSError::DoesNotFit => None,
            BCSError::Corrupted => None,
            BCSError::IO(ref e) => Some(e)
        }
    }
}

impl fmt::Display for BCSError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "BCSError: {} cause: {:?}", self.description(), self.cause())
    }
}

impl fmt::Debug for BCSError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        (self as &fmt::Display).fmt(f)
    }
}

impl convert::From<io::Error> for BCSError {
    fn from(err: io::Error) -> BCSError {
        BCSError::IO(err)
    }
}
