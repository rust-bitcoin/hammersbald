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
//! # Reference to persistent data
//! allows reference of a data space of 2^48

use page::PAGE_SIZE;

use std::cmp::Ordering;
use std::fmt;
use std::ops;

const INVALID: u64 = 0xffffffffffff;

#[derive(Eq, PartialEq, Hash, Copy, Clone, Debug)]
/// Pointer to persistent data. Limited to 2^48
pub struct PRef(u64);

impl Default for PRef {
    fn default() -> Self {
        PRef(INVALID)
    }
}

impl Ord for PRef {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}

impl PartialOrd for PRef {
    fn partial_cmp(&self, other: &PRef) -> Option<Ordering> {
        self.0.partial_cmp(&other.0)
    }
}

impl From<u64> for PRef {
    fn from(n: u64) -> Self {
        PRef(n)
    }
}

impl fmt::Display for PRef {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}", self.0)
    }
}

impl ops::Add<u64> for PRef {
    type Output = PRef;

    fn add(self, rhs: u64) -> <Self as ops::Add<u64>>::Output {
        PRef::from(self.as_u64() + rhs)
    }
}

impl ops::AddAssign<u64> for PRef {
    fn add_assign(&mut self, rhs: u64) {
        #[cfg(debug_assertions)]
        {
            if self.0 + rhs >= INVALID {
                panic!("pref would become invalid through addition");
            }
        }
        self.0 += rhs;
    }
}

impl ops::Sub<u64> for PRef {
    type Output = PRef;

    fn sub(self, rhs: u64) -> <Self as ops::Sub<u64>>::Output {
        PRef::from(self.as_u64() - rhs)
    }
}

impl ops::SubAssign<u64> for PRef {
    fn sub_assign(&mut self, rhs: u64) {
        #[cfg(debug_assertions)]
        {
            if rhs > self.0 {
                panic!("pref would become invalid through subtraction");
            }
        }
        self.0 -= rhs;
    }
}

impl PRef {
    /// construct an invalid pref
    pub fn invalid () -> PRef {
        PRef(INVALID)
    }

    /// is this a valid pref?
    pub fn is_valid (&self) -> bool {
        self.0 < INVALID
    }

    /// convert to a number
    pub fn as_u64 (&self) -> u64 {
        return self.0;
    }

    /// pref of the page of this pref
    pub fn this_page(&self) -> PRef {
        PRef::from((self.0/ PAGE_SIZE as u64)* PAGE_SIZE as u64)
    }

    /// compute page number of an pref
    pub fn page_number(&self) -> u64 {
        self.0/PAGE_SIZE as u64
    }

    /// position within the pref's page
    pub fn in_page_pos(&self) -> usize {
        (self.0 % PAGE_SIZE as u64) as usize
    }

    /// number of pages from this until an other
    pub fn pages_until(&self, other: PRef) -> usize {
        ((other.0 - self.0)/PAGE_SIZE as u64) as usize
    }

    /// next page
    pub fn next_page (&self) -> PRef {
        PRef(self.0 + PAGE_SIZE as u64)
    }

    /// previous page
    pub fn prev_page (&self) -> PRef {
        PRef(self.0 - PAGE_SIZE as u64)
    }

    /// add n pages
    pub fn add_pages(&self, n: usize) -> PRef {
        PRef(self.0 + n as u64 *PAGE_SIZE as u64)
    }
}
