# Hammersbald
A fast embedded blockchain database.

## Motivation
Generic databases and key-value stores offer much more functionality 
than needed to store and process a blockchain. Superfluous functionality (for a blockchain)
comes at a high cost in speed. 

## Name
Hammersbald is a German slang for "Haben wir es bald?" close to in english "Will we have it soon?". 
A term often used to express impatience. Hammersbald is the blockchain database for the impatient.
Hammersbald sounds also like the name of some ancient northern god.

## Status
It works.

## Design
See [Hammersbald](https://medium.com/@tamas.blummer/hammersbald-7c0bda14da1e) on medium

## API
This library implements the bare minimum of operations:

* insert data with a key
* find data with a key
* insert some data that can be referred to by an other data but has no key.
* find some data with known offset.
* start batch, that also ends current batch

There is no delete operation. An insert with a key renders a previous insert with same key inaccessible. 
Keys are not sorted and can not be iterated. 
 
Inserts must be grouped into batches. All inserts of a batch will be stored 
or none of them, in case the process dies while inserting in a batch.

Data inserted in a batch may be fetched before closing the batch.

Simplest use:
````
use hammersbald::{
        persistent,
        HammersbaldAPI
        };

// read cache size in 4k pages
const CACHED_PAGES:usize = 100;
// average re-use of a hash table entry
const BUCKET_FILL_TARGET:usize = 2;

let mut db = persistent("dbname", CACHED_PAGES, BUCKET_FILL_TARGET).unwrap();

db.put(b"some key", b"some data").unwrap();

db.batch().unwrap();

if let Some((pos, data)) = db.get(b"some key").unwrap() {
    assert_eq!(data, b"some data".to_vec());
}
else {
    panic!("can not find inserted data");
}


db.shutdown();
````

## Implementation
The persistent storage should be opened by only one process. 

The store is a persistent hash map using [Linear Hashing](https://en.wikipedia.org/wiki/Linear_hashing).

### Limits
The data storage size is limited to 2^48 (256TiB) due to the use of 6 byte persistent
pointers. A data element can not exceed 2^24 (16MiB) in length. Key length is limited to 255 bytes. 

