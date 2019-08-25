[![Safety Dance](https://img.shields.io/badge/unsafe-forbidden-success.svg)](https://github.com/rust-secure-code/safety-dance/)

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
````$Rust
use hammersbald::{
        persistent,
        HammersbaldAPI
        };

// read cache size in 4k pages
const CACHED_PAGES:usize = 100;
// average re-use of a hash table entry
const BUCKET_FILL_TARGET:usize = 2;

let mut db = persistent("dbname", CACHED_PAGES, BUCKET_FILL_TARGET).unwrap();

db.put_keyed(b"some key", b"some data").unwrap();

db.batch().unwrap();

if let Some((pos, data)) = db.get_keyed(b"some key").unwrap() {
    assert_eq!(data, b"some data".to_vec());
}
else {
    panic!("can not find inserted data");
}


db.shutdown();
````
### Optional Bitcoin API
A bitcoin adaptor is available if compiled with the bitcoin_support future.
Example use:
````$Rust
        // create a transient hammersbald
        let db = transient(1).unwrap();
        // promote to a bitcoin adapter
        let mut bdb = BitcoinAdaptor::new(db);

        // example transaction
        let tx = decode::<Transaction> (hex::decode("02000000000101ed30ca30ee83f13579da294e15c9d339b35d33c5e76d2fda68990107d30ff00700000000006db7b08002360b0000000000001600148154619cb0e7513fcdb1eb90cc9f86f3793b9d8ec382ff000000000022002027a5000c7917f785d8fc6e5a55adfca8717ecb973ebb7743849ff956d896a7ed04004730440220503890e657773607fb05c9ef4c4e73b0ab847497ee67b3b8cefb3688a73333180220066db0ca943a5932f309ac9d4f191300711a5fc206d7c3babd85f025eac30bca01473044022055f05c3072dfd389104af1f5ccd56fb5433efc602694f1f384aab703c77ac78002203c1133981d66dc48183e72a19cc0974b93002d35ad7d6ee4278d46b4e96f871a0147522102989711912d88acf5a4a18081104f99c2f8680a7de23f829f28db31fdb45b7a7a2102f0406fa1b49a9bb10c191fd83e2359867ecdace5ea990ce63d11478ed5877f1852ae81534220").unwrap()).unwrap();

        // store the transaction without associating a key
        let txref = bdb.put_encodable(&tx).unwrap();
        // retrieve by direct reference
        let (key, tx2) = bdb.get_decodable::<Transaction>(txref).unwrap();
        assert_eq!(tx, tx2);
        assert_eq!(key, tx.bitcoin_hash()[..].to_vec());

        // store the transaction with its hash as key
        let txref2 = bdb.put_hash_keyed(&tx).unwrap();
        // retrieve by hash
        if let Some((pref, tx3)) = bdb.get_hash_keyed::<Transaction>(&tx.bitcoin_hash()).unwrap() {
            assert_eq!(pref, txref2);
            assert_eq!(tx3, tx);
        }
        else {
            panic!("can not find tx");
        }
        bdb.batch().unwrap();
````

## Implementation
The persistent storage should be opened by only one process. 

The store is a persistent hash map using [Linear Hashing](https://en.wikipedia.org/wiki/Linear_hashing).

### Limits
The data storage size is limited to 2^48 (256TiB) due to the use of 6 byte persistent
pointers. A data element can not exceed 2^24 (16MiB) in length. Key length is limited to 255 bytes. 

## Release Notes
2.3.0 all bitcoin objects use CBOR serialization

2.2.0 add storage of CBOR serializable objects to bitcoin_adaptor

2.1.0 upgrade to rust-bitcoin 0.20, use bitcoin_hashes instead of siphasher

2.0.0 file format change, some savings

1.7.0 group subsequent reads and writes, upgrade to rust-bitcoin 0.18

1.6.0 upgrade to rust-bitcoin 0.17

1.5.1 add API may_have_key

1.5 upgrade to bitcoin 0.16

