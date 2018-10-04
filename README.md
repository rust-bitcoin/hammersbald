# Fast Blockchain store in Rust
A very fast persistent blockchain store.

## Motivation
Generic databases and key-value stores offer much more functionality 
than needed to store and process a blockchain. Superfluous functionality (for a blockchain)
comes at a high cost in speed. 

## Status
Work in progress. Not yet released, do not send PRs yet.


## API
This library in contrast only implements the bare minimum of operations:

* insert data with some keys
* find data with a key
* insert some data that can be referred to by an other data but has no key. Returns an offset.
* find some data with known offset.
* start batch, that also ends current batch

There is no delete operation. An insert with a key renders a previous insert with same key inaccessible. 
A call to dedup completely removes reference to previous data.

Keys are not sorted and can not be iterated. 
 
Inserts must be grouped into batches. All inserts of a batch will be stored 
or none of them, in case the process dies while inserting in a batch.

Data inserted in a batch may be fetched before closing the batch.

Only one process should open the same db.

### Optional bitcoin_support feature
* insert a header
* insert a block, that is a header enriched with transactions and application specific data
* fetch header or block and individual transactions or application data by their id

Since header and block have the same id, only the block will be accessible if inserted after the header. 

## Implementation
The persistent storage should be opened by only one process. 

The store is a persistent hash map using [Linear Hashing](https://en.wikipedia.org/wiki/Linear_hashing).

The data storage size is limited to 2^44 (16 TiB) due to the use of 5.5 byte persistent
pointers. A data element can not exceed 2^24 (16MiB) in length. 

Key length is limited to 255 bytes. Content can be associated to most 255 keys, 
duplicate storage of the same data can help to overcome this.





