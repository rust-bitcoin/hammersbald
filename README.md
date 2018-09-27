# Fast Blockchain store in Rust
A very fast persistent blockchain store.

## Motivation
Generic databases and key-value stores offer much more functionality 
than needed to store and process a blockchain. Superflous functionality (for a blockchain)
comes at a high cost in speed. 

## Status
It works, but still moving. Not yet released.


## API
This library in contrast only implements the bare mininum of operations:

* insert a header
* insert a block, that is a header enriched with transactions and application specific data
* fetch header or block and individual transactions or application data by their id

The only key available is the identity of headers, blocks and transactions. 
Keys are not sorted and can not be iterated over. 

There is no delete operation. An insert with a key renders a previous insert with same key
inaccessible. Since header and block have the same id, only the block will be accessible 
if inserted after the header. 
 
Inserts must be grouped into batches. All inserts of a batch will be stored 
or none of them, in case the process dies while inserting in a batch.
Data inserted in a batch may be fetched before closing the batch.

## Imlementation
The persistent storage should be opened by only one process. 

The store is a peristent hash map using [Linear Hashing](https://en.wikipedia.org/wiki/Linear_hashing).

The data storage size is limited to 2^48 (256 TiB) due to the use of 6 byte persistent
pointers. A data element can not exceed 2^24 (16MiB) in length. 






