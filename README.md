# Fast Blockchain store in Rust
A very fast persistent blockchain store and a convenience library for blockchain in-memory cache.

## Motivation
Generic databases and key-value stores offer much more functionality 
than needed to store and process a blockchain. Superflous functionality (for a blockchain)
comes at a high cost in speed. 

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

Both reads and writes are non-blocking. Any number of simultaneous 
reads or a single write operation is permitted. 
Inserts must be grouped into batches. All inserts of a batch will be stored 
or none of them, in case the process dies while inserting in a batch.
Data inserted in a batch may be fetched before closing the batch.

A convenience library adds an in-memory cache and methods to:

* iterate backward from any header following the chain
* iterate forward from genesis header to the tip header with most work
* compute the list of headers orphaned if a header is added and becomes the new tip

## Imlementation
The persistent storage should be opened by only one process. 

The store is a peristent hash map using [Linear Hashing](https://en.wikipedia.org/wiki/Linear_hashing).

The data storage size is limited to 2^48 (256 TiB) due to the use of 5 byte persistent
pointers. A data element can not exceed 2^24 (16MiB) in length. 
Writes and reads are performed in multiples of 4096 bytes.

The persistent store uses up to three files:
* the persistent hash table (.tbl)
* the data that is iterable on its own and can be used to rebuild the hash table (.dat)
* a temporary log file that helps to unwind a partial insert batch at re-open (.log)

Numbers stored in big endian.

### Blocks

The block is the unit of read and expansion for the data and key file. A block consists of
a payload and a used length less or equal to 4094 

+----+-------------------------------+
|    | payload                       |
+----+-------------------------------+
|u16 | used length                   |
+----+-------------------------------+


### Data file

The data file is strictly append only. Anything written stays there the only allowed operations are:
* append
* truncate to last known correct size

The data file starts with a magic number in two bytes spelling BCDA (blockchain data) in hex.
Thereafter any number or data elements stored prefixed with a length and type.

<pre>

+------------- block       -----------------+
|                                           |
|	+----+-------------------------------+  |
|	|u8  | magic (BC)                    |  |
|	+----+-------------------------------+  |
|	|u8  | magic (DA)                    |  |  
|	+----+-------------------------------+  |
|	+----+-------------------------------+  |
|	|u24 | data length                   |  |
|	+----+-------------------------------+  |
|	|u8  | data type                     |  |
|	+----+-------------------------------+  |
|	|[u8]| data                          |  |
|	+----+-------------------------------+  |
|   ....                                    |
+--------------------------------------------
....
</pre>

#### Spill over

A table bucket either points to data of type > 1 or to a spill over of the table with type 0.

A spill over may contain any number of data offsets pointing to data of type > 1.

Spill over must not be the last data element in the data file.

#### Data types

* 0 spill over
* 1 transaction or application defined data
* 2 header or block

##### Transaction or application specific data
<pre>
+----+-------------------------------------+
|u256| id                                  |
+----+-------------------------------------+
|[u8]| data                                |
+----+-------------------------------------+
</pre>

##### Header
<pre>
+----+-------------------------------------+
|[u8]| id of previous header               |
+----+-------------------------------------+
|[u8]| header as serialized in blocks      |
+----+-------------------------------------+
|u16 | number of data = 0                  |
+----+-------------------------------------+
|u256| tip                                 |
+----+-------------------------------------+
</pre>

##### Block
<pre>
+-----+-------------------------------------+
|[u8] | id of previous header or block      |
+-----+-------------------------------------+
|[u8] | header as serialized in blocks      |
+-----+-------------------------------------+
|u16  | number of data for the block        |
+-----+-------------------------------------+
|[256]| ids of data for the block           |
+-----+-------------------------------------+
|u256 | tip                                 |
+-----+-------------------------------------+
</pre>

Transactions and application defined data are inserted through insert of a block
therefore the last data element is always a header or a block and the last stored data
is the id of the tip with known u256 representation.


### Table file

The data file starts with a magic number in two bytes spelling BCFF in hex.
Thereafter any number of buckets storing 5 byte pointers into data.

The length of the table file allows calculation of current S and L of linear hashing:

* N = 1
* L = Floor(log2(used len-2))
* S = L/2 mod 2^(L-1)

<pre>
+------------- block       --------------------+
|                                              |
|  +--------+-------------------------------+  |
|  |u8      | magic (BC)                    |  |
|  +--------+-------------------------------+  |
|  |u8      | magic (FF)                    |  |
|  +--------+-------------------------------+  |
|  +--------+-------------------------------+  |
|  | u48    | data offset                   |  |
|  +--------+-------------------------------+  |
|  ...                                         |
+----------------------------------------------+
....
</pre>

### Log file

The log file starts with a magic number and last known correct file sizes.
Therafter any number of block offset and content tuples.

<pre>
+------------- block       --------------------+
|                                              |
|  +--------+-------------------------------+  |
|  |u8      | magic (BC)                    |  |
|  +--------+-------------------------------+  |
|  |u8      | magic (00)                    |  |
|  +--------+-------------------------------+  |
|  +--------+-------------------------------+  |
|  | u48    | last correct data file size   |  |
|  +--------+-------------------------------+  |
|  | u48    | last correct table file size  |  |
|  +--------+-------------------------------+  |
|  | u48    | block offset                  |  |
|  +--------+-------------------------------+  |
|  | [u8]   | block as before batch start   |  |
|  +--------+-------------------------------+  |
|  ....                                        |
+----------------------------------------------+
....
</pre>


Should the process crash while in an insert batch, then at open the log file will
trigger the following processing:
* truncate key and data files to last known correct size
* patch key file by applying the pre-image of its blocks
* delete the log file