# Fast Blockchain store in Rust
A very fast persistent blockchain store and a convenience library for blockchain in-memory cache.

## Status
Work in progress, everything might change before 0.1.0 release without notice.

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

### Page

The page is the unit of read and expansion for the files. A page consists of
a payload and a used length less or equal to 4090 

<pre>
+------------------------------------+
|    | payload                       |
+----+-------------------------------+
|u48 | page  offset                  |
+----+-------------------------------+
</pre>

### Data file

The data file is strictly append only. Anything written stays there the only allowed operations are:
* append
* truncate to last known correct size

<pre>

+------------- page        -----------------+
|                                           |
|   +----+-------------------------------+  |
|   |u8  | data type                     |  |
|   +----+-------------------------------+  |
|   |u24 | data length                   |  |
|   +----+-------------------------------+  |
|   |[u8]| data                          |  |
|   +----+-------------------------------+  |
|   ....                                    |
+--------------------------------------------
....
</pre>

#### Spill over

A table bucket either points to data of type > 1 or to a spill over of the table with type 0.

A spill over may contain any number of data offsets pointing to data of type > 1.

Spill over must not be the last data element in the data file.

#### Data types

* 0 padding (ignore)
* 1 application defined data
* 2 spill over of the hash table

##### Application specific data
<pre>
+----+-------------------------------------+
|u256| id                                  |
+----+-------------------------------------+
|    | app data                            |
+----+-------------------------------------+
</pre>

##### Spill over
<pre>
+----+-------------------------------------+
|u48 | data offset                         |
+----+-------------------------------------+
|u48 | next spill over or 0                |
+----+-------------------------------------+
</pre>

##### Transaction
<pre>
+----+-------------------------------------+
| u8 | app data type                       |
+----+-------------------------------------+
|[u8]| serialized transactio               |
+----+-------------------------------------+
</pre>

##### Header
<pre>
+----+-------------------------------------+
| u8 | app data type                       |
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
+------+-------------------------------------+
| u8   | app data type                       |
+------+-------------------------------------+
|[u8]  | id of previous header or block      |
+------+-------------------------------------+
|[u8]  | header as serialized in blocks      |
+------+-------------------------------------+
|u16   | number of data for the block        |
+------+-------------------------------------+
|[u256]| ids of data for the block           |
+------+-------------------------------------+
|u256  | tip                                 |
+------+-------------------------------------+
</pre>

Transactions and application defined data are inserted through insert of a block
therefore the last data element is always a header or a block and the last stored data
is the id of the tip with known u256 representation.


### Table file

<pre>
+------------- page        --------------------+
|                                              |
|  +--------+-------------------------------+  |
|  | u16    | L (starts at 9)               |  |
|  +--------+-------------------------------+  |
|  | u48    | S (starts at 0)               |  |
|  +--------+-------------------------------+  |
|  |        | padding zeros                 |  |
|  +--------+-------------------------------+  |
+----------------------------------------------+
+-------------- page        ---------------------+
|                                                |
|  +----------------+-------------------------+  |
|  |[(u48, u48);340]|(offset, spill over or 0)|  |
|  +----------------+-------------------------+  |
|  ...                                           |
+------------------------------------------------+
....
</pre>

### Log file

The log file starts with last known correct file sizes.
Therafter any number of pages that are pre-images of the updated table file.

<pre>
+------------- page        --------------------+
|                                              |
|  +--------+-------------------------------+  |
|  | u48    | last correct data file size   |  |
|  +--------+-------------------------------+  |
|  | u48    | last correct table file size  |  |
|  +--------+-------------------------------+  |
|  |        | padding zeros                 |  |
|  +--------+-------------------------------+  |
+----------------------------------------------+
+------------- page        --------------------+
|  a key file page  as before batch start      |
+----------------------------------------------+
....
</pre>


Should the process crash while in an insert batch, then at open the log file will
trigger the following processing:
* truncate key and data files to last known correct size
* patch key file by applying the pre-image of its pages
* delete the log file