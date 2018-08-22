# Fast Blockchain store in Rust
A very fast persistent blockchain store.

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
 
Inserts must be grouped into batches. All inserts of a batch will be stored 
or none of them, in case the process dies while inserting in a batch.
Data inserted in a batch may be fetched before closing the batch.

## Imlementation
The persistent storage should be opened by only one process. 

The store is a peristent hash map using [Linear Hashing](https://en.wikipedia.org/wiki/Linear_hashing).

The data storage size is limited to 2^48 (256 TiB) due to the use of 6 byte persistent
pointers. A data element can not exceed 2^24 (16MiB) in length. 
Writes and reads are performed in multiples of 4096 byte pages.

The persistent store uses up to three files:
* the persistent hash table (.tbl)
* the data that is iterable on its own and can be used to rebuild the hash table (.dat)
* the log file that helps to unwind a partial insert batch at re-open (.log)

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
A table bucket either points to data of type 1 or to a spill over as below

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
|u256| id of previous header               |
+----+-------------------------------------+
|[u8]| header as serialized in blocks      |
+----+-------------------------------------+
|u24 | length of additional of data        |
+----+-------------------------------------+
|[u8]| additional data                     |
+----+-------------------------------------+
|u24 | = 0                                 |
+----+-------------------------------------+
</pre>

##### Block
<pre>
+------+-------------------------------------+
| u8   | app data type                       |
+------+-------------------------------------+
|u256  | id of previous header or block      |
+------+-------------------------------------+
|[u8]  | header as serialized in blocks      |
+------+-------------------------------------+
|u24   | number of additional of data        |
+------+-------------------------------------+
|[u48] | additional data offsets             |
+------+-------------------------------------+
|u24   | number of transactions              |
+------+-------------------------------------+
|[u48] | offset of transactions              |
+------+-------------------------------------+
</pre>

Transactions and application defined data are inserted through insert of a block
therefore the last data element is always a header or a block and the last stored data
is the id of the tip with known u256 representation.


### Table file

<pre>
+------------- page        ----------------------+
|                                                |
|  +--------+-----first page only-----------+    |
|  | u48    | number of buckets             |    |
|  +--------+-------------------------------+    |
|  | u48    | step                          |    |
|  +--------+-------------------------------+    |
|                                                |
|  +----------------+-------------------------+  |
|  |[(u48, u48);339]|(offset, spill over or 0)|  |
|  +----------------+-------------------------+  |                                           |
+------------------------------------------------+
....
</pre>

### Log file

The log file starts with a page that holds last known correct file sizes.
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
* reset the log file