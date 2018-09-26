extern crate blockchain_store;
extern crate bitcoin;

use blockchain_store::infile::InFile;
use blockchain_store::bcdb::BCDBFactory;

use bitcoin::util::hash::Sha256dHash;

use std::time::{Instant};
use std::collections::HashSet;
use std::mem::transmute;

pub fn main () {
    let mut db = InFile::new_db("testdb").unwrap();
    db.init().unwrap();

    // transaction size assumed 300 bytes
    let data = [0x0u8;300];

    // simulating a blockchain ingest

    // number of transactions (100 million)
    let ntx = 100000000;
    // transactions per block
    let tb = 1000;
    // load batch size (in number of blocks)
    let bat = 1000;

    // generate unique keys
    println!("Generate keys ...");
    let mut keys = HashSet::new();
    for i in 1 .. ntx {
        let bytes: [u8; 8] = unsafe { transmute(i) };
        let hash = Sha256dHash::from_data(&bytes);
        let key = hash.data();
        keys.insert (key);
    }


    println!("Inserting data ...");
    let mut n = 0;
    let mut now = Instant::now();
    let mut elapsed;
    for key in &keys {
        db.put(key, &data).unwrap();
        n += 1;

        if n % (bat*tb) == 0 {
            db.batch().unwrap();
            elapsed = now.elapsed().as_secs();
            println!("Stored {} million transactions in {} seconds, {} inserts/second.", n/1000000, elapsed, n/elapsed);
        }
    }

    db.batch().unwrap();
    elapsed = now.elapsed().as_secs();
    println!("Stored {} million transactions in {} seconds, {} inserts/second ", ntx/1000000, elapsed, ntx/elapsed);

    println!("Reading data ...");
    now = Instant::now();
    for key in &keys {
        db.get(key).unwrap();
    }
    elapsed = now.elapsed().as_secs();
    println!("Read {} million transactions in {} seconds, {} read/second ", ntx/1000000, elapsed, ntx/elapsed);


    db.shutdown();
}