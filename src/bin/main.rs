extern crate blockchain_store;
extern crate rand;

use blockchain_store::infile::InFile;
use blockchain_store::bcdb::BCDBFactory;

use rand::thread_rng;
use std::collections::HashMap;
use rand::RngCore;
use std::time::{Instant};

pub fn main () {
    let mut db = InFile::new_db("testdb").unwrap();
    db.init().unwrap();

    let mut rng = thread_rng();

    let mut key = [0x0u8;32];
    // transaction size assumed 300 bytes
    let mut data = [0x0u8;300];
    let mut check = HashMap::new();

    // number of transactions
    let ntx = 10000000;
    // transactions per block
    let tb = 2000;
    // download batch size (number of blocks)
    let bat = 1000;

    let mut n = 0;
    let mut now = Instant::now();
    for _ in 1 .. ntx {
        rng.fill_bytes(&mut key);
        rng.fill_bytes(&mut data);
        db.put(&key[..], &data[..]).unwrap();
        n += 1;
        if n % (bat*tb) == 0 {
            db.batch().unwrap();
            println!("block {} {}", n / tb, now.elapsed().as_secs());
        }
        if n % 10 == 0 {
            check.insert(key, data);
        }
    }

    db.batch().unwrap();
    println!("{} million tx stored in {} blocks in batches of {} in {} seconds", ntx/1000000, ntx/tb, tb, now.elapsed().as_secs());

    now = Instant::now();
    for (k, v) in check {
        if db.get(&k[..]).unwrap().unwrap() != v.to_vec() {
            println!("failed to store correctly");
        }
    }
    println!("{} million tx retrieved in {} seconds", ntx/10000000, now.elapsed().as_secs());


    db.shutdown();
}