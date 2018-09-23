extern crate blockchain_store;
extern crate rand;

use blockchain_store::infile::InFile;
use blockchain_store::bcdb::BCDBFactory;

use rand::thread_rng;
use std::collections::HashMap;
use rand::RngCore;
use std::time::{Instant};

pub fn main () {
    let mut db = InFile::new_db("first").unwrap();
    db.init().unwrap();

    let mut rng = thread_rng();

    let mut check = HashMap::new();
    let mut key = [0x0u8;32];
    let mut data = [0x0u8;320];

    for _ in 1 .. 1000000 {
        rng.fill_bytes(&mut key);
        rng.fill_bytes(&mut data);
        check.insert(key, data);
    }

    let mut n = 0;
    let now = Instant::now();
    for (k, v) in check {
        db.put(&k, &v).unwrap();
        n += 1;
        if n % 100000 == 0 {
           db.batch().unwrap();
        }
    }
    db.batch().unwrap();
    println!("{}", now.elapsed().as_secs());


    db.shutdown();
}