extern crate bcdb;
extern crate rand;
extern crate simple_logger;
extern crate log;

use bcdb::persistent::Persistent;
use bcdb::api::BCDBFactory;
use bcdb::api::BCDBAPI;

use rand::{thread_rng, Rng};

use std::time::{Instant};

pub fn main () {
    simple_logger::init_with_level(log::Level::Info).unwrap();
    let mut db = Persistent::new_db("testdb", 100).unwrap();
    db.init().unwrap();

    // transaction size assumed 500 bytes
    let mut data = [0x0u8;500];

    // simulating a blockchain ingest

    // number of transactions
    let ntx = 20000000;
    // transactions per block
    let tb = 1000;
    // load batch size (in number of blocks)
    let bat = 1000;

    // check keys
    let mut check = Vec::with_capacity((ntx as usize)/100);


    println!("Inserting data ...");
    let mut n = 0;
    let mut now = Instant::now();
    let mut elapsed;
    let mut key = [0u8;32];
    for i in 0 .. ntx {
        thread_rng().fill(&mut data[..]);
        thread_rng().fill(&mut key[..]);

        let pref = db.put(&key, &data, &vec!()).unwrap();
        if i % 1000 == 0 {
            check.push ((pref, key.to_vec(), data.to_vec()));
        }
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

    println!("Shuffle keys...");
    thread_rng().shuffle(&mut check);
    println!("Reading data in random order...");
    now = Instant::now();
    for (pref, key, data) in &check {
        assert_eq!(db.get(key.as_slice()).unwrap(), Some((*pref, data.clone(), vec!())));
    }
    elapsed = now.elapsed().as_secs();
    if elapsed > 0 {
        println!("Read {} transactions in {} seconds, {} read/second ", (ntx/1000), elapsed, (ntx/1000) / elapsed);
    }

    db.shutdown();
}