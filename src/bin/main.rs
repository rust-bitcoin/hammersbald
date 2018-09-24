extern crate blockchain_store;
extern crate bitcoin;
extern crate rand;

use blockchain_store::infile::InFile;
use blockchain_store::bcdb::BCDBFactory;

use bitcoin::util::hash::Sha256dHash;

use rand::thread_rng;
use rand::RngCore;

use std::time::{Instant};

pub fn main () {
    let mut db = InFile::new_db("testdb").unwrap();
    db.init().unwrap();

    // transaction size assumed 300 bytes
    let mut data = [0x0u8;300];

    // number of transactions
    let ntx = 50000000;
    // transactions per block
    let tb = 1000;
    // download batch size (number of blocks)
    let bat = 1000;

    let mut rng = thread_rng();
    let mut n = 0;
    let mut now = Instant::now();
    for i in 1 .. ntx {
        use std::mem::transmute;

        let bytes: [u8; 4] = unsafe { transmute(i) };
        data[0 .. 4].copy_from_slice(&bytes[0 .. 4]);

        let hash = Sha256dHash::from_data(&data);
        let key = hash.data();

        db.put(&key[..], &data).unwrap();
        n += 1;

        if n % (bat*tb) == 0 {
            db.batch().unwrap();
            println!("stored {} txs in {} s", n, now.elapsed().as_secs());
        }
    }

    db.batch().unwrap();
    println!("stored {} txs in {} s", ntx, now.elapsed().as_secs());

    now = Instant::now();
    let m = ntx/10;
    for _ in 1 .. m {
        use std::mem::transmute;

        let n = rng.next_u32() % ntx;
        let bytes: [u8; 4] = unsafe { transmute(n) };
        data[0 .. 4].copy_from_slice(&bytes[0 .. 4]);

        let hash = Sha256dHash::from_data(&data);
        let key = hash.data();

        if db.get(&key).unwrap().unwrap() != data.to_vec() {
            println!("failed to store correctly");
        }
    }
    println!("{} txs retrieved in {} s", m, now.elapsed().as_secs());


    db.shutdown();
}