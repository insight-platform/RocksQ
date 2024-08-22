#![feature(test)]

extern crate test;

use queue_rs::blocking::MpmcQueue;
use queue_rs::mpmc::StartPosition;
use std::time::Duration;
use test::Bencher;

const COUNT: usize = 10;
const LABEL: &str = "label";

#[bench]
fn rw_mixed(b: &mut Bencher) {
    let block = vec![0u8; 256 * 1024];
    let path = "/tmp/test_mpmc_b1".to_string();
    _ = MpmcQueue::remove_db(&path);
    {
        let db = MpmcQueue::new(&path, Duration::from_secs(60)).unwrap();
        b.iter(|| {
            for _ in 0..COUNT {
                db.add(&[&block]).unwrap();
                db.next(1, LABEL, StartPosition::Oldest).unwrap();
            }
        });
    }
    MpmcQueue::remove_db(&path).unwrap();
}

#[bench]
fn write_read(b: &mut Bencher) {
    let block = vec![0u8; 256 * 1024];
    let path = "/tmp/test_mpmc_b2".to_string();
    _ = MpmcQueue::remove_db(&path);
    {
        let db = MpmcQueue::new(&path, Duration::from_secs(60)).unwrap();
        b.iter(|| {
            for _ in 0..COUNT {
                db.add(&[&block]).unwrap();
            }
            for _ in 0..COUNT {
                db.next(1, LABEL, StartPosition::Oldest).unwrap();
            }
        });
    }
    MpmcQueue::remove_db(&path).unwrap();
}
