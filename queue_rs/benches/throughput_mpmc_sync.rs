use criterion::{criterion_group, criterion_main, Criterion};
use queue_rs::blocking::MpmcQueue;
use queue_rs::mpmc::StartPosition;
use std::time::Duration;

const COUNT: usize = 100;
const LABEL: &str = "label";
const BLOCK_SIZE: usize = 64 * 1024;

fn rw_mixed(c: &mut Criterion) {
    let block = vec![0u8; BLOCK_SIZE];
    let path = "/tmp/test_mpmc_b1".to_string();
    _ = MpmcQueue::remove_db(&path);

    c.bench_function("mpmc_rw_mixed", |b| {
        let db = MpmcQueue::new(&path, Duration::from_secs(60)).unwrap();
        b.iter(|| {
            for _ in 0..COUNT {
                db.add(&[&block]).unwrap();
                db.next(1, LABEL, StartPosition::Oldest).unwrap();
            }
        });
    });

    MpmcQueue::remove_db(&path).unwrap();
}

fn write_read(c: &mut Criterion) {
    let block = vec![0u8; BLOCK_SIZE];
    let path = "/tmp/test_mpmc_b2".to_string();
    _ = MpmcQueue::remove_db(&path);

    c.bench_function("mpmc_write_read", |b| {
        let db = MpmcQueue::new(&path, Duration::from_secs(60)).unwrap();
        b.iter(|| {
            for _ in 0..COUNT {
                db.add(&[&block]).unwrap();
            }
            for _ in 0..COUNT {
                db.next(1, LABEL, StartPosition::Oldest).unwrap();
            }
        });
    });

    MpmcQueue::remove_db(&path).unwrap();
}

criterion_group!(benches, rw_mixed, write_read);
criterion_main!(benches);
