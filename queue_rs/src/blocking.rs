use crate::mpmc;
use crate::mpmc::StartPosition;
use anyhow::Result;
use parking_lot::Mutex;
use rocksdb::Options;
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone)]
pub struct PersistentQueueWithCapacity(Arc<Mutex<crate::PersistentQueueWithCapacity>>);

impl PersistentQueueWithCapacity {
    pub fn new(path: &str, max_elements: usize, db_options: Options) -> Result<Self> {
        let queue = crate::PersistentQueueWithCapacity::new(path, max_elements, db_options)?;
        let queue = Arc::new(Mutex::new(queue));
        Ok(Self(queue))
    }

    pub fn is_empty(&self) -> bool {
        self.0.lock().is_empty()
    }

    pub fn disk_size(&self) -> Result<usize> {
        self.0.lock().disk_size()
    }

    pub fn payload_size(&self) -> u64 {
        self.0.lock().payload_size()
    }

    pub fn len(&self) -> usize {
        self.0.lock().len()
    }

    pub fn push(&self, values: &[&[u8]]) -> Result<()> {
        self.0.lock().push(values)
    }

    pub fn pop(&self, max_elts: usize) -> Result<Vec<Vec<u8>>> {
        self.0.lock().pop(max_elts)
    }

    pub fn remove_db(path: &str) -> Result<()> {
        crate::PersistentQueueWithCapacity::remove_db(path)
    }
}

#[derive(Clone)]
pub struct MpmcQueue(Arc<Mutex<mpmc::MpmcQueue>>);

impl MpmcQueue {
    pub fn new(path: &str, ttl: Duration) -> Result<Self> {
        let inner = mpmc::MpmcQueue::new(path, ttl)?;
        Ok(Self(Arc::new(Mutex::new(inner))))
    }

    pub fn remove_db(path: &str) -> Result<()> {
        mpmc::MpmcQueue::remove_db(path)
    }

    pub fn disk_size(&self) -> Result<usize> {
        self.0.lock().disk_size()
    }

    pub fn len(&self) -> usize {
        self.0.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.lock().is_empty()
    }

    pub fn add(&self, values: &[&[u8]]) -> Result<()> {
        self.0.lock().add(values)
    }

    pub fn next(
        &self,
        max_elts: usize,
        label: &str,
        start_position: StartPosition,
    ) -> Result<(Vec<Vec<u8>>, bool)> {
        self.0.lock().next(max_elts, label, start_position)
    }

    pub fn get_labels(&self) -> Vec<String> {
        self.0.lock().get_labels()
    }

    pub fn remove_label(&self, label: &str) -> Result<bool> {
        self.0.lock().remove_label(label)
    }
}
