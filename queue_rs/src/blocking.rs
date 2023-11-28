use anyhow::Result;
use parking_lot::Mutex;
use rocksdb::Options;
use std::sync::Arc;

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

    pub fn size(&self) -> Result<usize> {
        self.0.lock().size()
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
