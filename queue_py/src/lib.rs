use parking_lot::Mutex;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use rocksdb::Options;
use std::sync::Arc;

#[pyclass]
pub struct PersistentQueueWithCapacity {
    queue: Arc<Mutex<queue_rs::PersistentQueueWithCapacity>>,
}

#[pymethods]
impl PersistentQueueWithCapacity {
    #[new]
    #[pyo3(signature=(path, max_elements = 1000000000))]
    fn new(path: String, max_elements: u128) -> PyResult<Self> {
        let queue =
            queue_rs::PersistentQueueWithCapacity::new(path, max_elements, Options::default())
                .map_err(|e| {
                    PyRuntimeError::new_err(format!("Failed to create persistent queue: {}", e))
                })?;
        let queue = Arc::new(Mutex::new(queue));
        Ok(Self { queue })
    }

    #[pyo3(signature = (item, no_gil = true))]
    fn push(&mut self, item: &PyBytes, no_gil: bool) -> PyResult<()> {
        let bytes = item.as_bytes();
        Python::with_gil(|py| {
            let f = || {
                self.queue
                    .lock()
                    .push(bytes)
                    .map_err(|e| PyRuntimeError::new_err(format!("Failed to push item: {}", e)))
            };

            if no_gil {
                py.allow_threads(|| f())
            } else {
                f()
            }
        })
    }

    #[pyo3(signature = (no_gil = true))]
    fn pop(&mut self, no_gil: bool) -> PyResult<Option<PyObject>> {
        Python::with_gil(|py| {
            Ok(if no_gil {
                py.allow_threads(|| self.queue.lock().pop())
            } else {
                self.queue.lock().pop()
            }
            .map(|e| e.map(|e| PyObject::from(PyBytes::new(py, e.as_slice()))))
            .map_err(|_| PyRuntimeError::new_err("Failed to pop item"))?)
        })
    }

    fn is_empty(&self) -> bool {
        self.queue.lock().is_empty()
    }

    fn size(&self) -> PyResult<u64> {
        self.queue
            .lock()
            .size()
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to get queue size: {}", e)))
    }

    fn len(&self) -> u128 {
        self.queue.lock().len()
    }

    #[staticmethod]
    fn remove_db(path: String) -> PyResult<()> {
        queue_rs::PersistentQueueWithCapacity::remove_db(path).map_err(|e| {
            PyRuntimeError::new_err(format!("Failed to remove persistent queue: {}", e))
        })
    }
}

#[pyfunction]
pub fn version() -> String {
    queue_rs::version().to_string()
}

#[pymodule]
fn rocksq(_: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(version, m)?)?;
    m.add_class::<PersistentQueueWithCapacity>()?;
    Ok(())
}
