use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use rocksdb::Options;

#[pyclass]
pub struct PersistentQueueWithCapacity(queue_rs::sync::PersistentQueueWithCapacity);

#[pymethods]
impl PersistentQueueWithCapacity {
    #[new]
    #[pyo3(signature=(path, max_elements = 1000_000_000))]
    fn new(path: &str, max_elements: usize) -> PyResult<Self> {
        let queue = queue_rs::sync::PersistentQueueWithCapacity::new(
            path,
            max_elements,
            Options::default(),
        )
        .map_err(|e| {
            PyRuntimeError::new_err(format!("Failed to create persistent queue: {}", e))
        })?;
        Ok(Self(queue))
    }

    #[pyo3(signature = (item, no_gil = true))]
    fn push(&mut self, item: Vec<&PyBytes>, no_gil: bool) -> PyResult<()> {
        let data = item.iter().map(|e| e.as_bytes()).collect::<Vec<&[u8]>>();
        Python::with_gil(|py| {
            let mut f = move || {
                self.0
                    .push(&data)
                    .map_err(|e| PyRuntimeError::new_err(format!("Failed to push item: {}", e)))
            };

            if no_gil {
                py.allow_threads(f)
            } else {
                f()
            }
        })
    }

    #[pyo3(signature = (max_elements = 1, no_gil = true))]
    fn pop(&mut self, max_elements: usize, no_gil: bool) -> PyResult<Vec<PyObject>> {
        Python::with_gil(|py| {
            Ok(if no_gil {
                py.allow_threads(|| self.0.pop(max_elements))
            } else {
                self.0.pop(max_elements)
            }
            .map(|results| {
                results
                    .into_iter()
                    .map(|r| PyObject::from(PyBytes::new(py, &r)))
                    .collect::<Vec<_>>()
            })
            .map_err(|_| PyRuntimeError::new_err("Failed to pop item"))?)
        })
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn size(&self) -> PyResult<usize> {
        Python::with_gil(|py| {
            py.allow_threads(|| {
                self.0.size().map_err(|e| {
                    PyRuntimeError::new_err(format!("Failed to get queue size: {}", e))
                })
            })
        })
    }

    fn len(&self) -> usize {
        self.0.len()
    }

    #[staticmethod]
    fn remove_db(path: &str) -> PyResult<()> {
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

#[cfg(test)]
mod tests {
    #[test]
    fn pass() {}
}
