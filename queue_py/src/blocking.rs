use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use rocksdb::Options;

/// A persistent queue with a fixed capacity. This is a blocking implementation.
///
/// Parameters
/// ----------
/// path : str
///   The path to the queue.
/// max_elements : int
///   The maximum number of elements the queue can hold. Default is ``1_000_000_000``.
///
/// Raises
/// ------
/// PyRuntimeError
///   If the queue could not be created.
///
#[pyclass]
pub struct PersistentQueueWithCapacity(queue_rs::blocking::PersistentQueueWithCapacity);

#[pymethods]
impl PersistentQueueWithCapacity {
    #[new]
    #[pyo3(signature=(path, max_elements = 1_000_000_000))]
    fn new(path: &str, max_elements: usize) -> PyResult<Self> {
        let queue = queue_rs::blocking::PersistentQueueWithCapacity::new(
            path,
            max_elements,
            Options::default(),
        )
        .map_err(|e| {
            PyRuntimeError::new_err(format!("Failed to create persistent queue: {}", e))
        })?;
        Ok(Self(queue))
    }

    /// Adds items to the queue.
    ///
    /// **GIL**: the method can optionally be called without the GIL.
    ///
    /// Parameters
    /// ----------
    /// items : list of bytes
    ///   The items to add to the queue.
    /// no_gil : bool
    ///   If True, the method will be called without the GIL. Default is ``True``.
    ///
    /// Raises
    /// ------
    /// PyRuntimeError
    ///   If the method fails.
    ///
    /// Returns
    /// -------
    /// None
    ///
    #[pyo3(signature = (items, no_gil = true))]
    fn push(&self, items: Vec<&PyBytes>, no_gil: bool) -> PyResult<()> {
        let data = items.iter().map(|e| e.as_bytes()).collect::<Vec<&[u8]>>();
        Python::with_gil(|py| {
            let f = || {
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

    /// Retrieves items from the queue.
    ///
    /// **GIL**: the method can optionally be called without the GIL.
    ///
    /// Parameters
    /// ----------
    /// max_elements : int
    ///   The maximum number of elements to retrieve. Default is ``1``.
    /// no_gil : bool
    ///   If True, the method will be called without the GIL. Default is ``True``.
    ///
    /// Raises
    /// ------
    /// PyRuntimeError
    ///   If the method fails.
    ///
    /// Returns
    /// -------
    /// items : list of bytes
    ///   The items retrieved from the queue.
    ///
    #[pyo3(signature = (max_elements = 1, no_gil = true))]
    fn pop(&self, max_elements: usize, no_gil: bool) -> PyResult<Vec<PyObject>> {
        Python::with_gil(|py| {
            if no_gil {
                py.allow_threads(|| self.0.pop(max_elements))
            } else {
                self.0.pop(max_elements)
            }
            .map(|results| {
                results
                    .into_iter()
                    .map(|r| {
                        PyBytes::new_with(py, r.len(), |b: &mut [u8]| {
                            b.copy_from_slice(&r);
                            Ok(())
                        })
                        .map(PyObject::from)
                    })
                    .collect::<PyResult<Vec<_>>>()
            })
            .map_err(|_| PyRuntimeError::new_err("Failed to pop item"))
        })?
    }

    /// Checks if the queue is empty.
    ///
    /// Returns
    /// -------
    /// bool
    ///   ``True`` if the queue is empty, ``False`` otherwise.
    ///
    #[getter]
    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the disk size of the queue in bytes.
    ///
    /// Returns
    /// -------
    /// size : int
    ///
    /// Raises
    /// ------
    /// PyRuntimeError
    ///   If the method fails.
    ///
    #[getter]
    fn disk_size(&self) -> PyResult<usize> {
        Python::with_gil(|py| {
            py.allow_threads(|| {
                self.0.disk_size().map_err(|e| {
                    PyRuntimeError::new_err(format!("Failed to get queue size: {}", e))
                })
            })
        })
    }

    /// Returns the size of the queue in bytes (only payload).
    ///
    /// Returns
    /// -------
    /// size : int
    ///
    #[getter]
    fn payload_size(&self) -> u64 {
        self.0.payload_size()
    }

    /// Returns the number of elements in the queue.
    ///
    /// Returns
    /// -------
    /// int
    ///   The number of elements in the queue.
    ///
    #[getter]
    fn len(&self) -> usize {
        self.0.len()
    }
}
