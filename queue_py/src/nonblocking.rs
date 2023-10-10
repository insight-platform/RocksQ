use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use rocksdb::Options;

/// A response variant containing the actual data for push, pop, size and length operations.
/// The object is created only by the library, there is no public constructor.
///
#[pyclass]
pub struct ResponseVariant(queue_rs::nonblocking::ResponseVariant);

#[pymethods]
impl ResponseVariant {
    /// Returns the data for the ``pop()`` operation.
    ///
    /// Raises
    /// ------
    /// PyRuntimeError
    ///   If the method fails.
    ///
    /// Returns
    /// -------
    /// list of bytes
    ///   The data for the ``pop()`` operation if the operation was successful,
    /// ``None``
    ///   if the future doesn't represent the ``pop()`` operation.
    ///
    #[getter]
    fn data(&self) -> PyResult<Option<Vec<PyObject>>> {
        match &self.0 {
            queue_rs::nonblocking::ResponseVariant::Pop(data) => Ok(data
                .as_ref()
                .map(|results| {
                    Python::with_gil(|py| {
                        Some(
                            results
                                .iter()
                                .map(|r| PyObject::from(PyBytes::new(py, &r)))
                                .collect::<Vec<_>>(),
                        )
                    })
                })
                .map_err(|e| PyRuntimeError::new_err(format!("Failed to get response: {}", e)))?),
            _ => Ok(None),
        }
    }

    /// Returns the length of the queue.
    ///
    /// Raises
    /// ------
    /// PyRuntimeError
    ///   If the method fails.
    ///
    /// Returns
    /// -------
    /// ``int``
    ///   The length of the queue if the operation was successful,
    /// ``None``
    ///   if the future doesn't represent the ``length()`` operation.
    ///
    #[getter]
    fn len(&self) -> Option<usize> {
        match &self.0 {
            queue_rs::nonblocking::ResponseVariant::Length(data) => Some(*data),
            _ => None,
        }
    }

    /// Returns the size of the queue.
    ///
    /// Raises
    /// ------
    /// PyRuntimeError
    ///   If the method fails.
    ///
    /// Returns
    /// -------
    /// ``int``
    ///   The size of the queue if the operation was successful,
    /// ``None``
    ///   if the future doesn't represent the ``size()`` operation.
    ///
    #[getter]
    fn size(&self) -> PyResult<Option<usize>> {
        match &self.0 {
            queue_rs::nonblocking::ResponseVariant::Size(data) => Ok(data
                .as_ref()
                .map(|r| Some(*r))
                .map_err(|e| PyRuntimeError::new_err(format!("Failed to get response: {}", e)))?),
            _ => Ok(None),
        }
    }
}

#[pyclass]
pub struct Response(queue_rs::nonblocking::Response);

#[pymethods]
impl Response {
    /// Checks if the response is ready.
    ///
    /// Returns
    /// -------
    /// ``bool``
    ///   ``True`` if the response is ready, ``False`` otherwise.
    ///
    #[getter]
    fn is_ready(&self) -> bool {
        self.0.is_ready()
    }

    /// Returns the response if it is ready.
    ///
    /// Raises
    /// ------
    /// PyRuntimeError
    ///   If the method fails.
    ///
    /// Returns
    /// -------
    /// :py:class:`ResponseVariant`
    ///   The response if it is ready,
    /// ``None``
    ///   otherwise.
    ///
    fn try_get(&self) -> PyResult<Option<ResponseVariant>> {
        Ok(self
            .0
            .try_get()
            .map(|rvo| rvo.map(ResponseVariant))
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to get response: {}", e)))?)
    }

    /// Returns the response in a blocking way.
    ///
    /// **GIL**: the method releases the GIL
    ///
    /// Raises
    /// ------
    /// PyRuntimeError
    ///   If the method fails.
    ///
    /// Returns
    /// -------
    /// :py:class:`ResponseVariant`
    ///   The response when it is ready.
    ///
    fn get(&self) -> PyResult<ResponseVariant> {
        Python::with_gil(|py| {
            py.allow_threads(|| {
                Ok(self.0.get().map(ResponseVariant).map_err(|e| {
                    PyRuntimeError::new_err(format!("Failed to get response: {}", e))
                })?)
            })
        })
    }
}

/// A persistent queue with a fixed capacity. This is a non-blocking implementation.
/// All methods return the future-like object :py:class:`Response` which must be used to get the actual response.
///
/// Parameters
/// ----------
/// path : str
///   The path to the queue.
/// max_elements : int
///   The maximum number of elements the queue can hold. Default to ``1000_000_000``.
/// max_inflight_ops : int
///   The maximum number of inflight operations. If the number of inflight operations reached its limit,
///   further ops are blocked until the capacity is available. Default to ``1000``.
///
/// Raises
/// ------
/// PyRuntimeError
///   If the queue could not be created.
///
#[pyclass]
pub struct PersistentQueueWithCapacity(queue_rs::nonblocking::PersistentQueueWithCapacity);

#[pymethods]
impl PersistentQueueWithCapacity {
    #[new]
    #[pyo3(signature=(path, max_elements = 1000_000_000, max_inflight_ops = 1000))]
    fn new(path: &str, max_elements: usize, max_inflight_ops: usize) -> PyResult<Self> {
        let q = queue_rs::nonblocking::PersistentQueueWithCapacity::new(
            path,
            max_elements,
            max_inflight_ops,
            Options::default(),
        )
        .map_err(|e| {
            PyRuntimeError::new_err(format!("Failed to create persistent queue: {}", e))
        })?;
        Ok(Self(q))
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
    /// :py:class:`Response`
    ///   The future-like object which must be used to get the actual response. For the push operation,
    ///   the response object is only useful to call for `is_ready()`.
    ///
    #[pyo3(signature = (items, no_gil = true))]
    fn push(&mut self, items: Vec<&PyBytes>, no_gil: bool) -> PyResult<Response> {
        let data = items.iter().map(|e| e.as_bytes()).collect::<Vec<&[u8]>>();
        Python::with_gil(|py| {
            let mut f = move || {
                self.0
                    .push(&data)
                    .map_err(|e| PyRuntimeError::new_err(format!("Failed to push items: {}", e)))
            };

            if no_gil {
                py.allow_threads(f)
            } else {
                f()
            }
        })
        .map(Response)
    }

    /// Retrieves items from the queue.
    ///
    /// **GIL**: the method can optionally be called without the GIL.
    ///
    /// Parameters
    /// ----------
    /// max_elements : int
    ///   The maximum number of elements to retrieve. Default to ``1``.
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
    /// :py:class:`Response`
    ///   The future-like object which must be used to get the actual response. For the pop operation,
    ///   the response object is useful to call for ``is_ready()``, ``try_get()`` and ``get()``.
    ///
    #[pyo3(signature = (max_elements = 1, no_gil = true))]
    fn pop(&mut self, max_elements: usize, no_gil: bool) -> PyResult<Response> {
        Python::with_gil(|py| {
            Ok(if no_gil {
                py.allow_threads(|| self.0.pop(max_elements))
            } else {
                self.0.pop(max_elements)
            }
            .map(Response)
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to pop items: {}", e)))?)
        })
    }

    /// Returns the number of elements in the queue.
    ///
    /// Raises
    /// ------
    /// PyRuntimeError
    ///
    /// Returns
    /// -------
    /// :py:class:`Response`
    ///   The future-like object which must be used to get the actual response. For the size operation,
    ///   the response object is useful to call for ``is_ready()``, ``try_get()`` and ``get()``.
    ///
    pub fn size(&self) -> PyResult<Response> {
        Ok(self
            .0
            .size()
            .map(Response)
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to get size: {}", e)))?)
    }

    /// Returns the length of the queue.
    ///
    /// Raises
    /// ------
    /// PyRuntimeError
    ///   If the method fails.
    ///
    /// Returns
    /// -------
    /// :py:class:`Response`
    ///   The future-like object which must be used to get the actual response. For the length operation,
    ///   the response object is useful to call for ``is_ready()``, ``try_get()`` and ``get()``.
    ///
    pub fn len(&self) -> PyResult<Response> {
        Ok(self
            .0
            .len()
            .map(Response)
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to get length: {}", e)))?)
    }
}
