use crate::{pylist_to_vec_of_byte_vec, value_as_slice, StartPosition};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList};
use queue_rs::mpmc;
use rocksdb::Options;
use std::time::Duration;

/// A response variant containing the actual data for push, pop, size and length operations of
/// ``PersistentQueueWithCapacity``. The object is created only by the library, there is no
/// public constructor.
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
        Python::with_gil(|py| match &self.0 {
            queue_rs::nonblocking::ResponseVariant::Pop(data) => Ok(Some(
                data.as_ref()
                    .map(|results| {
                        results
                            .iter()
                            .map(|r| {
                                PyBytes::new_bound_with(py, r.len(), |b: &mut [u8]| {
                                    b.copy_from_slice(r);
                                    Ok(())
                                })
                                .map(PyObject::from)
                            })
                            .collect::<PyResult<Vec<_>>>()
                    })
                    .map_err(|e| {
                        PyRuntimeError::new_err(format!("Failed to get response: {}", e))
                    })??,
            )),
            _ => Ok(None),
        })
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
        self.0
            .try_get()
            .map(|rvo| rvo.map(ResponseVariant))
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to get response: {}", e)))
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
                self.0
                    .get()
                    .map(ResponseVariant)
                    .map_err(|e| PyRuntimeError::new_err(format!("Failed to get response: {}", e)))
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
///   The maximum number of elements the queue can hold. Default to ``1_000_000_000``.
/// max_inflight_ops : int
///   The maximum number of inflight operations. If the number of inflight operations reached its limit,
///   further ops are blocked until the capacity is available. Default to ``1_000``.
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
    #[pyo3(signature=(path, max_elements = 1_000_000_000, max_inflight_ops = 1_000))]
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
    fn push(&self, items: &Bound<'_, PyList>, no_gil: bool) -> PyResult<Response> {
        let items = pylist_to_vec_of_byte_vec(items);
        let data = value_as_slice(&items);
        Python::with_gil(|py| {
            let f = || {
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

    #[getter]
    pub fn inflight_ops(&self) -> PyResult<usize> {
        self.0
            .inflight_ops()
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to get inflight ops: {}", e)))
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
    fn pop(&self, max_elements: usize, no_gil: bool) -> PyResult<Response> {
        Python::with_gil(|py| {
            if no_gil {
                py.allow_threads(|| self.0.pop(max_elements))
            } else {
                self.0.pop(max_elements)
            }
            .map(Response)
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to pop items: {}", e)))
        })
    }

    /// Returns the disk size of the queue.
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
    #[getter]
    pub fn disk_size(&self) -> PyResult<Response> {
        self.0
            .disk_size()
            .map(Response)
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to get size: {}", e)))
    }

    /// Returns the payload size of the queue.
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
    #[getter]
    pub fn payload_size(&self) -> PyResult<Response> {
        self.0
            .payload_size()
            .map(Response)
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to get size: {}", e)))
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
    #[getter]
    pub fn len(&self) -> PyResult<Response> {
        self.0
            .len()
            .map(Response)
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to get length: {}", e)))
    }
}

/// A response variant containing the actual data for add, next, size and length operations of
/// ``MpmcQueue``. The object is created only by the library, there is no public constructor.
///
#[pyclass]
pub struct MpmcResponseVariant(queue_rs::nonblocking::MpmcResponseVariant);

#[pymethods]
impl MpmcResponseVariant {
    /// Returns the data for the ``next()`` operation.
    ///
    /// Raises
    /// ------
    /// PyRuntimeError
    ///   If the method fails.
    ///
    /// Returns
    /// -------
    /// (list of bytes, bool)
    ///   The data for the ``next()`` operation if the operation was successful,
    /// ``None``
    ///   if the future doesn't represent the ``next()`` operation.
    ///
    #[getter]
    fn data(&self) -> PyResult<Option<(Vec<PyObject>, bool)>> {
        Python::with_gil(|py| match &self.0 {
            queue_rs::nonblocking::MpmcResponseVariant::Next(data) => Ok(Some(
                data.as_ref()
                    .map(|result| {
                        result
                            .0
                            .iter()
                            .map(|r| {
                                PyBytes::new_bound_with(py, r.len(), |b: &mut [u8]| {
                                    b.copy_from_slice(r);
                                    Ok(())
                                })
                                .map(PyObject::from)
                            })
                            .collect::<PyResult<Vec<_>>>()
                            .map(|e| (e, result.1))
                    })
                    .map_err(|e| {
                        PyRuntimeError::new_err(format!("Failed to get response: {}", e))
                    })??,
            )),
            _ => Ok(None),
        })
    }

    /// Returns the data for the ``get_labels()`` operation.
    ///
    /// Raises
    /// ------
    /// PyRuntimeError
    ///   If the method fails.
    ///
    /// Returns
    /// -------
    /// list of str
    ///   The data for the ``get_labels()`` operation if the operation was successful,
    /// ``None``
    ///   if the future doesn't represent the ``get_labels()`` operation.
    ///
    #[getter]
    fn labels(&self) -> Option<Vec<String>> {
        match &self.0 {
            queue_rs::nonblocking::MpmcResponseVariant::GetLabels(data) => Some(data.to_vec()),
            _ => None,
        }
    }

    /// Returns the result of ``remove_label()`` operation.
    ///
    /// Raises
    /// ------
    /// PyRuntimeError
    ///   If the method fails.
    ///
    /// Returns
    /// -------
    /// bool
    ///   ``True`` if the consumer label existed, ``False`` otherwise.
    /// ``None``
    ///   if the future doesn't represent the ``size()`` operation.
    ///
    #[getter]
    fn removed_label(&self) -> PyResult<Option<bool>> {
        match &self.0 {
            queue_rs::nonblocking::MpmcResponseVariant::RemoveLabel(data) => Ok(data
                .as_ref()
                .map(|r| Some(*r))
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
            queue_rs::nonblocking::MpmcResponseVariant::Length(data) => Some(*data),
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
            queue_rs::nonblocking::MpmcResponseVariant::Size(data) => Ok(data
                .as_ref()
                .map(|r| Some(*r))
                .map_err(|e| PyRuntimeError::new_err(format!("Failed to get response: {}", e)))?),
            _ => Ok(None),
        }
    }
}

#[pyclass]
pub struct MpmcResponse(queue_rs::nonblocking::MpmcResponse);

#[pymethods]
impl MpmcResponse {
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
    /// :py:class:`MpmcResponseVariant`
    ///   The response if it is ready,
    /// ``None``
    ///   otherwise.
    ///
    fn try_get(&self) -> PyResult<Option<MpmcResponseVariant>> {
        self.0
            .try_get()
            .map(|rvo| rvo.map(MpmcResponseVariant))
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to get response: {}", e)))
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
    /// :py:class:`MpmcResponseVariant`
    ///   The response when it is ready.
    ///
    fn get(&self) -> PyResult<MpmcResponseVariant> {
        Python::with_gil(|py| {
            py.allow_threads(|| {
                self.0
                    .get()
                    .map(MpmcResponseVariant)
                    .map_err(|e| PyRuntimeError::new_err(format!("Failed to get response: {}", e)))
            })
        })
    }
}

/// A persistent queue with a ttl that supports multiple consumers marked with labels. This is a
/// non-blocking implementation. All methods return the future-like object :py:class:`MpmcResponse`
/// which must be used to get the actual response.
///
/// Parameters
/// ----------
/// path : str
///   The path to the queue.
/// ttl : int
///   The amount of seconds after which the element in the queue will be removed. TTL is not strict.
///   It means that the element will remain in the queue for TTL seconds after insertion and the
///   queue will make efforts to remove the element after TTL seconds but it is not guaranteed to be
///   done immediately. Thus, consumers can retrieve expired but not removed elements.
/// max_inflight_ops : int
///   The maximum number of inflight operations. If the number of inflight operations reached its limit,
///   further ops are blocked until the capacity is available. Default to ``1_000``.
///
/// Raises
/// ------
/// PyRuntimeError
///   If the queue could not be created.
///
#[pyclass]
pub struct MpmcQueue(queue_rs::nonblocking::MpmcQueue);

#[pymethods]
impl MpmcQueue {
    #[new]
    #[pyo3(signature=(path, ttl, max_inflight_ops = 1_000))]
    fn new(path: &str, ttl: u32, max_inflight_ops: usize) -> PyResult<Self> {
        let q = queue_rs::nonblocking::MpmcQueue::new(
            path,
            Duration::from_secs(ttl as u64),
            max_inflight_ops,
        )
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to create mpmc queue: {}", e)))?;
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
    /// :py:class:`MpmcResponse`
    ///   The future-like object which must be used to get the actual response. For the add operation,
    ///   the response object is only useful to call for `is_ready()`.
    ///
    #[pyo3(signature = (items, no_gil = true))]
    fn add(&self, items: &Bound<'_, PyList>, no_gil: bool) -> PyResult<MpmcResponse> {
        let items = pylist_to_vec_of_byte_vec(items);
        let data = value_as_slice(&items);
        Python::with_gil(|py| {
            let f = || {
                self.0
                    .add(&data)
                    .map_err(|e| PyRuntimeError::new_err(format!("Failed to add items: {}", e)))
            };

            if no_gil {
                py.allow_threads(f)
            } else {
                f()
            }
        })
        .map(MpmcResponse)
    }

    #[getter]
    pub fn inflight_ops(&self) -> PyResult<usize> {
        self.0
            .inflight_ops()
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to get inflight ops: {}", e)))
    }

    /// Retrieves items from the queue.
    ///
    /// **GIL**: the method can optionally be called without the GIL.
    ///
    /// Parameters
    /// ----------
    /// label: str
    ///   The consumer label that determines the start position in the queue to retrieve elements.
    ///   If the label does not exist the start position is determined by ``option` parameter. If
    ///   the label exists the start position is the next element after the last call of this
    ///   method. If some elements are expired between the last and this call next non-expired
    ///   elements will be retrieved.
    /// start_position: StartPosition
    ///    The option that determines the start position in the queue to retrieve elements if the
    ///    consumer label does not exist.
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
    /// :py:class:`MpmcResponse`
    ///   The future-like object which must be used to get the actual response. For the add operation,
    ///   the response object is useful to call for ``is_ready()``, ``try_get()`` and ``get()``.
    ///
    #[pyo3(signature = (label, start_position, max_elements = 1, no_gil = true))]
    fn next(
        &self,
        label: &str,
        start_position: StartPosition,
        max_elements: usize,
        no_gil: bool,
    ) -> PyResult<MpmcResponse> {
        Python::with_gil(|py| {
            let start_position = match start_position {
                StartPosition::Oldest => mpmc::StartPosition::Oldest,
                StartPosition::Newest => mpmc::StartPosition::Newest,
            };
            if no_gil {
                py.allow_threads(|| self.0.next(max_elements, label, start_position))
            } else {
                self.0.next(max_elements, label, start_position)
            }
            .map(MpmcResponse)
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to pop items: {}", e)))
        })
    }

    /// Returns the disk size of the queue.
    ///
    /// Raises
    /// ------
    /// PyRuntimeError
    ///
    /// Returns
    /// -------
    /// :py:class:`MpmcResponse`
    ///   The future-like object which must be used to get the actual response. For the size operation,
    ///   the response object is useful to call for ``is_ready()``, ``try_get()`` and ``get()``.
    ///
    #[getter]
    pub fn disk_size(&self) -> PyResult<MpmcResponse> {
        self.0
            .disk_size()
            .map(MpmcResponse)
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to get size: {}", e)))
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
    /// :py:class:`MpmcResponse`
    ///   The future-like object which must be used to get the actual response. For the length operation,
    ///   the response object is useful to call for ``is_ready()``, ``try_get()`` and ``get()``.
    ///
    #[getter]
    pub fn len(&self) -> PyResult<MpmcResponse> {
        self.0
            .len()
            .map(MpmcResponse)
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to get length: {}", e)))
    }

    /// Returns the consumer labels.
    ///
    /// Returns
    /// -------
    /// labels: list of str
    ///   The consumer labels.
    ///
    #[getter]
    fn labels(&self) -> PyResult<MpmcResponse> {
        self.0
            .get_labels()
            .map(MpmcResponse)
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to get labels: {}", e)))
    }

    /// Remove the consumer label from the queue.
    ///
    /// **GIL**: the method can optionally be called without the GIL.
    ///
    /// Parameters
    /// ----------
    /// label : str
    ///   The consumer label to remove.
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
    /// bool
    ///   ``True`` if the consumer label existed, ``False`` otherwise.
    ///
    #[pyo3(signature = (label, no_gil = true))]
    fn remove_label(&self, label: &str, no_gil: bool) -> PyResult<MpmcResponse> {
        Python::with_gil(|py| {
            let f = || {
                self.0
                    .remove_label(label)
                    .map_err(|e| PyRuntimeError::new_err(format!("Failed to remove label: {}", e)))
            };

            if no_gil {
                py.allow_threads(f)
            } else {
                f()
            }
        })
        .map(MpmcResponse)
    }
}
