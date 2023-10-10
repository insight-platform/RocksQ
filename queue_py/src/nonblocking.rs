use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use rocksdb::Options;

#[pyclass]
pub struct ResponseVariant(queue_rs::nonblocking::ResponseVariant);

#[pymethods]
impl ResponseVariant {
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

    #[getter]
    fn len(&self) -> Option<usize> {
        match &self.0 {
            queue_rs::nonblocking::ResponseVariant::Length(data) => Some(*data),
            _ => None,
        }
    }

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
    #[getter]
    fn is_ready(&self) -> bool {
        self.0.is_ready()
    }

    fn try_get(&self) -> PyResult<Option<ResponseVariant>> {
        Ok(self
            .0
            .try_get()
            .map(|rvo| rvo.map(ResponseVariant))
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to get response: {}", e)))?)
    }

    fn get(&self) -> PyResult<ResponseVariant> {
        Ok(self
            .0
            .get()
            .map(ResponseVariant)
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to get response: {}", e)))?)
    }
}

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

    pub fn size(&self) -> PyResult<Response> {
        Ok(self
            .0
            .size()
            .map(Response)
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to get size: {}", e)))?)
    }

    pub fn len(&self) -> PyResult<Response> {
        Ok(self
            .0
            .len()
            .map(Response)
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to get length: {}", e)))?)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn pass() {}
}
