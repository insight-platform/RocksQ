use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3::wrap_pymodule;

mod blocking;
mod nonblocking;

/// Returns the version of the underlying queue_rs library.
///
/// Returns
/// -------
/// version : str
///   The version of the underlying queue_rs library.
///
#[pyfunction]
pub fn version() -> String {
    queue_rs::version().to_string()
}

/// Removes the queue at the given path. The queue must be closed.
///
/// Parameters
/// ----------
/// path : str
///   The path to the queue to remove.
///
/// Raises
/// ------
/// PyRuntimeError
///   If the queue could not be removed.
///
#[pyfunction]
fn remove_queue(path: &str) -> PyResult<()> {
    queue_rs::PersistentQueueWithCapacity::remove_db(path)
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to remove persistent queue: {}", e)))
}

#[pymodule]
fn rocksq_blocking(_: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<blocking::PersistentQueueWithCapacity>()?;
    Ok(())
}

#[pymodule]
fn rocksq_nonblocking(_: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<nonblocking::ResponseVariant>()?;
    m.add_class::<nonblocking::Response>()?;
    m.add_class::<nonblocking::PersistentQueueWithCapacity>()?;
    Ok(())
}

#[pymodule]
fn rocksq(py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(version, m)?)?;
    m.add_function(wrap_pyfunction!(remove_queue, m)?)?;

    m.add_wrapped(wrap_pymodule!(rocksq_blocking))?;
    m.add_wrapped(wrap_pymodule!(rocksq_nonblocking))?;

    let sys = PyModule::import(py, "sys")?;
    let sys_modules: &PyDict = sys.getattr("modules")?.downcast()?;

    sys_modules.set_item("rocksq.blocking", m.getattr("rocksq_blocking")?)?;
    sys_modules.set_item("rocksq.nonblocking", m.getattr("rocksq_nonblocking")?)?;

    Ok(())
}
