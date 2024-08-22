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

/// Removes ``PersistentQueueWithCapacity`` at the given path. The queue must be closed.
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

/// Removes ``MpmcQueue`` at the given path. The queue must be closed.
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
fn remove_mpmc_queue(path: &str) -> PyResult<()> {
    queue_rs::mpmc::MpmcQueue::remove_db(path)
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to remove mpmc queue: {}", e)))
}

#[pyclass]
#[derive(PartialEq, Copy, Clone)]
enum StartPosition {
    Oldest,
    Newest,
}

#[pymodule]
fn rocksq_blocking(_: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<blocking::PersistentQueueWithCapacity>()?;
    m.add_class::<blocking::MpmcQueue>()?;
    Ok(())
}

#[pymodule]
fn rocksq_nonblocking(_: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<nonblocking::ResponseVariant>()?;
    m.add_class::<nonblocking::Response>()?;
    m.add_class::<nonblocking::PersistentQueueWithCapacity>()?;

    m.add_class::<nonblocking::MpmcResponseVariant>()?;
    m.add_class::<nonblocking::MpmcResponse>()?;
    m.add_class::<nonblocking::MpmcQueue>()?;

    Ok(())
}

#[pymodule]
fn rocksq(py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(version, m)?)?;
    m.add_function(wrap_pyfunction!(remove_queue, m)?)?;
    m.add_function(wrap_pyfunction!(remove_mpmc_queue, m)?)?;

    m.add_wrapped(wrap_pymodule!(rocksq_blocking))?;
    m.add_wrapped(wrap_pymodule!(rocksq_nonblocking))?;

    m.add_class::<StartPosition>()?;

    let sys = PyModule::import(py, "sys")?;
    let sys_modules: &PyDict = sys.getattr("modules")?.downcast()?;

    sys_modules.set_item("rocksq.blocking", m.getattr("rocksq_blocking")?)?;
    sys_modules.set_item("rocksq.nonblocking", m.getattr("rocksq_nonblocking")?)?;

    Ok(())
}
