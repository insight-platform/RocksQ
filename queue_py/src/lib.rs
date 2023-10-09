use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3::wrap_pymodule;

mod sync;

#[pyfunction]
pub fn version() -> String {
    queue_rs::version().to_string()
}

#[pymodule]
fn rocksq_sync(_: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<sync::PersistentQueueWithCapacity>()?;
    Ok(())
}

#[pymodule]
fn rocksq(py: Python, m: &PyModule) -> PyResult<()> {
    m.add_wrapped(wrap_pymodule!(rocksq_sync))?;
    let sys = PyModule::import(py, "sys")?;
    let sys_modules: &PyDict = sys.getattr("modules")?.downcast()?;

    sys_modules.set_item("rocksq.sync", m.getattr("rocksq_sync")?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn pass() {}
}
