use pyo3::prelude::*;

mod classes;

use classes::{DownloadResults, S3Config, S3Downloader};

#[pymodule(gil_used = false)]
fn robinzhon(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<S3Config>()?;
    m.add_class::<S3Downloader>()?;
    m.add_class::<DownloadResults>()?;
    Ok(())
}
