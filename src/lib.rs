use pyo3::prelude::*;

mod results;
mod s3_config;
mod s3_downloader;
mod s3_uploader;

use results::Results;
use s3_config::S3Config;
use s3_downloader::S3Downloader;
use s3_uploader::S3Uploader;

#[pymodule(gil_used = false)]
fn robinzhon(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<S3Config>()?;
    m.add_class::<Results>()?;
    m.add_class::<S3Downloader>()?;
    m.add_class::<S3Uploader>()?;
    Ok(())
}
