use crate::results::{Results, RustOperationResult};
use crate::s3_config::S3Config;
use aws_sdk_s3::{operation::put_object::PutObjectOutput, primitives::ByteStream};
use futures::stream::{self, StreamExt};
use pyo3::exceptions::PyRuntimeError;
use pyo3::{pyclass, pymethods, PyResult};
use rayon::prelude::*;
use std::path::Path;

#[pyclass]
pub struct S3Uploader {
    s3_config: S3Config,
    max_concurrent_uploads: usize,
}

impl S3Uploader {
    async fn upload_single_file(
        &self,
        bucket_name: &str,
        object_key: &str,
        local_path: &str,
    ) -> Result<String, String> {
        let body = ByteStream::from_path(Path::new(local_path)).await;

        let response = self
            .s3_config
            .client
            .put_object()
            .bucket(bucket_name)
            .key(object_key)
            .body(body.unwrap())
            .send()
            .await
            .map_err(|e| format!("Failed to upload S3 object '{}': '{}'", local_path, e))?;

        let PutObjectOutput { .. } = response;
        Ok(local_path.to_string())
    }

    async fn upload_files_concurrent(
        &self,
        bucket_name: &str,
        paths_and_keys: Vec<(String, String)>,
    ) -> Result<RustOperationResult, String> {
        let bucket_name = bucket_name.to_string();

        let upload_futures = paths_and_keys.iter().map(|(local_path, object_key)| {
            let bucket_name = bucket_name.clone();

            async move {
                match self
                    .upload_single_file(&bucket_name, object_key, local_path)
                    .await
                {
                    Ok(path) => (Some(path), None),
                    Err(error) => (None, Some((local_path.clone(), error))),
                }
            }
        });

        let results: Vec<_> = stream::iter(upload_futures)
            .buffer_unordered(self.max_concurrent_uploads)
            .collect()
            .await;

        let successful: Vec<String> = results
            .par_iter()
            .filter(|(success, _)| matches!(success, Some(_path)))
            .map(|(success, _)| success.clone().unwrap())
            .collect();

        let failed: Vec<(String, String)> = results
            .par_iter()
            .filter(|(_, failure)| matches!(failure, Some((_local_path, _error))))
            .map(|(_, failure)| failure.clone().unwrap())
            .collect();

        Ok(RustOperationResult { successful, failed })
    }
}

#[pymethods]
impl S3Uploader {
    #[new]
    #[pyo3(signature = (region_name, max_concurrent_uploads=5))]
    fn new(region_name: &str, max_concurrent_uploads: usize) -> Self {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let s3_config = rt.block_on(S3Config::new(region_name.to_string()));

        Self {
            s3_config,
            max_concurrent_uploads,
        }
    }

    fn upload_file(
        &self,
        bucket_name: &str,
        object_key: &str,
        local_path: &str,
    ) -> PyResult<String> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| {
                PyRuntimeError::new_err(format!("Failed to create async runtime: {}", e))
            })?;

        let result = rt.block_on(async {
            self.upload_single_file(bucket_name, object_key, local_path)
                .await
        });

        result.map_err(PyRuntimeError::new_err)
    }

    pub fn upload_multiple_files(
        &self,
        bucket_name: &str,
        paths_and_keys: Vec<(String, String)>,
    ) -> PyResult<Results> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| {
                PyRuntimeError::new_err(format!("Failed to create async runtime: {}", e))
            })?;

        let result = rt.block_on(async {
            self.upload_files_concurrent(bucket_name, paths_and_keys)
                .await
        });

        match result {
            Ok(upload_result) => {
                let failed_paths: Vec<String> = upload_result
                    .failed
                    .par_iter()
                    .map(|(key, _error)| key.clone())
                    .collect();

                if !failed_paths.is_empty() {
                    eprintln!("Warning {} uploads failed:", failed_paths.len());
                    for (path, error) in &upload_result.failed {
                        eprintln!("  {}: {}", path, error);
                    }
                }

                Ok(Results::new(upload_result.successful, failed_paths))
            }
            Err(e) => Err(PyRuntimeError::new_err(e)),
        }
    }
}
