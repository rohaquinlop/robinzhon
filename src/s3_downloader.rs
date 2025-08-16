use std::path::Path;
use std::sync::Arc;

use crate::results::{Results, RustOperationResult};
use crate::s3_config::S3Config;
use futures::stream::{self, StreamExt};
use pyo3::exceptions::PyRuntimeError;
use pyo3::{pyclass, pymethods, PyResult};
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, BufWriter};

#[pyclass]
pub struct S3Downloader {
    s3_config: Arc<S3Config>,
    max_concurrent_downloads: usize,
}

impl S3Downloader {
    async fn download_single_file(
        s3_config: Arc<S3Config>,
        bucket_name: &str,
        object_key: &str,
        local_path: &str,
    ) -> Result<String, String> {
        let response = s3_config
            .client
            .get_object()
            .bucket(bucket_name)
            .key(object_key)
            .send()
            .await
            .map_err(|e| format!("Failed to get S3 object '{}': {}", object_key, e))?;

        let file = File::create(local_path)
            .await
            .map_err(|e| format!("Failed to create file '{}': {}", local_path, e))?;

        let mut writer = BufWriter::new(file);
        let mut body = response.body;

        while let Some(bytes) = body.try_next().await.map_err(|e| {
            format!(
                "Failed to read S3 response body for '{}': {}",
                object_key, e
            )
        })? {
            writer
                .write_all(&bytes)
                .await
                .map_err(|e| format!("Failed to write to file '{}': {}", local_path, e))?;
        }

        writer
            .flush()
            .await
            .map_err(|e| format!("Failed to flush file '{}': {}", local_path, e))?;

        Ok(local_path.to_string())
    }

    async fn download_files_concurrent(
        s3_config: Arc<S3Config>,
        bucket_name: &str,
        object_keys: Vec<String>,
        base_directory: &str,
        max_concurrent: usize,
    ) -> Result<RustOperationResult, String> {
        tokio::fs::create_dir_all(base_directory)
            .await
            .map_err(|e| format!("Failed to create directory '{}': {}", base_directory, e))?;

        let results: Vec<_> = stream::iter(object_keys.into_iter().map(|object_key| {
            let s3_config = Arc::clone(&s3_config);

            async move {
                let file_name = Path::new(&object_key)
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or(&object_key);

                let local_path = Path::new(&base_directory).join(file_name);
                let local_path_str = local_path.to_string_lossy().to_string();

                match Self::download_single_file(
                    s3_config,
                    bucket_name,
                    &object_key,
                    &local_path_str,
                )
                .await
                {
                    Ok(path) => (Some(path), None),
                    Err(error) => (None, Some((object_key, error))),
                }
            }
        }))
        .buffer_unordered(max_concurrent)
        .collect()
        .await;

        let mut successful = Vec::new();
        let mut failed = Vec::new();

        for (success, error) in results {
            match (success, error) {
                (Some(path), None) => successful.push(path),
                (None, Some((key, error))) => failed.push((key, error)),
                _ => unreachable!(),
            }
        }
        Ok(RustOperationResult { successful, failed })
    }

    async fn download_files_concurrent_with_paths(
        s3_config: Arc<S3Config>,
        bucket_name: String,
        downloads: Vec<(String, String)>,
        max_concurrent: usize,
    ) -> Result<RustOperationResult, String> {
        let results: Vec<_> =
            stream::iter(downloads.into_iter().map(|(object_key, local_path)| {
                let s3_config = Arc::clone(&s3_config);
                let bucket_name = bucket_name.clone();

                async move {
                    if let Some(parent) = Path::new(&local_path).parent() {
                        if let Err(e) = tokio::fs::create_dir_all(parent).await {
                            return (
                                None,
                                Some((
                                    object_key,
                                    format!(
                                        "Failed to create directory '{}': {}",
                                        parent.display(),
                                        e
                                    ),
                                )),
                            );
                        }
                    }
                    match Self::download_single_file(
                        s3_config,
                        &bucket_name,
                        &object_key,
                        &local_path,
                    )
                    .await
                    {
                        Ok(path) => (Some(path), None),
                        Err(error) => (None, Some((object_key, error))),
                    }
                }
            }))
            .buffer_unordered(max_concurrent)
            .collect()
            .await;

        let mut successful = Vec::new();
        let mut failed = Vec::new();

        for (success, error) in results {
            match (success, error) {
                (Some(path), None) => successful.push(path),
                (None, Some((key, error))) => failed.push((key, error)),
                _ => unreachable!(),
            }
        }

        Ok(RustOperationResult { successful, failed })
    }
}

#[pymethods]
impl S3Downloader {
    #[new]
    #[pyo3(signature = (region_name, max_concurrent_downloads=5))]
    fn new(region_name: &str, max_concurrent_downloads: usize) -> Self {
        let s3_config = std::thread::spawn({
            let region_name = region_name.to_string();
            move || {
                let rt = tokio::runtime::Runtime::new().unwrap();
                rt.block_on(S3Config::new(region_name))
            }
        })
        .join()
        .unwrap();

        Self {
            s3_config: Arc::new(s3_config),
            max_concurrent_downloads,
        }
    }

    #[pyo3(signature=(bucket_name, object_key, path_to_store))]
    fn download_file(
        &self,
        bucket_name: &str,
        object_key: &str,
        path_to_store: &str,
    ) -> PyResult<String> {
        let s3_config = Arc::clone(&self.s3_config);
        let bucket_name = bucket_name.to_string();
        let object_key = object_key.to_string();
        let path_to_store = path_to_store.to_string();

        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to create runtime: {}", e)))?;

        rt.block_on(async move {
            Self::download_single_file(s3_config, &bucket_name, &object_key, &path_to_store).await
        })
        .map_err(PyRuntimeError::new_err)
    }

    #[pyo3(signature = (bucket_name, object_keys, base_directory))]
    fn download_multiple_files(
        &self,
        bucket_name: &str,
        object_keys: Vec<String>,
        base_directory: &str,
    ) -> PyResult<Results> {
        let s3_config = Arc::clone(&self.s3_config);
        let bucket_name = bucket_name.to_string();
        let base_directory = base_directory.to_string();
        let max_concurrent = self.max_concurrent_downloads;

        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to create runtime: {}", e)))?;

        let result = rt.block_on(async move {
            Self::download_files_concurrent(
                s3_config,
                &bucket_name,
                object_keys,
                &base_directory,
                max_concurrent,
            )
            .await
        });

        match result {
            Ok(download_result) => {
                let failed: Vec<String> = download_result
                    .failed
                    .iter()
                    .map(|(key, _error)| key.clone())
                    .collect();

                Ok(Results::new(download_result.successful, failed))
            }
            Err(e) => Err(PyRuntimeError::new_err(e)),
        }
    }

    #[pyo3(signature = (bucket_name, downloads))]
    fn download_multiple_files_with_paths(
        &self,
        bucket_name: &str,
        downloads: Vec<(String, String)>,
    ) -> PyResult<Results> {
        let s3_config = Arc::clone(&self.s3_config);
        let bucket_name = bucket_name.to_string();
        let max_concurrent = self.max_concurrent_downloads;

        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to create runtime: {}", e)))?;

        let result = rt.block_on(async move {
            Self::download_files_concurrent_with_paths(
                s3_config,
                bucket_name,
                downloads,
                max_concurrent,
            )
            .await
        });

        match result {
            Ok(download_result) => {
                let failed = download_result
                    .failed
                    .iter()
                    .map(|(key, _error)| key.clone())
                    .collect();
                Ok(Results::new(download_result.successful, failed))
            }
            Err(e) => Err(PyRuntimeError::new_err(e)),
        }
    }
}
