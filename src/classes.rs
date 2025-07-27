use std::{fs::File, io::Write, path::Path};

use aws_config::{BehaviorVersion, Region};
use aws_sdk_s3::{self as s3};
use futures::stream::{self, StreamExt};
use pyo3::exceptions::PyRuntimeError;
use pyo3::{pyclass, pymethods, PyResult};

#[derive(Debug)]
pub struct DownloadResult {
    pub successful_downloads: Vec<String>,
    pub failed_downloads: Vec<(String, String)>, // (object_key, error_message)
}

#[pyclass]
#[derive(Debug, Clone)]
pub struct DownloadResults {
    #[pyo3(get)]
    pub successful: Vec<String>,
    #[pyo3(get)]
    pub failed: Vec<String>,
}

#[pymethods]
impl DownloadResults {
    #[new]
    fn new(successful: Vec<String>, failed: Vec<String>) -> Self {
        Self { successful, failed }
    }

    fn __repr__(&self) -> String {
        format!(
            "DownloadResults(successful={}, failed={})",
            self.successful.len(),
            self.failed.len()
        )
    }

    fn __str__(&self) -> String {
        format!(
            "DownloadResults: {} successful, {} failed",
            self.successful.len(),
            self.failed.len()
        )
    }

    fn is_complete_success(&self) -> bool {
        self.failed.is_empty()
    }

    fn has_success(&self) -> bool {
        !self.successful.is_empty()
    }

    fn has_failures(&self) -> bool {
        !self.failed.is_empty()
    }

    fn total_count(&self) -> usize {
        self.successful.len() + self.failed.len()
    }

    fn success_rate(&self) -> f64 {
        if self.total_count() == 0 {
            0.0
        } else {
            self.successful.len() as f64 / self.total_count() as f64
        }
    }
}

#[pyclass]
pub struct S3Config {
    client: s3::Client,
}

#[pyclass]
pub struct S3Downloader {
    s3_config: S3Config,
    max_concurrent_downloads: usize,
}

impl S3Config {
    pub async fn new(region_name: String) -> Self {
        let config = aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new(region_name))
            .load()
            .await;
        let client = s3::Client::new(&config);

        Self { client }
    }
}

impl S3Downloader {
    async fn download_single_file(
        &self,
        bucket_name: &str,
        object_key: &str,
        local_path: &str,
    ) -> Result<String, String> {
        let mut response = self
            .s3_config
            .client
            .get_object()
            .bucket(bucket_name)
            .key(object_key)
            .send()
            .await
            .map_err(|e| format!("Failed to get S3 object '{}': {}", object_key, e))?;

        let mut file = File::create(local_path)
            .map_err(|e| format!("Failed to create file '{}': {}", local_path, e))?;

        while let Some(bytes) = response.body.try_next().await.map_err(|e| {
            format!(
                "Failed to read S3 response body for '{}': {}",
                object_key, e
            )
        })? {
            file.write_all(&bytes)
                .map_err(|e| format!("Failed to write to file '{}': {}", local_path, e))?;
        }

        file.flush()
            .map_err(|e| format!("Failed to flush file '{}': {}", local_path, e))?;
        drop(file);

        Ok(local_path.to_string())
    }

    async fn download_files_concurrent(
        &self,
        bucket_name: &str,
        object_keys: Vec<String>,
        base_directory: &str,
    ) -> Result<DownloadResult, String> {
        std::fs::create_dir_all(base_directory)
            .map_err(|e| format!("Failed to create directory '{}': {}", base_directory, e))?;

        let bucket_name = bucket_name.to_string();
        let base_directory = base_directory.to_string();

        let download_futures = object_keys.iter().map(|object_key| {
            let bucket_name = bucket_name.clone();
            let base_directory = base_directory.clone();
            let object_key = object_key.clone();

            async move {
                let file_name = Path::new(&object_key)
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or(&object_key);

                let local_path = Path::new(&base_directory).join(file_name);
                let local_path_str = local_path.to_string_lossy().to_string();

                match self
                    .download_single_file(&bucket_name, &object_key, &local_path_str)
                    .await
                {
                    Ok(path) => (Some(path), None),
                    Err(error) => (None, Some((object_key, error))),
                }
            }
        });

        let results: Vec<_> = stream::iter(download_futures)
            .buffer_unordered(self.max_concurrent_downloads)
            .collect()
            .await;

        let mut successful_downloads = Vec::new();
        let mut failed_downloads = Vec::new();

        for (success, failure) in results {
            if let Some(path) = success {
                successful_downloads.push(path);
            }
            if let Some((key, error)) = failure {
                failed_downloads.push((key, error));
            }
        }

        Ok(DownloadResult {
            successful_downloads,
            failed_downloads,
        })
    }

    async fn download_files_concurrent_with_paths(
        &self,
        bucket_name: &str,
        downloads: Vec<(String, String)>,
    ) -> Result<DownloadResult, String> {
        let bucket_name = bucket_name.to_string();

        let download_futures = downloads.iter().map(|(object_key, local_path)| {
            let bucket_name = bucket_name.clone();
            let object_key = object_key.clone();
            let local_path = local_path.clone();

            async move {
                if let Some(parent) = Path::new(&local_path).parent() {
                    if let Err(e) = std::fs::create_dir_all(parent) {
                        return (
                            None,
                            Some((
                                object_key,
                                format!("Failed to create directory '{}': {}", parent.display(), e),
                            )),
                        );
                    }
                }

                match self
                    .download_single_file(&bucket_name, &object_key, &local_path)
                    .await
                {
                    Ok(path) => (Some(path), None),
                    Err(error) => (None, Some((object_key, error))),
                }
            }
        });

        let results: Vec<_> = stream::iter(download_futures)
            .buffer_unordered(self.max_concurrent_downloads)
            .collect()
            .await;

        let mut successful_downloads = Vec::new();
        let mut failed_downloads = Vec::new();

        for (success, failure) in results {
            if let Some(path) = success {
                successful_downloads.push(path);
            }
            if let Some((key, error)) = failure {
                failed_downloads.push((key, error));
            }
        }

        Ok(DownloadResult {
            successful_downloads,
            failed_downloads,
        })
    }
}

#[pymethods]
impl S3Downloader {
    #[new]
    #[pyo3(signature = (region_name, max_concurrent_downloads=5))]
    fn new(region_name: &str, max_concurrent_downloads: usize) -> Self {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let s3_config = rt.block_on(S3Config::new(region_name.to_string()));

        Self {
            s3_config,
            max_concurrent_downloads,
        }
    }

    fn download_file(
        &self,
        bucket_name: &str,
        object_key: &str,
        path_to_store: &str,
    ) -> PyResult<String> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| {
                PyRuntimeError::new_err(format!("Failed to create async runtime: {}", e))
            })?;

        let result = rt.block_on(async {
            self.download_single_file(bucket_name, object_key, path_to_store)
                .await
        });

        result.map_err(|e| PyRuntimeError::new_err(e))
    }

    pub fn download_multiple_files(
        &self,
        bucket_name: &str,
        object_keys: Vec<String>,
        base_directory: &str,
    ) -> PyResult<DownloadResults> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| {
                PyRuntimeError::new_err(format!("Failed to create async runtime: {}", e))
            })?;

        let result = rt.block_on(async {
            self.download_files_concurrent(bucket_name, object_keys, base_directory)
                .await
        });

        match result {
            Ok(download_result) => {
                let failed_keys: Vec<String> = download_result
                    .failed_downloads
                    .iter()
                    .map(|(key, _error)| key.clone())
                    .collect();

                if !download_result.failed_downloads.is_empty() {
                    eprintln!(
                        "Warning: {} downloads failed:",
                        download_result.failed_downloads.len()
                    );
                    for (key, error) in &download_result.failed_downloads {
                        eprintln!("  {}: {}", key, error);
                    }
                }

                Ok(DownloadResults::new(
                    download_result.successful_downloads,
                    failed_keys,
                ))
            }
            Err(e) => Err(PyRuntimeError::new_err(e)),
        }
    }

    pub fn download_multiple_files_with_paths(
        &self,
        bucket_name: &str,
        downloads: Vec<(String, String)>,
    ) -> PyResult<DownloadResults> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| {
                PyRuntimeError::new_err(format!("Failed to create async runtime: {}", e))
            })?;

        let result = rt.block_on(async {
            self.download_files_concurrent_with_paths(bucket_name, downloads)
                .await
        });

        match result {
            Ok(download_result) => {
                let failed_keys: Vec<String> = download_result
                    .failed_downloads
                    .iter()
                    .map(|(key, _error)| key.clone())
                    .collect();

                if !download_result.failed_downloads.is_empty() {
                    eprintln!(
                        "Warning: {} downloads failed:",
                        download_result.failed_downloads.len()
                    );
                    for (key, error) in &download_result.failed_downloads {
                        eprintln!("  {}: {}", key, error);
                    }
                }

                Ok(DownloadResults::new(
                    download_result.successful_downloads,
                    failed_keys,
                ))
            }
            Err(e) => Err(PyRuntimeError::new_err(e)),
        }
    }
}
