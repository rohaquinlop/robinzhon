use pyo3::{pyclass, pymethods};

#[derive(Debug)]
pub struct RustOperationResult {
    pub successful: Vec<String>,
    pub failed: Vec<(String, String)>,
}

#[pyclass]
#[derive(Debug, Clone)]
pub struct Results {
    #[pyo3(get)]
    pub successful: Vec<String>,
    #[pyo3(get)]
    pub failed: Vec<String>,
}

#[pymethods]
impl Results {
    #[new]
    pub fn new(successful: Vec<String>, failed: Vec<String>) -> Self {
        Self { successful, failed }
    }

    pub fn __repr__(&self) -> String {
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
