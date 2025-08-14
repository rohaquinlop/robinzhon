use aws_config::{BehaviorVersion, Region};
use aws_sdk_s3::{self as s3};
use pyo3::pyclass;

#[pyclass]
pub struct S3Config {
    pub client: s3::Client,
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
