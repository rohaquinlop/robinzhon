# robinzhon

A high-performance Python library for fast, concurrent S3 object downloads.

## Features

- **Fast downloads**: Concurrent downloads using async I/O
- **Resilient**: Continues downloading even if some files fail
- **Simple API**: Easy-to-use methods for single and batch downloads
- **Detailed results**: Get success/failure information for batch operations

## Requirements

- Python >= 3.8

## Installation

```bash
pip install robinzhon   # if you use pip
uv add robinzhon        # if you use uv
```

## Example

```python
from robinzhon import S3Downloader

# Initialize downloader
client = S3Downloader("us-east-1")

# Download a single file
download_path = client.download_file(
    "test-bucket", "test-object-key", "./test-object-key"
)
# download_path will be the file path where the object was downloaded

# Download multiple files to the same directory
files_to_download = [
    "data/file1.csv",
    "data/file2.json",
    "logs/app.log"
]
result = client.download_multiple_files(
    "test-bucket", files_to_download, "./downloads"
)

# Check results
print(f"Downloaded {len(result.successful)} files successfully")
print(f"Downloaded files: {result.successful}")
if result.has_failures():
    print(f"Failed to download: {result.failed}")

# Download files with custom paths
downloads = [
    ("data/input.csv", "./processed/input_data.csv"),
    ("config/settings.json", "./config/app_settings.json"),
]
result = client.download_multiple_files_with_paths("test-bucket", downloads)

print(f"Success rate: {result.success_rate():.1%}")
```

## Configuration

You can configure the maximum number of concurrent downloads:

```python
# Allow up to 10 concurrent downloads (default is 5)
client = S3Downloader("us-east-1", max_concurrent_downloads=10)
```

## Performance Test Results

```text
============================================================
Performance Test: 1000 files
============================================================

Testing threaded boto3 implementation...
Completed in 24.16s

Testing aioboto3 async implementation...
Completed in 27.70s

Testing robinzhon implementation...
Download completed in 10.30s
Verifying robinzhon downloads...

Performance Results (1000 files)
================================================================================
Metric                    robinzhon       threaded boto3  aioboto3        Winner
================================================================================
Duration (seconds)        10.30           24.16           27.70           robinzhon
Throughput (files/sec)    97.1            41.4            36.1            robinzhon
Success Rate (%)          100.0           100.0           100.0           robinzhon
Files Downloaded          1000            1000            1000
================================================================================

Performance Summary:
robinzhon is 2.3x faster than threaded boto3
robinzhon is 2.7x faster than aioboto3
```
