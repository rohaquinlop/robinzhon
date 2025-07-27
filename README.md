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
pip install robinzhon   # if you uses pip
uv add robinzhon        # if you uses uv
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

Testing Python S3Transfer implementation...
Completed in 40.05s

Testing robinzhon implementation...
Completed in 9.68s

Performance Results (1000 files)
────────────────────────────────────────────────────────────
Metric                    robinzhon       Python          Winner
────────────────────────────────────────────────────────────
Duration (seconds)        9.68            40.05           robinzhon (4.1x)
Throughput (files/sec)    103.3           25.0            robinzhon
Success Rate (%)          100.0           100.0           robinzhon
Strict Success Rate (%)   100.0           100.0           robinzhon
Files Downloaded          1000            1000
Actual Files on Disk      1000            1000
────────────────────────────────────────────────────────────
robinzhon is 75.8% faster than Python implementation
```
