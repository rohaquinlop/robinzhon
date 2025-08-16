# robinzhon

Minimal, high-performance Python helpers for concurrent S3 object transfers.

A small extension that exposes fast, concurrent downloads (and uploads) to Python
using a Rust implementation optimized for I/O-bound workloads.

## Install

- From PyPI: `pip install robinzhon` or `uv add robinzhon`
- From source (requires Rust + maturin):

```bash
# build and install into active venv
maturin develop --release
```

## Quick start

Download a single object:

```python
from robinzhon import S3Downloader

d = S3Downloader("us-east-1")
path = d.download_file("my-bucket", "path/to/object.txt", "./object.txt")
print(path)  # ./object.txt
```

Download many objects into a directory:

```python
files = ["data/a.csv", "data/b.csv"]
res = d.download_multiple_files("my-bucket", files, "./downloads")
print(res.successful)
print(res.failed)
```

Upload a single file or multiple files:

```python
from robinzhon import S3Uploader

u = S3Uploader("us-east-1")
u.upload_file("my-bucket", "dest/key.txt", "./local.txt")
paths_and_keys = [("./local1.txt", "key1"), ("./local2.txt", "key2")]
res = u.upload_multiple_files("my-bucket", paths_and_keys)
print(res.successful, res.failed)
```

## Configuration

- Both `S3Downloader` and `S3Uploader` accept an optional concurrency argument (default 5):

```python
S3Downloader("us-east-1", max_concurrent_downloads=10)
S3Uploader("us-east-1", max_concurrent_uploads=10)
```

## API summary

- Results
    - Attributes: `successful: List[str]`, `failed: List[str]`
    - Methods: `is_complete_success()`, `has_success()`, `has_failures()`, `total_count()`, `success_rate()`

- S3Downloader(region_name, max_concurrent_downloads=5)
    - `download_file(bucket, key, local_path) -> str`
    - `download_multiple_files(bucket, keys, base_dir) -> Results`
    - `download_multiple_files_with_paths(bucket, [(key, local_path), ...]) -> Results`

- S3Uploader(region_name, max_concurrent_uploads=5)
    - `upload_file(bucket, key, local_path) -> str`
    - `upload_multiple_files(bucket, [(local_path, key), ...]) -> Results`

## Building & testing

- Requires Rust toolchain and `maturin` to build the extension.
- Tests are simple Python tests that assume the compiled extension is available.

## License

See the [LICENSE](LICENSE) file in the repository.
