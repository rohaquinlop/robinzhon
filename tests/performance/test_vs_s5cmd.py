"""
Performance benchmark comparing s5cmd vs robinzhon.

This module provides parametrized tests for comparing:
- s5cmd: Go-based CLI tool for parallel S3 operations
- robinzhon: Rust-based async S3 downloader

Key features:
- Parametrized file counts and worker counts
- Comprehensive performance metrics
- Easy customization via command line or pytest parameters

Usage:
    # Run all parametrized tests
    pytest tests/performance/test_vs_s5cmd.py -v

    # Run specific test with custom parameters
    python tests/performance/test_vs_s5cmd.py 100 32

    # Run quick test with pytest
    pytest tests/performance/test_vs_s5cmd.py::test_s5cmd_vs_robinzhon_quick_check -v
"""

import csv
import os
import subprocess
import tempfile
import time
import shutil
from pathlib import Path
from typing import List, Tuple

import pytest
import robinzhon


class S5cmdDownloader:
    """S5cmd implementation using subprocess for comparison."""

    def __init__(self, max_workers: int = 20):
        self.max_workers = max_workers
        # Check if s5cmd is available
        if not shutil.which("s5cmd"):
            raise RuntimeError(
                "s5cmd not found in PATH. Please install s5cmd first."
            )

    def download_multiple_files_with_paths(
        self, bucket_name: str, downloads: List[Tuple[str, str]]
    ) -> dict:
        """Download multiple files using s5cmd with a batch file."""
        successful = []
        failed = []
        all_target_paths = [local_path for _, local_path in downloads]

        # Create directories for all target paths
        for _, local_path in downloads:
            os.makedirs(os.path.dirname(local_path), exist_ok=True)

        # Create a temporary batch file for s5cmd
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".txt", delete=False
        ) as batch_file:
            batch_filename = batch_file.name
            for object_key, local_path in downloads:
                # s5cmd batch command format: cp "s3://bucket/key" "/local/path"
                # Quote both source and destination to handle spaces and special characters
                batch_file.write(
                    f'cp "s3://{bucket_name}/{object_key}" "{local_path}"\n'
                )

        try:
            # Run s5cmd with the batch file
            cmd = [
                "s5cmd",
                "--numworkers",
                str(self.max_workers),
                "--json",  # Get JSON output for easier parsing
                "run",
                batch_filename,
            ]

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=300,  # 5 minute timeout
            )

            # Parse s5cmd output to determine success/failure
            # s5cmd can have partial successes even with non-zero return code
            # Check which files actually exist and have content regardless of return code
            for _, local_path in downloads:
                if (
                    os.path.isfile(local_path)
                    and os.path.getsize(local_path) > 0
                ):
                    successful.append(local_path)
                else:
                    failed.append(local_path)

            # Log any errors for debugging
            if result.returncode != 0 and result.stderr:
                print(f"s5cmd stderr: {result.stderr.strip()}")
            if result.stdout and "--json" in cmd:
                # Parse JSON output for detailed error information if needed
                for line in result.stdout.strip().split("\n"):
                    if line.strip() and '"error"' in line:
                        print(f"s5cmd error detail: {line.strip()}")

        except subprocess.TimeoutExpired:
            print("s5cmd timed out")
            failed = [local_path for _, local_path in downloads]
        except Exception as e:
            print(f"s5cmd execution error: {e}")
            failed = [local_path for _, local_path in downloads]
        finally:
            try:
                os.unlink(batch_filename)
            except OSError:
                pass

        actual_files_count = self._count_existing_files(all_target_paths)
        strict_success_rate = (
            actual_files_count / len(downloads) if downloads else 0
        )

        return {
            "successful": successful,
            "failed": failed,
            "total": len(downloads),
            "success_rate": len(successful) / len(downloads)
            if downloads
            else 0,
            "strict_success_rate": strict_success_rate,
            "actual_files_count": actual_files_count,
        }

    def _count_existing_files(self, file_paths: List[str]) -> int:
        """Count how many files actually exist on disk with non-zero size."""
        return sum(
            1
            for path in file_paths
            if os.path.isfile(path) and os.path.getsize(path) > 0
        )


class PerformanceMetrics:
    """Simple class to capture and display performance metrics."""

    def __init__(self, name: str):
        self.name = name
        self.start_time = None
        self.end_time = None
        self.duration = None
        self.results = None
        self.verification_data = None

    def start(self):
        self.start_time = time.time()

    def end(self, results):
        self.end_time = time.time()
        self.duration = self.end_time - self.start_time
        self.results = results

    def set_verification_data(self, actual_count: int, strict_rate: float):
        """Set verification data calculated outside of timing."""
        self.verification_data = {
            "actual_files_count": actual_count,
            "strict_success_rate": strict_rate,
        }

    def throughput(self) -> float:
        """Files per second."""
        if self.duration and self.results:
            total_files = getattr(
                self.results,
                "total_count",
                lambda: self.results.get("total", 0),
            )()
            return total_files / self.duration
        return 0

    def success_rate(self) -> float:
        """Success rate as percentage."""
        if hasattr(self.results, "success_rate"):
            return self.results.success_rate() * 100
        elif isinstance(self.results, dict):
            return self.results.get("success_rate", 0) * 100
        return 0

    def strict_success_rate(self) -> float:
        """Strict success rate as percentage (based on actual file counts)."""
        if self.verification_data:
            return self.verification_data["strict_success_rate"] * 100
        elif (
            isinstance(self.results, dict)
            and "strict_success_rate" in self.results
        ):
            return self.results.get("strict_success_rate", 0) * 100
        return self.success_rate()

    def successful_count(self) -> int:
        """Number of successful downloads."""
        if hasattr(self.results, "successful"):
            return len(self.results.successful)
        elif isinstance(self.results, dict):
            return len(self.results.get("successful", []))
        return 0

    def actual_files_count(self) -> int:
        """Number of files that actually exist on disk."""
        if self.verification_data:
            return self.verification_data["actual_files_count"]
        elif (
            isinstance(self.results, dict)
            and "actual_files_count" in self.results
        ):
            return self.results.get("actual_files_count", 0)
        return self.successful_count()


def load_test_data(csv_file: str, limit: int = 50) -> List[Tuple[str, str]]:
    """Load test data from CSV file, limiting to first N entries for manageable testing."""
    downloads = []
    csv_path = Path(__file__).parent / csv_file

    if not csv_path.exists():
        return []

    with open(csv_path, "r") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if i >= limit:
                break
            bucket_name = row["BUCKET_NAME"]
            object_key = row["IMAGE_PATH"]
            downloads.append((bucket_name, object_key))

    return downloads


def create_download_paths(
    downloads: List[Tuple[str, str]], base_dir: str
) -> List[Tuple[str, str]]:
    """Create local file paths for downloads."""
    return [
        (object_key, os.path.join(base_dir, f"file_{i:03d}.jpg"))
        for i, (bucket_name, object_key) in enumerate(downloads)
    ]


@pytest.mark.performance
@pytest.mark.parametrize(
    "file_count,max_workers",
    [
        (10, 8),
        (50, 16),
        (100, 20),
        (200, 32),
        (500, 40),
        (1000, 50),
    ],
)
def test_s5cmd_vs_robinzhon_performance(file_count, max_workers):
    """
    Compare performance between s5cmd and robinzhon.

    Compares:
    - s5cmd (Go-based CLI tool with parallel S3 operations)
    - robinzhon (Rust-based async implementation)

    Tests different file counts and worker counts to see how performance scales.
    """
    print(f"\n{'=' * 60}")
    print(
        f"s5cmd vs robinzhon Performance Test: {file_count} files, {max_workers} workers"
    )
    print(f"{'=' * 60}")

    test_data = load_test_data("objects_and_keys.csv", limit=file_count)
    if not test_data:
        pytest.skip("No test data available")

    bucket_name = test_data[0][0]

    with (
        tempfile.TemporaryDirectory(prefix="s5cmd_test_") as s5cmd_dir,
        tempfile.TemporaryDirectory(prefix="robinzhon_test_") as rust_dir,
    ):
        s5cmd_downloads = create_download_paths(test_data, s5cmd_dir)
        rust_downloads = create_download_paths(test_data, rust_dir)

        # Test s5cmd first
        print("\nTesting s5cmd implementation...")
        s5cmd_metrics = PerformanceMetrics("s5cmd")
        s5cmd_metrics.start()

        try:
            s5cmd_downloader = S5cmdDownloader(max_workers=max_workers)
            s5cmd_results = s5cmd_downloader.download_multiple_files_with_paths(
                bucket_name, s5cmd_downloads
            )
            s5cmd_metrics.end(s5cmd_results)
            print(f"Completed in {s5cmd_metrics.duration:.2f}s")
        except Exception as e:
            print(f"Failed: {e}")
            pytest.skip(f"s5cmd test failed: {e}")

        # Test robinzhon
        print("\nTesting robinzhon implementation...")
        rust_metrics = PerformanceMetrics("robinzhon")
        rust_metrics.start()

        try:
            rust_downloader = robinzhon.S3Downloader("us-east-1", max_workers)
            rust_results = rust_downloader.download_multiple_files_with_paths(
                bucket_name, rust_downloads
            )
            rust_metrics.end(rust_results)
            print(f"Download completed in {rust_metrics.duration:.2f}s")

            # Verify robinzhon downloads
            print("Verifying robinzhon downloads...")
            rust_target_paths = [local_path for _, local_path in rust_downloads]
            rust_actual_count = s5cmd_downloader._count_existing_files(
                rust_target_paths
            )

            if hasattr(rust_results, "total_count"):
                total = rust_results.total_count()
            else:
                total = len(rust_downloads)
            rust_strict_rate = rust_actual_count / total if total else 0
            rust_metrics.set_verification_data(
                rust_actual_count, rust_strict_rate
            )

        except Exception as e:
            print(f"Failed: {e}")
            pytest.skip(f"robinzhon test failed: {e}")

        # Display results
        print(
            f"\nPerformance Results ({file_count} files, {max_workers} workers)"
        )
        print(f"{'=' * 75}")
        print(f"{'Metric':<25} {'s5cmd':<15} {'robinzhon':<15} {'Winner'}")
        print(f"{'=' * 75}")

        # Duration comparison
        durations = {
            "s5cmd": s5cmd_metrics.duration,
            "robinzhon": rust_metrics.duration,
        }
        duration_winner = min(durations, key=durations.get)

        print(
            f"{'Duration (seconds)':<25} {s5cmd_metrics.duration:<15.2f} {rust_metrics.duration:<15.2f} {duration_winner}"
        )

        # Throughput comparison
        s5cmd_throughput = s5cmd_metrics.throughput()
        rust_throughput = rust_metrics.throughput()
        throughputs = {
            "s5cmd": s5cmd_throughput,
            "robinzhon": rust_throughput,
        }
        throughput_winner = max(throughputs, key=throughputs.get)

        print(
            f"{'Throughput (files/sec)':<25} {s5cmd_throughput:<15.1f} {rust_throughput:<15.1f} {throughput_winner}"
        )

        # Success rate comparison
        s5cmd_success = s5cmd_metrics.success_rate()
        rust_success = rust_metrics.success_rate()
        success_rates = {
            "s5cmd": s5cmd_success,
            "robinzhon": rust_success,
        }
        success_winner = max(success_rates, key=success_rates.get)

        print(
            f"{'Success Rate (%)':<25} {s5cmd_success:<15.1f} {rust_success:<15.1f} {success_winner}"
        )

        # Files downloaded
        s5cmd_files = s5cmd_metrics.successful_count()
        rust_files = rust_metrics.successful_count()

        print(f"{'Files Downloaded':<25} {s5cmd_files:<15} {rust_files:<15}")

        print(f"{'=' * 75}")

        # Performance summary
        print("\nPerformance Summary:")
        if duration_winner == "robinzhon":
            speedup = s5cmd_metrics.duration / rust_metrics.duration
            print(f"robinzhon is {speedup:.1f}x faster than s5cmd")
        elif duration_winner == "s5cmd":
            speedup = rust_metrics.duration / s5cmd_metrics.duration
            print(f"s5cmd is {speedup:.1f}x faster than robinzhon")
        else:
            print("Performance is comparable")

        if throughput_winner == "robinzhon":
            throughput_ratio = rust_throughput / s5cmd_throughput
            print(
                f"robinzhon has {throughput_ratio:.1f}x higher throughput than s5cmd"
            )
        elif throughput_winner == "s5cmd":
            throughput_ratio = s5cmd_throughput / rust_throughput
            print(
                f"s5cmd has {throughput_ratio:.1f}x higher throughput than robinzhon"
            )


@pytest.mark.performance
@pytest.mark.parametrize(
    "file_count,max_workers",
    [
        (5, 8),
        (5, 16),
    ],
)
def test_s5cmd_vs_robinzhon_quick_check(file_count, max_workers):
    """
    Quick performance check with just a few files for development/CI.
    Compares s5cmd against robinzhon implementation with different worker counts.
    """
    print(f"\n{'=' * 60}")
    print(
        f"s5cmd vs robinzhon Quick Performance Test: {file_count} files, {max_workers} workers"
    )
    print(f"{'=' * 60}")

    test_data = load_test_data("objects_and_keys.csv", limit=file_count)
    if not test_data:
        print("No test data available")
        return

    bucket_name = test_data[0][0]

    with (
        tempfile.TemporaryDirectory(prefix="s5cmd_test_") as s5cmd_dir,
        tempfile.TemporaryDirectory(prefix="robinzhon_test_") as rust_dir,
    ):
        s5cmd_downloads = create_download_paths(test_data, s5cmd_dir)
        rust_downloads = create_download_paths(test_data, rust_dir)

        # Test s5cmd
        print("\nTesting s5cmd implementation...")
        s5cmd_metrics = PerformanceMetrics("s5cmd")
        s5cmd_metrics.start()

        try:
            s5cmd_downloader = S5cmdDownloader(max_workers=max_workers)
            s5cmd_results = s5cmd_downloader.download_multiple_files_with_paths(
                bucket_name, s5cmd_downloads
            )
            s5cmd_metrics.end(s5cmd_results)
            print(f"Completed in {s5cmd_metrics.duration:.2f}s")
        except Exception as e:
            print(f"s5cmd failed: {e}")
            return

        # Test robinzhon
        print("\nInitializing robinzhon downloader...")
        try:
            rust_downloader = robinzhon.S3Downloader("us-east-1", max_workers)
        except Exception as e:
            print(f"Failed to initialize robinzhon downloader: {e}")
            return

        print("\nTesting robinzhon implementation...")
        rust_metrics = PerformanceMetrics("robinzhon")
        rust_metrics.start()

        try:
            rust_results = rust_downloader.download_multiple_files_with_paths(
                bucket_name, rust_downloads
            )
            rust_metrics.end(rust_results)
            print(f"Download completed in {rust_metrics.duration:.2f}s")
        except Exception as e:
            print(f"robinzhon failed: {e}")
            return

        # Display results
        print(
            f"\nPerformance Results ({file_count} files, {max_workers} workers)"
        )
        print(f"{'=' * 75}")
        print(f"{'Metric':<25} {'s5cmd':<15} {'robinzhon':<15} {'Winner'}")
        print(f"{'=' * 75}")

        durations = {
            "s5cmd": s5cmd_metrics.duration,
            "robinzhon": rust_metrics.duration,
        }
        duration_winner = min(durations, key=durations.get)

        print(
            f"{'Duration (seconds)':<25} {s5cmd_metrics.duration:<15.2f} {rust_metrics.duration:<15.2f} {duration_winner}"
        )

        s5cmd_throughput = s5cmd_metrics.throughput()
        rust_throughput = rust_metrics.throughput()
        throughputs = {
            "s5cmd": s5cmd_throughput,
            "robinzhon": rust_throughput,
        }
        throughput_winner = max(throughputs, key=throughputs.get)

        print(
            f"{'Throughput (files/sec)':<25} {s5cmd_throughput:<15.1f} {rust_throughput:<15.1f} {throughput_winner}"
        )

        s5cmd_success = s5cmd_metrics.success_rate()
        rust_success = rust_metrics.success_rate()
        success_rates = {
            "s5cmd": s5cmd_success,
            "robinzhon": rust_success,
        }
        success_winner = max(success_rates, key=success_rates.get)

        print(
            f"{'Success Rate (%)':<25} {s5cmd_success:<15.1f} {rust_success:<15.1f} {success_winner}"
        )

        s5cmd_files = s5cmd_metrics.successful_count()
        rust_files = rust_metrics.successful_count()

        print(f"{'Files Downloaded':<25} {s5cmd_files:<15} {rust_files:<15}")

        print(f"{'=' * 75}")

        print("\nPerformance Summary:")
        if duration_winner == "robinzhon":
            speedup = s5cmd_metrics.duration / rust_metrics.duration
            print(f"robinzhon is {speedup:.1f}x faster than s5cmd")
        elif duration_winner == "s5cmd":
            speedup = rust_metrics.duration / s5cmd_metrics.duration
            print(f"s5cmd is {speedup:.1f}x faster than robinzhon")
        else:
            print("Performance is comparable")


def run_custom_benchmark(file_count: int = 5, max_workers: int = 8):
    """
    Run a custom benchmark with specified parameters.

    Args:
        file_count: Number of files to download
        max_workers: Number of workers to use
    """
    test_s5cmd_vs_robinzhon_quick_check(
        file_count=file_count, max_workers=max_workers
    )


if __name__ == "__main__":
    import sys

    # Allow running with custom parameters from command line
    if len(sys.argv) >= 3:
        file_count = int(sys.argv[1])
        max_workers = int(sys.argv[2])
        print(
            f"Running custom benchmark: {file_count} files, {max_workers} workers"
        )
        run_custom_benchmark(file_count=file_count, max_workers=max_workers)
    else:
        print("Running default quick benchmark: 5 files, 8 workers")
        print("Usage: python test_vs_s5cmd.py <file_count> <max_workers>")
        run_custom_benchmark()
