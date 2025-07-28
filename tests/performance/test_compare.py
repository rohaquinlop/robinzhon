import csv
import os
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List, Tuple

import boto3
import pytest
import robinzhon
from boto3.s3.transfer import S3Transfer


class PythonS3Downloader:
    """Python implementation using S3Transfer with ThreadPoolExecutor for comparison."""

    def __init__(self, region_name: str, max_workers: int = 8):
        self.region_name = region_name
        self.max_workers = max_workers
        self.s3_client = boto3.client("s3", region_name=region_name)
        self.transfer = S3Transfer(self.s3_client)

    def download_multiple_files_with_paths(
        self, bucket_name: str, downloads: List[Tuple[str, str]]
    ) -> dict:
        """Download multiple files using ThreadPoolExecutor for concurrent downloads."""
        successful = []
        failed = []

        def download_single(
            object_key: str, local_path: str
        ) -> Tuple[bool, str]:
            try:
                os.makedirs(os.path.dirname(local_path), exist_ok=True)

                self.transfer.download_file(bucket_name, object_key, local_path)
                return True, local_path
            except Exception:
                return False, object_key

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_download = {
                executor.submit(download_single, obj_key, local_path): (
                    obj_key,
                    local_path,
                )
                for obj_key, local_path in downloads
            }

            for future in as_completed(future_to_download):
                obj_key, local_path = future_to_download[future]
                try:
                    success, result = future.result()
                    if success:
                        successful.append(result)
                    else:
                        failed.append(result)
                except Exception:
                    failed.append(obj_key)

        return {
            "successful": successful,
            "failed": failed,
            "total": len(downloads),
            "success_rate": len(successful) / len(downloads)
            if downloads
            else 0,
        }

    def verify_downloads(
        self, results: dict, all_target_paths: List[str]
    ) -> dict:
        """Verify downloaded files exist and have non-zero size. Run this outside of timing."""
        actual_files_count = self._count_existing_files(all_target_paths)
        strict_success_rate = (
            actual_files_count / results["total"] if results["total"] else 0
        )

        results["strict_success_rate"] = strict_success_rate
        results["actual_files_count"] = actual_files_count
        return results

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
@pytest.mark.parametrize("file_count", [100, 500, 1000])
def test_performance_comparison(file_count):
    """
    Compare performance between robinzhon and Python S3Transfer implementations.

    Tests different file counts to see how performance scales.
    """
    print(f"\n{'=' * 60}")
    print(f"Performance Test: {file_count} files")
    print(f"{'=' * 60}")

    test_data = load_test_data("objects_and_keys.csv", limit=file_count)
    if not test_data:
        pytest.skip("No test data available")

    bucket_name = test_data[0][0]
    max_workers = 20

    with tempfile.TemporaryDirectory(
        prefix="robinzhon_test_"
    ) as rust_dir, tempfile.TemporaryDirectory(
        prefix="python_test_"
    ) as python_dir:
        rust_downloads = create_download_paths(test_data, rust_dir)
        python_downloads = create_download_paths(test_data, python_dir)

        print("\nInitializing downloaders...")
        try:
            python_downloader = PythonS3Downloader(
                "us-east-1", max_workers=max_workers
            )
            rust_downloader = robinzhon.S3Downloader("us-east-1", max_workers)
        except Exception as e:
            pytest.skip(f"Failed to initialize downloaders: {e}")

        print("\nTesting Python S3Transfer implementation...")
        python_metrics = PerformanceMetrics("Python S3Transfer")
        python_metrics.start()

        try:
            python_results = (
                python_downloader.download_multiple_files_with_paths(
                    bucket_name, python_downloads
                )
            )
            python_metrics.end(python_results)
            print(f"Download completed in {python_metrics.duration:.2f}s")

            print("Verifying Python downloads...")
            python_target_paths = [
                local_path for _, local_path in python_downloads
            ]
            python_results = python_downloader.verify_downloads(
                python_results, python_target_paths
            )
            python_metrics.set_verification_data(
                python_results["actual_files_count"],
                python_results["strict_success_rate"],
            )

        except Exception as e:
            print(f"Failed: {e}")
            pytest.skip(f"Python test failed: {e}")

        print("\nTesting robinzhon implementation...")
        rust_metrics = PerformanceMetrics("robinzhon")
        rust_metrics.start()

        try:
            rust_results = rust_downloader.download_multiple_files_with_paths(
                bucket_name, rust_downloads
            )
            rust_metrics.end(rust_results)
            print(f"Download completed in {rust_metrics.duration:.2f}s")

            print("Verifying robinzhon downloads...")
            rust_target_paths = [local_path for _, local_path in rust_downloads]
            rust_actual_count = python_downloader._count_existing_files(
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

        print(
            f"\nPerformance Results ({file_count} files, {max_workers} workers)"
        )
        print(f"{'─' * 60}")
        print(f"{'Metric':<25} {'robinzhon':<15} {'Python':<15} {'Winner'}")
        print(f"{'─' * 60}")

        duration_winner = (
            "robinzhon"
            if rust_metrics.duration < python_metrics.duration
            else "Python"
        )
        speedup = max(rust_metrics.duration, python_metrics.duration) / min(
            rust_metrics.duration, python_metrics.duration
        )
        print(
            f"{'Duration (seconds)':<25} {rust_metrics.duration:<15.2f} {python_metrics.duration:<15.2f} {duration_winner} ({speedup:.1f}x)"
        )

        rust_throughput = rust_metrics.throughput()
        python_throughput = python_metrics.throughput()
        throughput_winner = (
            "robinzhon" if rust_throughput > python_throughput else "Python"
        )
        print(
            f"{'Throughput (files/sec)':<25} {rust_throughput:<15.1f} {python_throughput:<15.1f} {throughput_winner}"
        )

        rust_success = rust_metrics.success_rate()
        python_success = python_metrics.success_rate()
        success_winner = (
            "robinzhon" if rust_success >= python_success else "Python"
        )
        print(
            f"{'Success Rate (%)':<25} {rust_success:<15.1f} {python_success:<15.1f} {success_winner}"
        )

        rust_strict = rust_metrics.strict_success_rate()
        python_strict = python_metrics.strict_success_rate()
        strict_winner = (
            "robinzhon" if rust_strict >= python_strict else "Python"
        )
        print(
            f"{'Strict Success Rate (%)':<25} {rust_strict:<15.1f} {python_strict:<15.1f} {strict_winner}"
        )

        rust_files = rust_metrics.successful_count()
        python_files = python_metrics.successful_count()
        print(f"{'Files Downloaded':<25} {rust_files:<15} {python_files:<15}")

        rust_actual = rust_metrics.actual_files_count()
        python_actual = python_metrics.actual_files_count()
        print(
            f"{'Actual Files on Disk':<25} {rust_actual:<15} {python_actual:<15}"
        )

        print(f"{'─' * 60}")

        if rust_metrics.duration < python_metrics.duration:
            speedup_factor = python_metrics.duration / rust_metrics.duration
            improvement_percent = (
                (python_metrics.duration - rust_metrics.duration)
                / python_metrics.duration
            ) * 100
            print(
                f"robinzhon is {improvement_percent:.1f}% faster ({speedup_factor:.2f}x speedup)"
            )
        elif rust_metrics.duration > python_metrics.duration:
            slowdown_factor = rust_metrics.duration / python_metrics.duration
            decline_percent = (
                (rust_metrics.duration - python_metrics.duration)
                / python_metrics.duration
            ) * 100
            print(
                f"robinzhon is {decline_percent:.1f}% slower ({slowdown_factor:.2f}x slower)"
            )
        else:
            print("Both implementations have identical performance")


@pytest.mark.performance
def test_quick_performance_check():
    """Quick performance check with just a few files for development/CI."""
    print(f"\n{'=' * 60}")
    print("Performance Test: 5 files")
    print(f"{'=' * 60}")

    test_data = load_test_data("objects_and_keys.csv", limit=5)
    if not test_data:
        print("No test data available")
        return

    bucket_name = test_data[0][0]
    max_workers = 8

    with tempfile.TemporaryDirectory(
        prefix="robinzhon_test_"
    ) as rust_dir, tempfile.TemporaryDirectory(
        prefix="python_test_"
    ) as python_dir:
        rust_downloads = create_download_paths(test_data, rust_dir)
        python_downloads = create_download_paths(test_data, python_dir)

        print("\nInitializing downloaders...")
        try:
            python_downloader = PythonS3Downloader(
                "us-east-1", max_workers=max_workers
            )
            rust_downloader = robinzhon.S3Downloader("us-east-1", max_workers)
        except Exception as e:
            print(f"Failed to initialize downloaders: {e}")
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
            print(f"Failed: {e}")
            return

        print("\nTesting Python S3Transfer implementation...")
        python_metrics = PerformanceMetrics("Python S3Transfer")
        python_metrics.start()

        try:
            python_results = (
                python_downloader.download_multiple_files_with_paths(
                    bucket_name, python_downloads
                )
            )
            python_metrics.end(python_results)
            print(f"Download completed in {python_metrics.duration:.2f}s")
        except Exception as e:
            print(f"Failed: {e}")
            return

        print("Verifying Python downloads...")
        python_target_paths = [local_path for _, local_path in python_downloads]
        python_results = python_downloader.verify_downloads(
            python_results, python_target_paths
        )
        python_metrics.set_verification_data(
            python_results["actual_files_count"],
            python_results["strict_success_rate"],
        )

        print("Verifying robinzhon downloads...")
        rust_target_paths = [local_path for _, local_path in rust_downloads]
        rust_actual_count = python_downloader._count_existing_files(
            rust_target_paths
        )
        rust_strict_rate = (
            rust_actual_count / len(rust_downloads) if rust_downloads else 0
        )
        rust_metrics.set_verification_data(rust_actual_count, rust_strict_rate)

        print(f"\nPerformance Results (5 files, {max_workers} workers)")
        print(f"{'─' * 60}")
        print(f"{'Metric':<25} {'robinzhon':<15} {'Python':<15} {'Winner'}")
        print(f"{'─' * 60}")

        duration_winner = (
            "robinzhon"
            if rust_metrics.duration < python_metrics.duration
            else "Python"
        )
        speedup = max(rust_metrics.duration, python_metrics.duration) / min(
            rust_metrics.duration, python_metrics.duration
        )
        print(
            f"{'Duration (seconds)':<25} {rust_metrics.duration:<15.2f} {python_metrics.duration:<15.2f} {duration_winner} ({speedup:.1f}x)"
        )

        rust_throughput = rust_metrics.throughput()
        python_throughput = python_metrics.throughput()
        throughput_winner = (
            "robinzhon" if rust_throughput > python_throughput else "Python"
        )
        print(
            f"{'Throughput (files/sec)':<25} {rust_throughput:<15.1f} {python_throughput:<15.1f} {throughput_winner}"
        )

        rust_success = rust_metrics.success_rate()
        python_success = python_metrics.success_rate()
        success_winner = (
            "robinzhon" if rust_success >= python_success else "Python"
        )
        print(
            f"{'Success Rate (%)':<25} {rust_success:<15.1f} {python_success:<15.1f} {success_winner}"
        )

        rust_strict = rust_metrics.strict_success_rate()
        python_strict = python_metrics.strict_success_rate()
        strict_winner = (
            "robinzhon" if rust_strict >= python_strict else "Python"
        )
        print(
            f"{'Strict Success Rate (%)':<25} {rust_strict:<15.1f} {python_strict:<15.1f} {strict_winner}"
        )

        rust_files = rust_metrics.successful_count()
        python_files = python_metrics.successful_count()
        print(f"{'Files Downloaded':<25} {rust_files:<15} {python_files:<15}")

        rust_actual = rust_metrics.actual_files_count()
        python_actual = python_metrics.actual_files_count()
        print(
            f"{'Actual Files on Disk':<25} {rust_actual:<15} {python_actual:<15}"
        )

        print(f"{'─' * 60}")

        if rust_metrics.duration < python_metrics.duration:
            speedup_factor = python_metrics.duration / rust_metrics.duration
            improvement_percent = (
                (python_metrics.duration - rust_metrics.duration)
                / python_metrics.duration
            ) * 100
            print(
                f"robinzhon is {improvement_percent:.1f}% faster ({speedup_factor:.2f}x speedup)"
            )
        elif rust_metrics.duration > python_metrics.duration:
            slowdown_factor = rust_metrics.duration / python_metrics.duration
            decline_percent = (
                (rust_metrics.duration - python_metrics.duration)
                / python_metrics.duration
            ) * 100
            print(
                f"robinzhon is {decline_percent:.1f}% slower ({slowdown_factor:.2f}x slower)"
            )
        else:
            print("Both implementations have identical performance")
