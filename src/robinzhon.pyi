from typing import List, Tuple

class DownloadResults:
    """
    Results of a batch download operation.

    This class contains the results of downloading multiple files, with separate
    lists for successful and failed downloads, plus utility methods for checking
    the overall status.

    Attributes:
        successful: List of local file paths where files were successfully downloaded
        failed: List of S3 object keys that failed to download
    """

    successful: List[str]
    failed: List[str]

    def __init__(self, successful: List[str], failed: List[str]) -> None:
        """
        Initialize download results.

        Args:
            successful: List of successfully downloaded file paths
            failed: List of failed S3 object keys
        """
        ...

    def is_complete_success(self) -> bool:
        """
        Check if all downloads were successful.

        Returns:
            True if no downloads failed, False otherwise

        Example:
            >>> if result.is_complete_success():
            ...     print("All files downloaded successfully!")
        """
        ...

    def has_success(self) -> bool:
        """
        Check if there were any successful downloads.

        Returns:
            True if at least one download succeeded, False otherwise
        """
        ...

    def has_failures(self) -> bool:
        """
        Check if there were any failed downloads.

        Returns:
            True if at least one download failed, False otherwise

        Example:
            >>> if result.has_failures():
            ...     print(f"Need to retry {len(result.failed)} files")
        """
        ...

    def total_count(self) -> int:
        """
        Get the total number of download attempts.

        Returns:
            Total number of files attempted (successful + failed)
        """
        ...

    def success_rate(self) -> float:
        """
        Calculate the success rate of the downloads.

        Returns:
            Success rate as a float between 0.0 and 1.0

        Example:
            >>> rate = result.success_rate()
            >>> print(f"Success rate: {rate:.1%}")
        """
        ...

    def __str__(self) -> str:
        """String representation showing counts."""
        ...

    def __repr__(self) -> str:
        """Developer representation showing counts."""
        ...

class S3Config:
    """
    Internal AWS S3 configuration class.

    This class is used internally by S3Downloader to manage AWS S3 client configuration.
    You typically don't need to interact with this class directly.
    """

    ...

class S3Downloader:
    """
    High-performance AWS S3 file downloader with concurrent download capabilities.

    This class provides methods to download files from AWS S3 buckets, with support
    for both single file downloads and concurrent batch downloads using async I/O
    for optimal network utilization and performance.

    Example:
        >>> downloader = S3Downloader("us-east-1")
        >>> downloader.download_file("my-bucket", "path/to/file.txt", "./local-file.txt")
        './local-file.txt'
    """

    def __init__(
        self, region_name: str, max_concurrent_downloads: int = 5
    ) -> None:
        """
        Initialize the S3 downloader with the specified AWS region.

        Args:
            region_name: AWS region name (e.g., "us-east-1", "eu-west-1")
            max_concurrent_downloads: Maximum concurrent downloads from S3. Default = 5

        Example:
            >>> downloader = S3Downloader("us-east-1")
        """
        ...

    def download_file(
        self, bucket_name: str, object_key: str, path_to_store: str
    ) -> str:
        """
        Download a single file from S3 to the local filesystem.

        Args:
            bucket_name: Name of the S3 bucket
            object_key: S3 object key (path to the file in the bucket)
            path_to_store: Local file path where the downloaded file will be stored

        Returns:
            The local file path where the file was stored

        Raises:
            RuntimeError: If the download fails due to network issues, permissions,
                         or file system errors

        Example:
            >>> downloader = S3Downloader("us-east-1")
            >>> result = downloader.download_file(
            ...     "my-bucket",
            ...     "data/file.csv",
            ...     "./downloads/file.csv"
            ... )
            >>> print(result)
            './downloads/file.csv'
        """
        ...

    def download_multiple_files(
        self, bucket_name: str, object_keys: List[str], base_directory: str
    ) -> DownloadResults:
        """
        Download multiple files from S3 concurrently to a base directory.

        Files are downloaded concurrently using async I/O for optimal network utilization
        and significantly improved performance compared to sequential downloads. This approach
        is highly efficient for I/O-bound operations like file downloads.

        **Resilient Error Handling**: If some files fail to download, the operation continues
        and returns both successful and failed downloads for programmatic handling.

        Args:
            bucket_name: Name of the S3 bucket
            object_keys: List of S3 object keys to download
            base_directory: Local directory where all files will be stored

        Returns:
            DownloadResults object with successful and failed download lists

        Raises:
            RuntimeError: If the base directory cannot be created or other system errors

        Note:
            Performance scales with network bandwidth and connection limits rather than
            CPU cores, making this highly efficient for downloading many files.
            Failed downloads are logged as warnings and available in result.failed.

        Example:
            >>> downloader = S3Downloader("us-east-1")
            >>> files = [
            ...     "data/file1.csv",          # exists
            ...     "data/missing-file.json",  # doesn't exist
            ...     "logs/app.log"            # exists
            ... ]
            >>> result = downloader.download_multiple_files(
            ...     "my-bucket",
            ...     files,
            ...     "./downloads"
            ... )
            # Warning: 1 downloads failed:
            #   data/missing-file.json: S3 object not found
            >>> print(f"Success: {len(result.successful)}, Failed: {len(result.failed)}")
            Success: 2, Failed: 1
            >>> if result.has_failures():
            ...     print(f"Failed files: {result.failed}")
            Failed files: ['data/missing-file.json']
            >>> for path in result.successful:
            ...     print(f"Downloaded: {path}")
        """
        ...

    def download_multiple_files_with_paths(
        self, bucket_name: str, downloads: List[Tuple[str, str]]
    ) -> DownloadResults:
        """
        Download multiple files from S3 concurrently with custom local paths.

        This method allows you to specify custom local paths for each downloaded file,
        providing full control over the destination file structure. Downloads are
        executed concurrently using async I/O for maximum performance.

        **Resilient Error Handling**: If some files fail to download, the operation continues
        and returns both successful and failed downloads for programmatic handling.

        Args:
            bucket_name: Name of the S3 bucket
            downloads: List of tuples where each tuple contains:
                      (object_key, local_path) - S3 key and desired local file path

        Returns:
            DownloadResults object with successful and failed download lists

        Raises:
            RuntimeError: If directories cannot be created or other system errors

        Note:
            Directories are automatically created as needed for each file path.
            Performance is optimized for I/O-bound operations with concurrent execution.
            Failed downloads are logged as warnings and available in result.failed.

        Example:
            >>> downloader = S3Downloader("us-east-1")
            >>> downloads = [
            ...     ("data/input.csv", "./processed/input_data.csv"),        # exists
            ...     ("config/missing.json", "./config/app_settings.json"),  # doesn't exist
            ...     ("logs/2024/app.log", "./logs/application.log")         # exists
            ... ]
            >>> result = downloader.download_multiple_files_with_paths(
            ...     "my-bucket",
            ...     downloads
            ... )
            # Warning: 1 downloads failed:
            #   config/missing.json: S3 object not found
            >>> print(f"Success rate: {result.success_rate():.1%}")
            Success rate: 66.7%
            >>> if not result.is_complete_success():
            ...     print("Some downloads failed, checking what to retry...")
            ...     for failed_key in result.failed:
            ...         print(f"  Retry: {failed_key}")
        """
        ...
