from robinzhon import S3Downloader, DownloadResults


def test_basic_download(mocker):
    """Test basic single file download functionality."""

    mock_download = mocker.patch("robinzhon.S3Downloader.download_file")
    mock_download.return_value = "./test-object-key"

    client = S3Downloader("us-east-1")
    download = client.download_file(
        "test-bucket", "test-object-key", "./test-object-key"
    )

    mock_download.assert_called_once_with(
        "test-bucket", "test-object-key", "./test-object-key"
    )

    assert download == "./test-object-key"


def test_multiple_downloads(mocker):
    """Test multiple files download functionality."""

    mock_results = DownloadResults(
        successful=[
            "./object/located/on/different/path",
            "./object/located/on/another/path",
        ],
        failed=[],
    )

    mock_download_multiple = mocker.patch(
        "robinzhon.S3Downloader.download_multiple_files"
    )
    mock_download_multiple.return_value = mock_results

    client = S3Downloader("us-east-1")
    results = client.download_multiple_files(
        "test-bucket",
        [
            "object/located/on/different/path",
            "object/located/on/another/path",
        ],
        "./",
    )

    mock_download_multiple.assert_called_once_with(
        "test-bucket",
        [
            "object/located/on/different/path",
            "object/located/on/another/path",
        ],
        "./",
    )

    assert results is not None
    assert results.is_complete_success()
    assert results.has_success()
    assert not results.has_failures()
    assert len(results.successful) == 2
    assert len(results.failed) == 0
    assert results.success_rate() == 1.0


def test_download_with_failures(mocker):
    """Test multiple downloads with some failures."""

    mock_results = DownloadResults(
        successful=["./success-file.txt"], failed=["failed-object-key"]
    )

    mock_download_multiple = mocker.patch(
        "robinzhon.S3Downloader.download_multiple_files"
    )
    mock_download_multiple.return_value = mock_results

    client = S3Downloader("us-east-1")
    results = client.download_multiple_files(
        "test-bucket", ["success-object", "failed-object-key"], "./"
    )

    assert results is not None
    assert not results.is_complete_success()
    assert results.has_success()
    assert results.has_failures()
    assert len(results.successful) == 1
    assert len(results.failed) == 1
    assert results.success_rate() == 0.5
    assert results.total_count() == 2


def test_download_failure(mocker):
    """Test handling of download failures."""

    mock_download = mocker.patch("robinzhon.S3Downloader.download_file")
    mock_download.side_effect = RuntimeError("S3 connection failed")

    client = S3Downloader("us-east-1")

    try:
        client.download_file(
            "test-bucket", "test-object-key", "./test-object-key"
        )
        assert False, "Expected exception was not raised"
    except RuntimeError as e:
        assert "S3 connection failed" in str(e)

    mock_download.assert_called_once()


def test_download_results_functionality():
    """Test DownloadResults class functionality without mocking."""

    success_results = DownloadResults(
        successful=["file1.txt", "file2.txt"], failed=[]
    )

    assert success_results.is_complete_success()
    assert success_results.has_success()
    assert not success_results.has_failures()
    assert success_results.total_count() == 2
    assert success_results.success_rate() == 1.0

    mixed_results = DownloadResults(
        successful=["file1.txt"], failed=["failed_file.txt"]
    )

    assert not mixed_results.is_complete_success()
    assert mixed_results.has_success()
    assert mixed_results.has_failures()
    assert mixed_results.total_count() == 2
    assert mixed_results.success_rate() == 0.5

    failure_results = DownloadResults(
        successful=[], failed=["failed1.txt", "failed2.txt"]
    )

    assert not failure_results.is_complete_success()
    assert not failure_results.has_success()
    assert failure_results.has_failures()
    assert failure_results.total_count() == 2
    assert failure_results.success_rate() == 0.0
