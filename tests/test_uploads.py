from robinzhon import S3Uploader, Results


def test_basic_upload(mocker):
    """Test basic single file upload functionality."""

    mock_upload = mocker.patch("robinzhon.S3Uploader.upload_file")
    mock_upload.return_value = "./local.txt"

    client = S3Uploader("us-east-1")
    upload = client.upload_file("test-bucket", "dest/key.txt", "./local.txt")

    mock_upload.assert_called_once_with(
        "test-bucket", "dest/key.txt", "./local.txt"
    )

    assert upload == "./local.txt"


def test_multiple_uploads(mocker):
    """Test multiple files upload functionality."""

    mock_results = Results(
        successful=["./local1.txt", "./local2.txt"], failed=[]
    )

    mock_upload_multiple = mocker.patch(
        "robinzhon.S3Uploader.upload_multiple_files"
    )
    mock_upload_multiple.return_value = mock_results

    client = S3Uploader("us-east-1")
    results = client.upload_multiple_files(
        "test-bucket", [("./local1.txt", "key1"), ("./local2.txt", "key2")]
    )

    mock_upload_multiple.assert_called_once_with(
        "test-bucket", [("./local1.txt", "key1"), ("./local2.txt", "key2")]
    )

    assert results is not None
    assert results.is_complete_success()
    assert results.has_success()
    assert not results.has_failures()
    assert len(results.successful) == 2
    assert len(results.failed) == 0
    assert results.success_rate() == 1.0


def test_upload_with_failures(mocker):
    """Test multiple uploads with some failures."""

    mock_results = Results(
        successful=["./local_ok.txt"], failed=["./local_failed.txt"]
    )

    mock_upload_multiple = mocker.patch(
        "robinzhon.S3Uploader.upload_multiple_files"
    )
    mock_upload_multiple.return_value = mock_results

    client = S3Uploader("us-east-1")
    results = client.upload_multiple_files(
        "test-bucket",
        [("./local_ok.txt", "okkey"), ("./local_failed.txt", "badkey")],
    )

    assert results is not None
    assert not results.is_complete_success()
    assert results.has_success()
    assert results.has_failures()
    assert len(results.successful) == 1
    assert len(results.failed) == 1
    assert results.success_rate() == 0.5
    assert results.total_count() == 2


def test_upload_failure(mocker):
    """Test handling of upload failures."""

    mock_upload = mocker.patch("robinzhon.S3Uploader.upload_file")
    mock_upload.side_effect = RuntimeError("S3 upload failed")

    client = S3Uploader("us-east-1")

    try:
        client.upload_file("test-bucket", "dest/key.txt", "./local.txt")
        assert False, "Expected exception was not raised"
    except RuntimeError as e:
        assert "S3 upload failed" in str(e)

    mock_upload.assert_called_once()
