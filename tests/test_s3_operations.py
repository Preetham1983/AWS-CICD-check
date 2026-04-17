from unittest.mock import MagicMock

from botocore.exceptions import ClientError

from s3_service.s3_operations import S3Operations


def test_create_bucket_success() -> None:
    client = MagicMock()
    operations = S3Operations(client)

    assert operations.create_bucket("demo-bucket") is True
    client.create_bucket.assert_called_once_with(Bucket="demo-bucket")


def test_create_bucket_already_exists_is_treated_as_success() -> None:
    client = MagicMock()
    client.create_bucket.side_effect = ClientError(
        {"Error": {"Code": "BucketAlreadyExists", "Message": "exists"}},
        "CreateBucket",
    )
    operations = S3Operations(client)

    assert operations.create_bucket("demo-bucket") is True


def test_create_bucket_other_client_error_returns_false() -> None:
    client = MagicMock()
    client.create_bucket.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "denied"}},
        "CreateBucket",
    )
    operations = S3Operations(client)

    assert operations.create_bucket("demo-bucket") is False


def test_list_buckets_returns_bucket_names() -> None:
    client = MagicMock()
    client.list_buckets.return_value = {
        "Buckets": [{"Name": "bucket-a"}, {"Name": "bucket-b"}],
    }
    operations = S3Operations(client)

    assert operations.list_buckets() == ["bucket-a", "bucket-b"]


def test_upload_download_list_and_delete_object() -> None:
    client = MagicMock()
    client.get_object.return_value = {"Body": MagicMock(read=MagicMock(return_value=b"hello"))}
    client.list_objects_v2.return_value = {"Contents": [{"Key": "file.txt", "Size": 5}]}
    operations = S3Operations(client)

    assert operations.upload_object("bucket", "file.txt", b"hello") is True
    assert operations.download_object("bucket", "file.txt") == b"hello"
    assert operations.list_objects("bucket") == [{"Key": "file.txt", "Size": 5}]
    assert operations.delete_object("bucket", "file.txt") is True
