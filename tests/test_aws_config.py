from unittest.mock import patch

from connection.aws_config import AwsConfig


def test_get_s3_client_is_cached() -> None:
    config = AwsConfig()

    with patch("connection.aws_config.boto3.client") as mock_client:
        first = config.get_s3_client()
        second = config.get_s3_client()

    mock_client.assert_called_once_with(
        "s3",
        endpoint_url="http://localhost:4566",
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    assert first is second


def test_get_dynamodb_client_is_cached() -> None:
    config = AwsConfig()

    with patch("connection.aws_config.boto3.client") as mock_client:
        first = config.get_dynamodb_client()
        second = config.get_dynamodb_client()

    mock_client.assert_called_once_with(
        "dynamodb",
        endpoint_url="http://localhost:4566",
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    assert first is second
