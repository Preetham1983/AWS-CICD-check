"""AWS configuration and client factory."""

import boto3


class AwsConfig:
    """Manages AWS client configuration for multiple services."""

    def __init__(
        self,
        endpoint_url: str = "http://localhost:4566",
        region_name: str = "us-east-1",
        access_key_id: str = "test",
        secret_access_key: str = "test",
    ):
        """Initialize AWS configuration.

        Args:
            endpoint_url: LocalStack or AWS endpoint
            region_name: AWS region
            access_key_id: AWS access key
            secret_access_key: AWS secret key
        """
        self.endpoint_url = endpoint_url
        self.region_name = region_name
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key
        self._s3_client: object | None = None
        self._dynamodb_client: object | None = None

    def _make_client(self, service: str) -> object:
        """Create a boto3 client for the given AWS service."""
        return boto3.client(
            service,
            endpoint_url=self.endpoint_url,
            region_name=self.region_name,
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key,
        )

    def get_s3_client(self):
        """Get or create S3 client (singleton pattern)."""
        if self._s3_client is None:
            self._s3_client = self._make_client("s3")
        return self._s3_client

    def get_dynamodb_client(self):
        """Get or create DynamoDB client (singleton pattern)."""
        if self._dynamodb_client is None:
            self._dynamodb_client = self._make_client("dynamodb")
        return self._dynamodb_client
