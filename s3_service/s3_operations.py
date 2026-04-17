"""S3 operations service."""
from typing import Any

from botocore.exceptions import ClientError


class S3Operations:
    """Handles S3 bucket and object operations."""

    def __init__(self, s3_client: Any):
        """Initialize S3 operations.
        
        Args:
            s3_client: Boto3 S3 client instance
        """
        self.s3_client = s3_client

    def create_bucket(self, bucket_name: str) -> bool:
        """Create an S3 bucket.
        
        Args:
            bucket_name: Name of the bucket to create
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self.s3_client.create_bucket(Bucket=bucket_name)
            print(f"✓ Bucket '{bucket_name}' created successfully")
            return True
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code == "BucketAlreadyExists":
                print(f"ℹ Bucket '{bucket_name}' already exists")
                return True
            elif error_code == "BucketAlreadyOwnedByYou":
                print(f"ℹ Bucket '{bucket_name}' already owned by you")
                return True
            else:
                print(f"✗ Error creating bucket: {e}")
                return False
        except Exception as e:
            print(f"✗ Unexpected error creating bucket: {e}")
            return False

    def list_buckets(self) -> list[str]:
        """List all S3 buckets.
        
        Returns:
            List of bucket names
        """
        try:
            response = self.s3_client.list_buckets()
            buckets = [bucket["Name"] for bucket in response.get("Buckets", [])]
            return buckets
        except Exception as e:
            print(f"✗ Error listing buckets: {e}")
            return []

    def upload_object(self, bucket_name: str, key: str, body: bytes) -> bool:
        """Upload an object to S3.
        
        Args:
            bucket_name: Target bucket name
            key: Object key
            body: Object content
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self.s3_client.put_object(Bucket=bucket_name, Key=key, Body=body)
            print(f"✓ Object '{key}' uploaded to '{bucket_name}'")
            return True
        except Exception as e:
            print(f"✗ Error uploading object: {e}")
            return False

    def download_object(self, bucket_name: str, key: str) -> bytes | None:
        """Download an object from S3.
        
        Args:
            bucket_name: Source bucket name
            key: Object key
            
        Returns:
            Object content or None if error
        """
        try:
            response = self.s3_client.get_object(Bucket=bucket_name, Key=key)
            content = response["Body"].read()
            print(f"✓ Object '{key}' downloaded from '{bucket_name}'")
            return content
        except Exception as e:
            print(f"✗ Error downloading object: {e}")
            return None

    def list_objects(self, bucket_name: str) -> list[dict[str, Any]]:
        """List objects in a bucket.
        
        Args:
            bucket_name: Bucket name
            
        Returns:
            List of objects with metadata
        """
        try:
            response = self.s3_client.list_objects_v2(Bucket=bucket_name)
            objects = response.get("Contents", [])
            return objects
        except Exception as e:
            print(f"✗ Error listing objects: {e}")
            return []

    def delete_object(self, bucket_name: str, key: str) -> bool:
        """Delete an object from S3.
        
        Args:
            bucket_name: Bucket name
            key: Object key
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self.s3_client.delete_object(Bucket=bucket_name, Key=key)
            print(f"✓ Object '{key}' deleted from '{bucket_name}'")
            return True
        except Exception as e:
            print(f"✗ Error deleting object: {e}")
            return False
