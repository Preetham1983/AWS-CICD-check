"""AWS S3 and DynamoDB operations with LocalStack."""
from connection import AwsConfig
from dynamodb_service import DynamoDBOperations
from s3_service import S3Operations


def main() -> None:
    """Main entry point for S3 and DynamoDB operations."""
    config = AwsConfig(
        endpoint_url="http://localhost:4566",
        region_name="us-east-1",
    )

    # --- S3 ---
    s3_ops = S3Operations(config.get_s3_client())

    bucket_name = "my-bucket1"
    s3_ops.create_bucket(bucket_name)

    print("\nAvailable Buckets:")
    for bucket in s3_ops.list_buckets():
        print(f"  - {bucket}")

    # --- DynamoDB ---
    dynamo_ops = DynamoDBOperations(config.get_dynamodb_client())

    table_name = "users"
    dynamo_ops.create_table(table_name, partition_key="id")

    dynamo_ops.put_item(table_name, {"id": {"S": "1"}, "name": {"S": "Alice"}, "age": {"N": "30"}})
    dynamo_ops.put_item(table_name, {"id": {"S": "2"}, "name": {"S": "Bob"}, "age": {"N": "25"}})

    print("\nGet item id=1:")
    item = dynamo_ops.get_item(table_name, {"id": {"S": "1"}})
    print(f"  {item}")

    print("\nAll items in table:")
    for row in dynamo_ops.scan(table_name):
        print(f"  {row}")

    print("\nAvailable Tables:")
    for t in dynamo_ops.list_tables():
        print(f"  - {t}")


if __name__ == "__main__":
    main()
