"""DynamoDB operations service."""
from typing import Any

from botocore.exceptions import ClientError


class DynamoDBOperations:
    """Handles DynamoDB table and item operations."""

    def __init__(self, dynamodb_client: Any):
        """Initialize DynamoDB operations.

        Args:
            dynamodb_client: Boto3 DynamoDB client instance
        """
        self.dynamodb_client = dynamodb_client

    def create_table(
        self,
        table_name: str,
        partition_key: str,
        sort_key: str | None = None,
        billing_mode: str = "PAY_PER_REQUEST",
    ) -> bool:
        """Create a DynamoDB table.

        Args:
            table_name: Name of the table to create
            partition_key: Partition key attribute name (always a String)
            sort_key: Optional sort key attribute name (always a String)
            billing_mode: 'PAY_PER_REQUEST' or 'PROVISIONED'

        Returns:
            True if successful or table already exists, False otherwise
        """
        key_schema = [{"AttributeName": partition_key, "KeyType": "HASH"}]
        attribute_definitions = [{"AttributeName": partition_key, "AttributeType": "S"}]

        if sort_key:
            key_schema.append({"AttributeName": sort_key, "KeyType": "RANGE"})
            attribute_definitions.append({"AttributeName": sort_key, "AttributeType": "S"})

        try:
            self.dynamodb_client.create_table(
                TableName=table_name,
                KeySchema=key_schema,
                AttributeDefinitions=attribute_definitions,
                BillingMode=billing_mode,
            )
            print(f"✓ Table '{table_name}' created successfully")
            return True
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code == "ResourceInUseException":
                print(f"ℹ Table '{table_name}' already exists")
                return True
            print(f"✗ Error creating table: {e}")
            return False
        except Exception as e:
            print(f"✗ Unexpected error creating table: {e}")
            return False

    def delete_table(self, table_name: str) -> bool:
        """Delete a DynamoDB table.

        Args:
            table_name: Name of the table to delete

        Returns:
            True if successful, False otherwise
        """
        try:
            self.dynamodb_client.delete_table(TableName=table_name)
            print(f"✓ Table '{table_name}' deleted")
            return True
        except ClientError as e:
            print(f"✗ Error deleting table: {e}")
            return False

    def list_tables(self) -> list[str]:
        """List all DynamoDB tables.

        Returns:
            List of table names
        """
        try:
            response = self.dynamodb_client.list_tables()
            return response.get("TableNames", [])
        except Exception as e:
            print(f"✗ Error listing tables: {e}")
            return []

    def put_item(self, table_name: str, item: dict[str, Any]) -> bool:
        """Put an item into a DynamoDB table.

        Args:
            table_name: Target table name
            item: Item dict in DynamoDB attribute format, e.g.
                  {"id": {"S": "123"}, "name": {"S": "Alice"}}

        Returns:
            True if successful, False otherwise
        """
        try:
            self.dynamodb_client.put_item(TableName=table_name, Item=item)
            print(f"✓ Item written to '{table_name}'")
            return True
        except Exception as e:
            print(f"✗ Error putting item: {e}")
            return False

    def get_item(
        self, table_name: str, key: dict[str, Any]
    ) -> dict[str, Any] | None:
        """Get a single item from a DynamoDB table by its primary key.

        Args:
            table_name: Table name
            key: Primary key dict in DynamoDB attribute format, e.g.
                 {"id": {"S": "123"}}

        Returns:
            Item dict or None if not found / error
        """
        try:
            response = self.dynamodb_client.get_item(TableName=table_name, Key=key)
            item = response.get("Item")
            if item is None:
                print(f"ℹ Item not found in '{table_name}'")
            return item
        except Exception as e:
            print(f"✗ Error getting item: {e}")
            return None

    def delete_item(self, table_name: str, key: dict[str, Any]) -> bool:
        """Delete an item from a DynamoDB table.

        Args:
            table_name: Table name
            key: Primary key dict in DynamoDB attribute format

        Returns:
            True if successful, False otherwise
        """
        try:
            self.dynamodb_client.delete_item(TableName=table_name, Key=key)
            print(f"✓ Item deleted from '{table_name}'")
            return True
        except Exception as e:
            print(f"✗ Error deleting item: {e}")
            return False

    def scan(self, table_name: str) -> list[dict[str, Any]]:
        """Scan all items in a DynamoDB table.

        Args:
            table_name: Table name

        Returns:
            List of items
        """
        try:
            response = self.dynamodb_client.scan(TableName=table_name)
            return response.get("Items", [])
        except Exception as e:
            print(f"✗ Error scanning table: {e}")
            return []

    def query(
        self,
        table_name: str,
        key_condition_expression: str,
        expression_attribute_values: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """Query items in a DynamoDB table by key condition.

        Args:
            table_name: Table name
            key_condition_expression: Condition expression string,
                e.g. "id = :id_val"
            expression_attribute_values: Values dict, e.g.
                {":id_val": {"S": "123"}}

        Returns:
            List of matching items
        """
        try:
            response = self.dynamodb_client.query(
                TableName=table_name,
                KeyConditionExpression=key_condition_expression,
                ExpressionAttributeValues=expression_attribute_values,
            )
            return response.get("Items", [])
        except Exception as e:
            print(f"✗ Error querying table: {e}")
            return []
