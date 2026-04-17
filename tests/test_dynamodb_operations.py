from unittest.mock import MagicMock

from botocore.exceptions import ClientError

from dynamodb_service.dynamodb_operations import DynamoDBOperations


def test_create_table_with_sort_key() -> None:
    client = MagicMock()
    operations = DynamoDBOperations(client)

    assert operations.create_table("users", partition_key="id", sort_key="timestamp") is True
    client.create_table.assert_called_once_with(
        TableName="users",
        KeySchema=[
            {"AttributeName": "id", "KeyType": "HASH"},
            {"AttributeName": "timestamp", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "id", "AttributeType": "S"},
            {"AttributeName": "timestamp", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )


def test_create_table_existing_is_treated_as_success() -> None:
    client = MagicMock()
    client.create_table.side_effect = ClientError(
        {"Error": {"Code": "ResourceInUseException", "Message": "exists"}},
        "CreateTable",
    )
    operations = DynamoDBOperations(client)

    assert operations.create_table("users", partition_key="id") is True


def test_create_table_generic_error_returns_false() -> None:
    client = MagicMock()
    client.create_table.side_effect = RuntimeError("boom")
    operations = DynamoDBOperations(client)

    assert operations.create_table("users", partition_key="id") is False


def test_table_and_item_operations() -> None:
    client = MagicMock()
    client.list_tables.return_value = {"TableNames": ["users"]}
    client.get_item.return_value = {"Item": {"id": {"S": "1"}, "name": {"S": "Alice"}}}
    client.scan.return_value = {"Items": [{"id": {"S": "1"}}]}
    client.query.return_value = {"Items": [{"id": {"S": "1"}}]}

    operations = DynamoDBOperations(client)

    assert operations.list_tables() == ["users"]

    item = {"id": {"S": "1"}, "name": {"S": "Alice"}}
    key = {"id": {"S": "1"}}

    assert operations.put_item("users", item) is True
    assert operations.get_item("users", key) == item
    assert operations.scan("users") == [{"id": {"S": "1"}}]
    assert operations.query("users", "id = :id_val", {":id_val": {"S": "1"}}) == [
        {"id": {"S": "1"}}
    ]
    assert operations.delete_item("users", key) is True
    assert operations.delete_table("users") is True


def test_get_item_not_found_returns_none() -> None:
    client = MagicMock()
    client.get_item.return_value = {}
    operations = DynamoDBOperations(client)

    assert operations.get_item("users", {"id": {"S": "404"}}) is None
