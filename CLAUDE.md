# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

This project uses `uv` for dependency and environment management.

```bash
# Install dependencies
uv sync

# Run the main script
uv run python main.py

# Run tests
uv run pytest

# Run a single test file
uv run pytest tests/test_file.py

# Run with coverage
uv run pytest --cov

# Lint
uv run ruff check .

# Format
uv run black .

# Type check
uv run mypy .
```

LocalStack must be running before executing `main.py` or any tests that hit S3:
```bash
localstack start
```

## Architecture

The project interacts with AWS services via LocalStack (local AWS emulator at `http://localhost:4566`).

- **`connection/`** — `AwsConfig` is a client factory that caches one singleton boto3 client per service via `get_s3_client()` / `get_dynamodb_client()`. All clients share the same endpoint/region/credentials. To target real AWS, pass a different `endpoint_url` and real credentials. Adding a new service means adding a `_<service>_client` field and a `get_<service>_client()` method that calls `_make_client("<service>")`.
- **`s3_service/`** — `S3Operations` wraps the S3 client: `create_bucket`, `list_buckets`, `upload_object`, `download_object`, `list_objects`, `delete_object`.
- **`dynamodb_service/`** — `DynamoDBOperations` wraps the DynamoDB client: `create_table`, `delete_table`, `list_tables`, `put_item`, `get_item`, `delete_item`, `scan`, `query`. Items and keys use raw DynamoDB attribute format (`{"attr": {"S": "value"}}`).
- **`main.py`** — Wires everything together: one `AwsConfig` instance, separate `*Operations` objects per service.

## Tooling

- Ruff and Black both enforce line-length 100, targeting Python 3.12.
- Ruff lint rules enabled: `E`, `F`, `I` (isort), `B` (bugbear), `UP` (pyupgrade).
- Mypy is configured in strict mode.
- Tests live in `tests/` (pytest `testpaths` setting).
