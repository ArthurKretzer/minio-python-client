# MinIO and PySpark Integration

This project demonstrates how to use MinIO for object storage with Python and simple example using PySpark. It includes:

A custom MinIO client for managing objects in MinIO.

A PySpark example that generates and uploads data in Parquet format to MinIO.

## Project Structure

```text
LICENSE
README.md
example.py               # Example script using MinIO client
minio_client.py          # MinIO client implementation
pyproject.toml           # Project dependencies and configuration
pyspark-example.ipynb    # PySpark example with MinIO integration
ruff.toml                # Ruff configuration for linting
uv.lock                  # UV lock file for package management

logger/                  # Logging utilities
└── logger.py            # Custom logger
└── example.py           # Example usage of logger
```

## Prerequisites

- Python 3.12

- MinIO running locally or remotely

- UV for package management

- Ruff for linting

## Installation

1. Clone the Repository using SSH

    ```bash
    git@github.com:ArthurKretzer/minio-python-client.git
    cd minio-python-client
    ```

2. Install Dependencies with UV

    ```bash
    uv venv
    uv sync
    ```

3. Configure Environment Variables

    Create a .env file or export the variables:

    ```text
    MINIO_ENDPOINT = 127.0.0.1:9000
    MINIO_ACCESS_KEY = 1234567
    MINIO_SECRET_KEY = 1234567
    MINIO_BUCKET = raw
    MINIO_DESTINATION_REPO=example
    MINIO_VERIFY_SSL=True
    MINIO_SECURE_ACCESS=True

    LOG_LEVEL="INFO"
    STORE_LOG_LOCALLY=False
    ```

## Running the Examples

1. Using the MinIO Client

    ```bash
    python example.py
    ```

    This script will:

    - Upload a text file to MinIO.

    - Download and print its content.

    - Delete the file from MinIO.

    The steps will be prompted to you so you can access MinIO and see each step happening.

2. Running the PySpark Example

    ```bash
    jupyter notebook pyspark-example.ipynb
    ```

    This notebook will:

    - Generate a DataFrame.

    - Save it in Parquet format with Snappy compression.

    - Upload it to MinIO.

3. Linting with Ruff

    To check code quality and formatting:

    ```bash
    ruff check .
    ```

## Notes

- Ensure the MinIO bucket (test-bucket) exists or create it manually.

- Adjust the MinIO configurations according to your environment.

## License

This project is licensed under the MIT License. See the LICENSE file for details.