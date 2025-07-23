# tests/conftest.py
"""
Pytest configuration and fixtures for the lagoon-sunc integration tests.

This module sets up the testing environment, including:
- Spinning up Docker containers for source and destination S3 services (MinIO).
- Providing fixtures for S3 services endpoints and credentials.
- Creating and cleaning up isolated S3 buckets for each test function.
- Offering helper factories for generating test data and manifests.
"""

import os
import uuid
from pathlib import Path
from typing import Any, AsyncGenerator, Callable, Dict, Generator, List, Tuple

import boto3
import polars as pl
import pytest
import pytest_asyncio
import requests
from aiobotocore.session import AioSession, get_session
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError
from requests.exceptions import ConnectionError
from types_boto3_s3.service_resource import Bucket, S3ServiceResource

from lagoon_sync.config import AppConfig, Config

# --- Constants ---
S3_ACCESS_KEY: str = "minio-key"
S3_SECRET_KEY: str = "minio-secret"
S3_REGION: str = "us-east-1"


# --- Docker Fixtures ---
@pytest.fixture(scope="session")
def docker_compose_file(pytestconfig: pytest.Config) -> str:
    """
    Locate the docker-compose.yml file for the test suite.

    Args:
        pytestconfig (pytest.Config): The pytest configuration objects.

    Returns:
        str: The absolyte path to the docker-compose.yml file.
    """
    return str(Path(pytestconfig.rootdir) / "tests" / "docker-compose.yml")


@pytest.fixture(scope="session")
def docker_compose_project_name() -> str:
    """
    Define a unique, static project name for the Docker stack.

    Returns:
        str: A unique name for the docker-compose project.
    """
    return "lagoon-sync-tests"


def _is_s3_responsive(url: str) -> bool:
    """
    Check if the MinIO health endpoint is responsive.

    Args:
        url (str): The base URL of the MinIO API.

    Returns:
        bool: True if the service is responsive, False otherwise.
    """
    try:
        # The health check endpoint for MinIO is /minio/health/live
        response: requests.Response = requests.get(f"{url}/minio/health/live")
        return response.status_code == 200
    except ConnectionError:
        return False


@pytest.fixture(scope="session")
def source_s3_service(
    docker_ip: str,
    docker_services: Any,
) -> Dict[str, Any]:
    """
    Ensure the source S3 service is running and return its connection details.

    Args:
        docker_ip (str): The IP address of the Docker host, provided by pytest-docker.
        docker_services (Any): The pytest-docker services fixture.

    Returns:
        Dict[str, Any]: A dictionary with connection details for the source S3 service.
    """
    port: int = docker_services.port_for("minio-source", 9000)
    api_url: str = f"http://{docker_ip}:{port}"
    docker_services.wait_until_responsive(
        timeout=30.0, pause=0.1, check=lambda: _is_s3_responsive(api_url)
    )
    return {
        "endpoint_url": api_url,
        "aws_access_key_id": S3_ACCESS_KEY,
        "aws_secret_access_key": S3_SECRET_KEY,
        "region_name": S3_REGION,
    }


@pytest.fixture(scope="session")
def dest_s3_service(docker_ip: str, docker_services: Any) -> Dict[str, Any]:
    """
    Ensures the destination S3 service is running and return its connection details.

    Args:
        docker_ip (str): The IP address of the Docker host, provided by pytest-docker.
        docker_services (Any): The pytest-docker services fixture.

    Returns:
        Dict[str, Any]: A dictionary with connection details for the destination
            S3 service.
    """
    port: int = docker_services.port_for("minio-destination", 9000)
    api_url: str = f"http://{docker_ip}:{port}"
    docker_services.wait_until_responsive(
        timeout=30.0, pause=0.1, check=lambda: _is_s3_responsive(api_url)
    )
    return {
        "endpoint_url": api_url,
        "aws_access_key_id": S3_ACCESS_KEY,
        "aws_secret_access_key": S3_SECRET_KEY,
        "region_name": S3_REGION,
    }


# --- Application Fixtures ---
@pytest_asyncio.fixture(scope="function")
async def s3_buckets(
    source_s3_service: Dict[str, Any],
    dest_s3_service: Dict[str, Any],
) -> AsyncGenerator[Dict[str, str], None]:
    """
    Create unique, isolated S3 buckets for a single test function.

    This fixture provides a clean S3 environment for each test. It sets the
    necessary environment variables for the applications's Config object and
    guarantees cleanup of buckets and their contents after the test.

    Args:
        source_s3_service (Dict[str, Any]): Connection details for the source S3.
        dest_s3_service (Dict[str, Any]): Connection details for the destination S3.

    Yield:
        AsyncGenerator[Dict[str, str], None]: A dictionary with the names of
            the created source and destination buckets.
    """
    session: AioSession = get_session()
    bucket_name_suffix: str = f"test-bucket-{uuid.uuid4()}"
    source_bucket: str = f"source-{bucket_name_suffix}"
    dest_bucket: str = f"dest-{bucket_name_suffix}"

    os.environ["LAGOON_SOURCE_ENDPOINT_URL"] = source_s3_service["endpoint_url"]
    os.environ["LAGOON_SOURCE_ACCESS_KEY_ID"] = S3_ACCESS_KEY
    os.environ["LAGOON_SOURCE_SECRET_ACCESS_KEY"] = S3_SECRET_KEY
    os.environ["LAGOON_SOURCE_BUCKET"] = source_bucket
    os.environ["LAGOON_SOURCE_REGION"] = S3_REGION

    os.environ["LAGOON_DESTINATION_ENDPOINT_URL"] = dest_s3_service["endpoint_url"]
    os.environ["LAGOON_DESTINATION_ACCESS_KEY_ID"] = S3_ACCESS_KEY
    os.environ["LAGOON_DESTINATION_SECRET_ACCESS_KEY"] = S3_SECRET_KEY
    os.environ["LAGOON_DESTINATION_BUCKET"] = dest_bucket
    os.environ["LAGOON_DESTINATION_REGION"] = S3_REGION

    # Create buckets
    async with (
        session.create_client("s3", **source_s3_service) as s3_source,
        session.create_client("s3", **dest_s3_service) as s3_dest,
    ):
        await s3_source.create_bucket(Bucket=source_bucket)
        await s3_dest.create_bucket(Bucket=dest_bucket)

    yield {"source": source_bucket, "destination": dest_bucket}

    # Cleanup: boto3 is simpler for synchronous, recursive delete
    boto_config: BotoConfig = BotoConfig(
        retries={"max_attempts": 0, "mode": "standard"}
    )
    s3_resource_source: S3ServiceResource = boto3.resource(
        "s3", **source_s3_service, config=boto_config
    )
    s3_resource_dest: S3ServiceResource = boto3.resource(
        "s3", **dest_s3_service, config=boto_config
    )

    for resource, bucket in [
        (s3_resource_source, source_bucket),
        (s3_resource_dest, dest_bucket),
    ]:
        try:
            bucket_obj: Bucket = resource.Bucket(bucket)
            bucket_obj.objects.all().delete()
            bucket_obj.delete()
        except ClientError as e:
            if e.response["Error"]["Code"] != "NoSuchBucket":
                raise


@pytest.fixture(scope="function")
def test_config(tmp_path: Path) -> Config:
    """
    Provide a Config object with a temporary data directory for isolation.

    Args:
        tmp_path (Path): The pytest fixture for a temporary directory.

    Returns:
        Config: A Config instance for use in tests.
    """
    app_config: AppConfig = AppConfig(data_dir=tmp_path, max_concurrency=10)
    return Config(app=app_config)


@pytest.fixture(scope="function")
def object_generator() -> Generator[
    Callable[[Path, int], Tuple[List[str], Path]], None, None
]:
    """
    Provide a factory to generate test files and Polars-based Parquet manifest.

    This simplifies test setup by creating dummy object files and a manifest
    that points to them, ready for upload to the source S3 bucket.

    Yields:
        A factory function that accepts a base path and number of objects,
        returning a list of object keys and the path to the manifest file.
    """

    def _creator(base_path: Path, num_objects: int) -> Tuple[List[str], Path]:
        """
        The actual factory implementation.

        Args:
            base_path (Path): The temporary directory to create files in.
            num_objects (int): The number of object files to create.

        Returns:
            Tuple[List[str], Path]: A tuple containing the list of object keys and
                the manifest path.
        """
        object_keys: List[str] = [f"data/obj_{i}.txt" for i in range(num_objects)]
        for key in object_keys:
            p: Path = base_path / key
            p.parent.mkdir(exist_ok=True, parents=True)
            p.write_text(f"content of {key}")

        manifest_path: Path = base_path / "manifests" / "manifest.parquet"
        manifest_path.parent.mkdir(exist_ok=True)
        df: pl.DataFrame = pl.DataFrame({"uri": object_keys})
        df.write_parquet(manifest_path)
        return object_keys, manifest_path

    yield _creator
