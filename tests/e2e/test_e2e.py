# tests/e2e/test_e2e.py
"""
End-to-end integration tests for the Lagoon-Sync pipeline.

These tests run the entire application against live, Docker-based S3
services to verify core functionalities like full sync, resumability,
and idempotency in a realistic environment.
"""

import asyncio
from pathlib import Path
from typing import Any, Callable, Dict, List, Set, Tuple

import pytest
from aiobotocore.session import AioSession, get_session
from types_aiobotocore_s3.client import S3Client
from types_aiobotocore_s3.paginator import ListObjectsV2Paginator

from lagoon_sync.config import Config
from lagoon_sync.pipeline import LagoonSyncPipeline


async def get_all_keys(bucket_name: str, s3_config: Dict[str, Any]) -> Set[str]:
    """
    Retrieve all object keys from a given S3 bucket.

    Args:
        bucket_name (str): The name of the bucket to list objects from.
        s3_config (Dict[str, Any]): Connection details for the S3 service.

    Returns:
        Set[str]: A set of all object keys found in the bucket.
    """
    session: AioSession = get_session()
    async with session.create_client("s3", **s3_config) as client:
        paginator: ListObjectsV2Paginator = client.get_paginator("list_objects_v2")
        return {
            content["Key"]
            async for result in paginator.paginate(Bucket=bucket_name)
            for content in result.get("Contents", [])
        }


async def upload_test_data(
    client: S3Client,
    bucket: str,
    keys: List[str],
    manifest_path: Path,
    local_base_path: Path,
) -> None:
    """
    Upload a manifest and a set of object files to a bucket.

    Args:
        client (S3Client): An initialized S3 client.
        bucket (str): The target bucket name.
        keys (List[str]): A list of object keys to upload.
        manifest_path (Path): The local path to the manifest file.
        local_base_path (Path): The local directory where object files are stored.
    """
    await client.put_object(
        Bucket=bucket,
        Key="manifests/manifest.parquet",
        Body=manifest_path.read_bytes(),
    )
    upload_tasks: List[asyncio.Task[Any]] = [
        asyncio.create_task(
            client.put_object(
                Bucket=bucket,
                Key=key,
                Body=(local_base_path / key).read_bytes(),
            )
        )
        for key in keys
    ]
    if upload_tasks:
        await asyncio.gather(*upload_tasks)


@pytest.mark.asyncio
async def test_full_sync(
    s3_buckets: Dict[str, str],
    source_s3_service: Dict[str, Any],
    dest_s3_service: Dict[str, Any],
    test_config: Config,
    tmp_path: Path,
    object_generator: Callable[[Path, int], Tuple[List[str], Path]],
) -> None:
    """
    Tests a complete, successful sync from source to destination.

    Arrange:
        - Create 50 local files and a corresponding manifest.
        - Upload the files and manifest to the source S3 bucket.
    Act:
        - Run the LagoonSyncPipeline to completion.
    Assert:
        - Verify that all object keys from the manifest exist in the
          destination bucket and that no extra files are present.

    Args:
        s3_buckets (Dict[str, str]): Fixture providing source and destination
            bucket names.
        source_s3_service (Dict[str, Any]): Fixture with connection details
            for the source S3.
        dest_s3_service (Dict[str, Any]): Fixture with connection details
            for the destination S3.
        test_config (Config): Fixture providing an isolated application configuration.
        tmp_path (Path): Pytest fixture for a temporary directory.
        object_generator: Factory fixture to create test files and a manifest.
    """
    # ARRANGE
    source_bucket: str = s3_buckets["source"]
    dest_bucket: str = s3_buckets["destination"]

    object_keys: List[str]
    manifest_path: Path
    object_keys, manifest_path = object_generator(tmp_path, 50)

    session: AioSession = get_session()
    async with session.create_client("s3", **source_s3_service) as client:
        await upload_test_data(
            client, source_bucket, object_keys, manifest_path, tmp_path
        )

    # ACT
    pipeline: LagoonSyncPipeline = LagoonSyncPipeline(test_config, asyncio.Event())
    await pipeline.run()

    # ASSERT
    dest_keys: Set[str] = await get_all_keys(dest_bucket, dest_s3_service)
    assert set(object_keys) == dest_keys


@pytest.mark.asyncio
async def test_resumability(
    s3_buckets: Dict[str, str],
    source_s3_service: Dict[str, Any],
    dest_s3_service: Dict[str, Any],
    test_config: Config,
    tmp_path: Path,
    object_generator: Callable[[Path, int], Tuple[List[str], Path]],
) -> None:
    """
    Tests that the pipeline resumes without re-copying completed files.

    Arrange:
        - Create 100 files and manifest, upload to source.
    Act:
        - Run the pipeline bu send a shutdown signal partway through.
    Assert:
        - Verify that the destination contains a partial set of files.
    Act (Part 2):
        - Run the pipeline again to completion.
    Assert (Part 2):
        - Verify that the destination now contains all files.

    Args:
        s3_buckets (Dict[str, str]): Fixture providing source and destination
            bucket names.
        source_s3_service (Dict[str, Any]): Fixture with connection details
            for the source S3.
        dest_s3_service (Dict[str, Any]): Fixture with connection details
            for the destination S3.
        test_config (Config): Fixture providing an isolated application configuration.
        tmp_path (Path): Pytest fixture for a temporary directory.
        object_generator: Factory fixture to create test files and a manifest.
    """
    # ARRANGE
    source_bucket: str = s3_buckets["source"]
    dest_bucket: str = s3_buckets["destination"]

    object_keys: List[str]
    manifest_path: Path
    object_keys, manifest_path = object_generator(tmp_path, 100)

    session: AioSession = get_session()
    async with session.create_client("s3", **source_s3_service) as client:
        await upload_test_data(
            client, source_bucket, object_keys, manifest_path, tmp_path
        )

    # ACT 1: Run pipeline and interrupt it
    shutdown_event: asyncio.Event = asyncio.Event()
    pipeline1: LagoonSyncPipeline = LagoonSyncPipeline(test_config, shutdown_event)
    loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()
    loop.call_later(0.2, shutdown_event.set)
    await pipeline1.run()

    # ASSERT 1: Check for partial completion
    dest_keys_part1: Set[str] = await get_all_keys(dest_bucket, dest_s3_service)
    assert 0 < len(dest_keys_part1) < len(object_keys)
    assert dest_keys_part1.issubset(set(object_keys))

    # ACT 2: Run the pipeline again to completion
    pipeline2: LagoonSyncPipeline = LagoonSyncPipeline(test_config, asyncio.Event())
    await pipeline2.run()

    # ASSERT 2: Check for full completion
    dest_keys_part2: Set[str] = await get_all_keys(dest_bucket, dest_s3_service)
    assert set(object_keys) == dest_keys_part2


@pytest.mark.asyncio
async def test_idempotency(
    s3_buckets: Dict[str, str],
    source_s3_service: Dict[str, Any],
    dest_s3_service: Dict[str, Any],
    test_config: Config,
    tmp_path: Path,
    object_generator: Callable[[Path, int], Tuple[List[str], Path]],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """
    Tests that running a completed sync again results in no new operations.

    Arrange:
        - Create 20 files and a manifest, upload to source.
    Act:
        - Run the pipeline to completion.
        - Clear logs and run the exact same pipeline again.
    Assert:
        - Verify that the second run logs that all objects are already synced
          and performs no transfers.

    Args:
        s3_buckets (Dict[str, str]): Fixture providing source and destination
            bucket names.
        source_s3_service (Dict[str, Any]): Fixture with connection details
            for the source S3.
        dest_s3_service (Dict[str, Any]): Fixture with connection details
            for the destination S3.
        test_config (Config): Fixture providing an isolated application configuration.
        tmp_path (Path): Pytest fixture for a temporary directory.
        object_generator: Factory fixture to create test files and a manifest.
        caplog (pytest.LogCaptureFixture): Pytest fixture to capture log output.
    """
    # ARRANGE
    source_bucket: str = s3_buckets["source"]

    object_keys: List[str]
    manifest_path: Path
    object_keys, manifest_path = object_generator(tmp_path, 20)
    session: AioSession = get_session()
    async with session.create_client("s3", **source_s3_service) as client:
        await upload_test_data(
            client, source_bucket, object_keys, manifest_path, tmp_path
        )

    # ACT 1: Run to completion
    await LagoonSyncPipeline(test_config, asyncio.Event()).run()

    # ACT 2: Run again and capture logs
    caplog.clear()
    with caplog.at_level("INFO"):
        await LagoonSyncPipeline(test_config, asyncio.Event()).run()

    # ASSERT
    assert "All objects from manifests are already synced." in caplog.text
