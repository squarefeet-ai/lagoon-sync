# src/lagoon_sync/worker.py
"""
Defines the core transfer worker function.

This module contains the logic for copying a single object from source to
destination, measuring performance, and reporting metrics.
"""

import asyncio
import logging
import time
from typing import TYPE_CHECKING, Optional

from lagoon_sync.controller import DynamicSemaphore, TransferMetrics
from lagoon_sync.db import ObjectStatus, ProgressDB

if TYPE_CHECKING:
    from aiobotocore.response import StreamingBody
    from types_aiobotocore_s3.client import S3Client
    from types_aiobotocore_s3.type_defs import (
        GetObjectOutputTypeDef,
    )

logger: logging.Logger = logging.getLogger(__name__)


async def copy_object(
    key: str,
    source_client: "S3Client",
    dest_client: "S3Client",
    source_bucket: str,
    dest_bucket: str,
    semaphore: DynamicSemaphore,
    metrics_queue: asyncio.Queue[TransferMetrics],
    progress_db: ProgressDB,
) -> None:
    """
    Copies a single object, measures performance, and updates progress.

    This function performs a streaming copy from source to destination to
    minimize memory usage. It is the core unit of work in the pipeline.

    Args:
        key (str): The object key to copy.
        source_client (S3Client): An initialized S3 client for the source.
        dest_client (S3Client): An initialized S3 client for the destination.
        source_bucket (str): The source bucket name.
        dest_bucket (str): The destination bucket name.
        semaphore (DynamicSemaphore): The concurrency limiter.
        metrics_queue (asyncio.Queue): Queue to report transfer metrics.
        progress_db (ProgressDB): The database for progress tracking.
    """
    await semaphore.acquire()
    start_time: float = time.monotonic()
    rtt_ms: Optional[float] = None
    object_size: int = 0
    err_tag: Optional[str] = None

    try:
        # Get object from source
        get_object_start_time: float = time.monotonic()
        response: "GetObjectOutputTypeDef" = await source_client.get_object(
            Bucket=source_bucket, Key=key
        )
        rtt_ms: float = (time.monotonic() - get_object_start_time) * 1000

        body: "StreamingBody" = response["Body"]
        data: bytes = await body.read()
        object_size = len(data)

        await dest_client.put_object(
            Bucket=dest_bucket,
            Key=key,
            Body=data,
            ContentLength=object_size,
        )

        progress_db.set_status(key, ObjectStatus.COMPLETED)
        logger.debug(f"Successfully copied 's3://{source_bucket}/{key}'")

    except Exception as e:
        err_tag: str = type(e).__name__
        progress_db.set_status(key, ObjectStatus.FAILED)
        logger.error(f"Failed to copy 's3://{source_bucket}/{key}': {err_tag} - {e}")
        # We don't re-raise; the failure is recorded in the DB and metrics.
        # The next run can retry FAILED objects if desired.
    finally:
        semaphore.release()
        duration_s: float = time.monotonic() - start_time
        metrics: TransferMetrics = TransferMetrics(
            bytes=object_size,
            duration_s=duration_s,
            err_tag=err_tag,
            rtt_ms=rtt_ms,
        )
        await metrics_queue.put(metrics)
