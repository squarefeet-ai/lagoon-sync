# src/lagoon_sync/worker.py
"""
Defines the core transfer worker function.

This module contains the logic for a single worker task that continuously
pulls object keys from a queue and performs a robust, streaming,
cross-provider transfer with data integrity checks.
"""

import asyncio
import logging
import time
from typing import TYPE_CHECKING, Optional

from botocore.exceptions import ClientError

from lagoon_sync.config import Config
from lagoon_sync.controller import DynamicSemaphore, TransferMetrics
from lagoon_sync.db import ObjectStatus, ProgressDB
from lagoon_sync.exceptions import TransferError

if TYPE_CHECKING:
    from aiobotocore.response import StreamingBody
    from rich.progress import Progress, TaskID
    from types_aiobotocore_s3.client import S3Client
    from types_aiobotocore_s3.type_defs import (
        GetObjectOutputTypeDef,
        HeadObjectOutputTypeDef,
    )

logger: logging.Logger = logging.getLogger(__name__)


async def transfer_worker(
    worker_id: int,
    config: Config,
    key_queue: asyncio.Queue[str],
    source_client: "S3Client",
    dest_client: "S3Client",
    semaphore: DynamicSemaphore,
    metrics_queue: asyncio.Queue[TransferMetrics],
    progress_db: ProgressDB,
    progress_bar: "Progress",
    progress_task_id: "TaskID",
) -> None:
    """
    A long-lived worker task that processes keys from a queue.

    This function runs in a continuous loop, fetching a key, acquiring the
    concurrency semaphore, performing the transfer, and reporting results.
    It is designed to be cancelled gracefully when the queue is empty.

    Args:
        worker_id (int): A unique identifier for this worker.
        config (Config): The application configuration.
        key_queue (asyncio.Queue[str]): The queue from which to pull object keys.
        source_client (S3Client): An initialized S3 client for the source.
        dest_client (S3Client): An initialized S3 client for the destination.
        semaphore (DynamicSemaphore): The concurrency limiter.
        metrics_queue (asyncio.Queue[TransferMetrics]): Queue to report transfer metrics to the controller.
        progress_db (ProgressDB): The database for persistent progress tracking.
        progress_bar (Progress): The rich Progress instance for UI updates.
        progress_task_id (TaskID): The TaskID for the main progress bar.
    """
    logger.debug(f"Worker {worker_id} started.")
    while True:
        try:
            key: str = await key_queue.get()
            await _transfer_object_with_retry(
                key=key,
                config=config,
                source_client=source_client,
                dest_client=dest_client,
                semaphore=semaphore,
                metrics_queue=metrics_queue,
                progress_db=progress_db,
            )
            key_queue.task_done()
            progress_bar.update(
                progress_task_id, advance=1, concurrency=semaphore.limit
            )
        except asyncio.CancelledError:
            logger.debug(f"Worker {worker_id} shutting down.")
            break
        except Exception:
            logger.exception(f"Critical error in worker {worker_id}. Shutting down.")
            break


async def _transfer_object_with_retry(
    key: str,
    config: Config,
    source_client: "S3Client",
    dest_client: "S3Client",
    semaphore: DynamicSemaphore,
    metrics_queue: asyncio.Queue[TransferMetrics],
    progress_db: ProgressDB,
) -> None:
    """
    Transfers a single object with integrity checking.

    This function handles the full lifecycle for one object: metadata fetch,
    streaming transfer, and integrity verification. Retries for transient
    network errors are handled by the underlying botocore client config.

    Args:
        key (str): The object key to transfer.
        config (Config): The application configuration.
        source_client (S3Client): The S3 client for the source bucket.
        dest_client (S3Client): The S3 client for the destination bucket.
        semaphore (DynamicSemaphore): The concurrency control semaphore.
        metrics_queue (asyncio.Queue[TransferMetrics]): The queue for reporting performance metrics.
        progress_db (ProgressDB): The database for tracking progress.
    """
    await semaphore.acquire()
    start_time: float = time.monotonic()
    rtt_ms: Optional[float] = None
    object_size: int = 0
    err_tag: Optional[str] = None

    try:
        # 1. Get source metadata (size, ETag) and measure RTT
        head_start_time: float = time.monotonic()
        source_meta: "HeadObjectOutputTypeDef" = await source_client.head_object(
            Bucket=config.source.bucket, Key=key
        )
        rtt_ms = (time.monotonic() - head_start_time) * 1000
        object_size = source_meta["ContentLength"]
        source_etag: str = source_meta["ETag"]

        # 2. Perform streaming GET -> PUT transfer
        if object_size > 0:
            response: GetObjectOutputTypeDef = await source_client.get_object(
                Bucket=config.source.bucket, Key=key
            )
            stream: StreamingBody = response["Body"]
            # NOTE: To satisfy S3 providers that require Content-Length and
            # avoid client-side errors with chunked encoding, we must read
            # the entire object into memory before the PUT operation.
            body_bytes: bytes = await stream.read()
            await dest_client.put_object(
                Bucket=config.destination.bucket,
                Key=key,
                Body=body_bytes,
                ContentLength=object_size,
            )
        else:
            # Explicitly handle zero-byte objects
            logger.debug(f"Object '{key}' has size 0, creating empty object.")
            await dest_client.put_object(
                Bucket=config.destination.bucket, Key=key, Body=b"", ContentLength=0
            )

        # 3. Verify integrity on destination
        dest_meta: HeadObjectOutputTypeDef = await dest_client.head_object(
            Bucket=config.destination.bucket, Key=key
        )
        if dest_meta["ETag"] != source_etag:
            raise TransferError(
                f"Integrity check failed for '{key}': ETag mismatch "
                f"({source_etag} != {dest_meta['ETag']})"
            )
        if dest_meta["ContentLength"] != object_size:
            raise TransferError(
                f"Integrity check failed for '{key}': ContentLength mismatch "
                f"({object_size} != {dest_meta['ContentLength']})"
            )

        progress_db.set_status(key, ObjectStatus.COMPLETED)
        logger.debug(f"Successfully transferred 's3://{config.source.bucket}/{key}'")

    except (ClientError, TransferError) as e:
        err_tag = type(e).__name__
        progress_db.set_status(key, ObjectStatus.FAILED)
        logger.error(f"Failed to transfer '{key}': {err_tag} - {e}")
    except Exception as e:
        err_tag = type(e).__name__
        progress_db.set_status(key, ObjectStatus.FAILED)
        logger.exception(f"An unexpected error occurred transferring '{key}'")
    finally:
        semaphore.release()
        duration_s: float = time.monotonic() - start_time
        metrics: TransferMetrics = TransferMetrics(
            bytes=object_size,
            duration_s=duration_s,
            err_tag=err_tag,
            rtt_ms=rtt_ms,
        )
        # This await provides back-pressure if the controller is busy
        await metrics_queue.put(metrics)
