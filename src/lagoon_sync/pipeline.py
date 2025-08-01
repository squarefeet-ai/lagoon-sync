# src/lagoon_sync/pipeline.py
"""Core orchestration logic for the lagoon-sync pipeline."""

import asyncio
import logging
from pathlib import PurePath
from typing import TYPE_CHECKING, Any, AsyncIterator, Dict, List, Optional, Set

import polars as pl
from aiobotocore.session import AioSession, get_session
from botocore.config import Config as BotoConfig
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TaskID,
    TextColumn,
    TimeRemainingColumn,
)

from lagoon_sync.config import Config
from lagoon_sync.controller import CongestionController, DynamicSemaphore
from lagoon_sync.db import ProgressDB
from lagoon_sync.worker import transfer_worker

if TYPE_CHECKING:
    from types_aiobotocore_s3.client import S3Client
    from types_aiobotocore_s3.paginator import ListObjectsV2Paginator
    from types_aiobotocore_s3.type_defs import ListObjectsV2OutputTypeDef

logger: logging.Logger = logging.getLogger(__name__)


class LagoonSyncPipeline:
    """Orchestrates the entire sync from start to finish."""

    def __init__(self, config: Config, shutdown_event: asyncio.Event) -> None:
        """
        Initializes the pipeline with the given configuration.

        Args:
            config (Config): The application configuration.
            shutdown_event (asyncio.Event): Event to signal graceful shutdown.
        """
        self._config: Config = config
        self._shutdown_event: asyncio.Event = shutdown_event
        self._session: AioSession = get_session()

    async def run(self) -> None:
        """
        Executes the full synchronization pipeline.

        This method discovers manigests, filters out already completed objects,
        and runs the concurrent copy operations with dynamic concurrency control.
        """
        logger.info("Starting lagoon-sync pipeline.")
        # Explicitly set signature_version and disable payload signing. This is
        # the robust configuration for non-AWS S3 providers that require
        # Content-Length and support SigV4, preventing client-side conflicts.
        boto_config: BotoConfig = BotoConfig(
            signature_version="s3v4",
            max_pool_connections=self._config.app.max_concurrency + 50,
            retries={"max_attempts": self._config.app.transfer_max_attempts},
            s3={"payload_signing_enabled": False},
        )
        with ProgressDB(
            self._config.app.data_dir / "progress.lmdb",
            self._config.app.db_map_size_gb,
        ) as progress_db:
            async with (
                self._session.create_client(
                    "s3",
                    **self._config.source.as_boto_dict(),
                    config=boto_config,
                ) as source_client,
                self._session.create_client(
                    "s3",
                    **self._config.destination.as_boto_dict(),
                    config=boto_config,
                ) as dest_client,
            ):
                if self._shutdown_event.is_set():
                    return

                object_keys: Set[str] = await self._get_object_keys_from_manifests(
                    source_client
                )

                if self._shutdown_event.is_set():
                    return

                if not object_keys:
                    logger.info("No objects found in manifests. Pipeline finished.")
                    return

                pending_keys: List[str] = progress_db.filter_pending_keys(object_keys)

                if not pending_keys:
                    logger.info("All objects from manifests are already synced.")
                    return

                logger.info(
                    f"Found {len(pending_keys)} objects to transfer "
                    f"({len(object_keys) - len(pending_keys)} already completed)."
                )

                await self._run_transfers(
                    pending_keys, source_client, dest_client, progress_db
                )

            if not self._shutdown_event.is_set():
                logger.info("Lagoon-sync pipeline completed successfully")

    async def _read_manifest_worker(
        self,
        queue: asyncio.Queue[Optional[str]],
        all_keys: Set[str],
        storage_options: Dict[str, str],
    ) -> None:
        """
        Consumes manifest keys from a queue and processes them by streaming.

        Args:
            queue (asyncio.Queue(Optional[str])): Queue of manifest S3 keys to process.
            all_keys (Set[str]): A set to which all discovered object keys will be added.
            storage_options (Dict[str, str]): Credentials for Polars to access S3.
        """
        loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()
        column_name: str = self._config.app.manifest_column_name

        while not self._shutdown_event.is_set():
            manifest_key: Optional[str] = await queue.get()
            if manifest_key is None:  # Sentinel value to signal completion
                queue.task_done()
                break

            logger.debug(f"Processing manifest: {manifest_key}")
            manifest_uri: str = f"s3://{self._config.source.bucket}/{manifest_key}"
            try:

                def read_keys_from_manifest() -> List[str]:
                    return (
                        pl.scan_parquet(manifest_uri, storage_options=storage_options)
                        .select(column_name)
                        .collect()
                        .get_column(column_name)
                        .to_list()
                    )

                keys_from_manifest: List[str] = await loop.run_in_executor(
                    None, read_keys_from_manifest
                )

                for key in keys_from_manifest:
                    if ".." in PurePath(key).parts or PurePath(key).is_absolute():
                        logger.warning(
                            f"Skipping unsafe key from manifest "
                            f"'{manifest_key}': '{key}'"
                        )
                        continue
                    all_keys.add(key)
            except Exception as e:
                logger.error(f"Failed to read manifest '{manifest_key}': {e}")
            finally:
                queue.task_done()

    async def _get_object_keys_from_manifests(
        self,
        source_client: "S3Client",
    ) -> Set[str]:
        """
        Concurrently scans and parses all Parquet manifests to collect object keys.

        Args:
            source_client (S3Client): The aiobotocore S3 client for the source bucket.

        Returns:
            Set[str]: All unique, validated object keys from all manifests.
        """
        logger.info(
            "Scanning for new manifests under "
            f"'s3://{self._config.source.bucket}"
            f"/{self._config.app.manifest_prefix}'..."
        )
        paginator: "ListObjectsV2Paginator" = source_client.get_paginator(
            "list_objects_v2"
        )
        pages: AsyncIterator["ListObjectsV2OutputTypeDef"] = paginator.paginate(
            Bucket=self._config.source.bucket,
            Prefix=self._config.app.manifest_prefix,
        )

        storage_options: Dict[str, str] = {
            "aws_access_key_id": self._config.source.access_key_id,
            "aws_secret_access_key": self._config.source.secret_access_key,
            "aws_endpoint_url": self._config.source.endpoint_url,
            "aws_region": self._config.source.region,
            "aws_allow_http": "true",  # Often needed for non-AWS S3
        }

        all_keys: Set[str] = set()
        manifest_queue: asyncio.Queue[Optional[str]] = asyncio.Queue(maxsize=2000)
        num_workers: int = 8

        # Start consumer tasks
        worker_tasks: List[asyncio.Task[None]] = [
            asyncio.create_task(
                self._read_manifest_worker(
                    manifest_queue,
                    all_keys,
                    storage_options,
                )
            )
            for _ in range(num_workers)
        ]

        # Producer: discover manifests and add them to the queue
        async for page in pages:
            if self._shutdown_event.is_set():
                logger.warning("Shutdown initiated, stopping manifest scan.")
                break

            for manifest_obj in page.get("Contents", []):
                if manifest_obj["Key"].endswith(".parquet"):
                    await manifest_queue.put(manifest_obj["Key"])

        # Signal workers to exit
        for _ in range(num_workers):
            await manifest_queue.put(None)

        await manifest_queue.join()
        for task in worker_tasks:
            task.cancel()
        await asyncio.gather(*worker_tasks, return_exceptions=True)

        if self._shutdown_event.is_set():
            logger.warning("Shutdown initiated, returning partial key set.")
            return set()

        logger.info(f"Found {len(all_keys)} total objects across all manifests.")
        return all_keys

    async def _wait_for_queue_completion(
        self,
        producer_task: asyncio.Task[None],
        queue: asyncio.Queue[Any],
    ) -> None:
        """
        Waits for a producer task to finish and then for the queue to be drained.

        Args:
            producer_task (asyncio.Task[None]):
            queue (asyncio.Queue[Any]):
        """
        await producer_task
        await queue.join()

    async def _run_transfers(
        self,
        pending_keys: List[str],
        source_client: "S3Client",
        dest_client: "S3Client",
        progress_db: ProgressDB,
    ) -> None:
        """
        Manages the concurrent transfer of objects using a worker pool.

        Args:
            pending_keys (List[str]): The list of object keys to transfer.
            source_client (S3Client): The initialized source S3 client.
            dest_client (S3Client): The initialized destination S3 client.
            progress_db (ProgressDB): The progress tracking database instance.
        """
        semaphore: DynamicSemaphore = DynamicSemaphore(
            self._config.app.min_concurrency,
            self._config.app.min_concurrency,
            self._config.app.max_concurrency,
        )
        controller: CongestionController = CongestionController(
            semaphore,
            self._config.app,
            self._shutdown_event,
        )

        controller_task: Optional[asyncio.Task[None]] = None
        if self._config.app.controller_enabled and len(pending_keys) > 1000:
            controller_task = asyncio.create_task(controller.run())
        else:
            logger.info(
                "Congestion controller disabled (or job too small). "
                f"Using fixed concurrency: {self._config.app.max_concurrency}"
            )
            semaphore.set_limit(self._config.app.max_concurrency)

        key_queue: asyncio.Queue[str] = asyncio.Queue()

        progress: Progress = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeRemainingColumn(),
            TextColumn("([bold cyan]Concurrency: {task.fields[concurrency]})"),
            transient=True,
        )

        with progress:
            task_id: TaskID = progress.add_task(
                "Syncing...", total=len(pending_keys), concurrency=semaphore.limit
            )

            async def producer(keys: List[str]) -> None:
                """Feeds the key_queue."""
                for key in keys:
                    if self._shutdown_event.is_set():
                        break
                    await key_queue.put(key)

            producer_task: asyncio.Task[None] = asyncio.create_task(
                producer(pending_keys)
            )

            worker_tasks: List[asyncio.Task[None]] = [
                asyncio.create_task(
                    transfer_worker(
                        worker_id=i,
                        config=self._config,
                        key_queue=key_queue,
                        source_client=source_client,
                        dest_client=dest_client,
                        semaphore=semaphore,
                        metrics_queue=controller.metrics_queue,
                        progress_db=progress_db,
                        progress_bar=progress,
                        progress_task_id=task_id,
                    )
                )
                for i in range(self._config.app.max_concurrency)
            ]

            # Race normal completion against a shutdown signal
            normal_completion_task: asyncio.Task[None] = asyncio.create_task(
                self._wait_for_queue_completion(producer_task, key_queue)
            )
            shutdown_task: asyncio.Task[None] = asyncio.create_task(
                self._shutdown_event.wait()
            )

            done, pending = await asyncio.wait(
                {normal_completion_task, shutdown_task},
                return_when=asyncio.FIRST_COMPLETED,
            )

            if shutdown_task in done:
                logger.warning("Shutdown signal received. Terminating transfers.")
            else:
                logger.info("All pending objects have been processed.")

            # Clean up all running tasks
            logger.info("Stopping all background tasks...")
            for task in pending:
                task.cancel()
            producer_task.cancel()
            for task in worker_tasks:
                task.cancel()

            await asyncio.gather(producer_task, *worker_tasks, return_exceptions=True)

        if controller_task:
            await controller.stop()
