# src/lagoon_sync/pipeline.py
"""Core orchestration logic for the lagoon-sync pipeline."""

import asyncio
import logging
from pathlib import PurePath
from typing import TYPE_CHECKING, AsyncIterator, Dict, List, Optional, Set

import polars as pl
from aiobotocore.session import AioSession, get_session
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskID,
    TaskProgressColumn,
    TextColumn,
    TimeRemainingColumn,
)

from lagoon_sync.config import Config
from lagoon_sync.controller import CongestionController, DynamicSemaphore
from lagoon_sync.db import ProgressDB
from lagoon_sync.worker import copy_object

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
        self._progress_db: ProgressDB = ProgressDB(
            config.app.data_dir / "progress.lmdb"
        )
        self._processed_manifests_db: ProgressDB = ProgressDB(
            config.app.data_dir / "manifests.lmdb",
        )
        self._session: AioSession = get_session()

    async def run(self) -> None:
        """
        Executes the full synchronization pipeline.

        This method discovers manigests, filters out already completed objects,
        and runs the concurrent copy operations with dynamic concurrency control.
        """
        logger.info("Starting lagoon-sync pipeline.")
        try:
            async with (
                self._session.create_client(
                    "s3", **self._config.source.as_boto_dict()
                ) as source_client,
                self._session.create_client(
                    "s3", **self._config.destination.as_boto_dict()
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
                    logger.info("No new objects to sync. Pipeline finished.")
                    return

                await self._run_transfers(object_keys, source_client, dest_client)

            if not self._shutdown_event.is_set():
                logger.info("Lagoon-sync pipeline completed successfully")
        finally:
            self._progress_db.close()
            self._processed_manifests_db.close()

    async def _get_object_keys_from_manifests(
        self,
        source_client: "S3Client",
    ) -> Set[str]:
        """
        Scan every Parquet manifest under `app.manifest_prefix`, collect
        the object keys they list, validate them, and return one deduplicated set.

        Decisions about what still needs copying are made later, by comparing
        this set with entries in `progress.lmdb`, we do not mark
        manifests as done here.

        Args:
            source_client (S3Client): The aiobotocore S3 client for the source bucket.

        Returns:
            Set[str]: All unique object keys that still need to be transferred.
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

        async for page in pages:
            if self._shutdown_event.is_set():
                logger.warning("Shutdown initiated, stopping manifest scan.")
                break

            for manifest_obj in page.get("Contents", []):
                manifest_key: str = manifest_obj["Key"]

                if not manifest_key.endswith(".parquet"):
                    continue

                logger.info(f"Processing new manifest: {manifest_key}")
                manifest_uri: str = f"s3://{self._config.source.bucket}/{manifest_key}"

                try:
                    # Use Polars to scan S3 directly, avoiding local download
                    lf: pl.LazyFrame = pl.scan_parquet(
                        manifest_uri, storage_options=storage_options
                    )
                    keys_df: pl.DataFrame = lf.select(
                        self._config.app.manifest_column_name
                    ).collect()
                    keys_from_manifest: Set[str] = set(
                        keys_df[self._config.app.manifest_column_name].to_list()
                    )

                    # Validate keys to prevent path traversal attacks
                    validated_keys: Set[str] = set()
                    for key in keys_from_manifest:
                        if ".." in PurePath(key).parts or PurePath(key).is_absolute():
                            logger.warning(
                                f"Skipping unsafe key from manifest "
                                f"'{manifest_key}': '{key}'"
                            )
                            continue
                        validated_keys.add(key)

                    all_keys.update(validated_keys)

                except Exception as e:
                    logger.error(f"Failed to read manifest '{manifest_key}': {e}")

        logger.info(f"Found {len(all_keys)} total objects across all new manifests.")
        return all_keys

    async def _run_transfers(
        self,
        object_keys: Set[str],
        source_client: "S3Client",
        dest_client: "S3Client",
    ) -> None:
        """
        Manages the concurrent transfer of a list of objects.

        Args:
            object_key (Set[str]): The set of object keys to transfer.
            source_client (S3Client): The initialized source S3 client.
            dest_client (S3Client): The initialized destination S3 client.
        """
        pending_keys: List[str] = [
            key for key in object_keys if not self._progress_db.is_completed(key)
        ]

        if not pending_keys:
            logger.info("All objects from manifests are already synced.")
            return

        logger.info(
            f"Found {len(pending_keys)} objects to transfer "
            f"({len(object_keys) - len(pending_keys)} already completed)."
        )

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
        if self._config.app.controller_enabled and len(pending_keys) > 2500:
            controller_task = asyncio.create_task(controller.run())
        else:
            logger.info(
                "Congestion controller disabled (or job too small). "
                f"Using fixed concurrency: {self._config.app.max_concurrency}"
            )
            semaphore.set_limit(self._config.app.max_concurrency)

        progress: Progress = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TimeRemainingColumn(),
            TextColumn("{task.fields[info]}"),
            transient=True,
        )

        with progress:
            task_id: TaskID = progress.add_task(
                "Syncing...", total=len(pending_keys), info=""
            )
            workers: Set[asyncio.Task[None]] = {
                asyncio.create_task(
                    copy_object(
                        key=key,
                        source_client=source_client,
                        dest_client=dest_client,
                        source_bucket=self._config.source.bucket,
                        dest_bucket=self._config.destination.bucket,
                        semaphore=semaphore,
                        metrics_queue=controller.metrics_queue,
                        progress_db=self._progress_db,
                    )
                )
                for key in pending_keys
            }

            if not workers:
                return

            shutdown_waiter: asyncio.Task[None] = asyncio.create_task(
                self._shutdown_event.wait()
            )
            active_workers: Set[asyncio.Task[None]] = workers

            while active_workers:
                done, pending = await asyncio.wait(
                    active_workers | {shutdown_waiter},
                    return_when=asyncio.FIRST_COMPLETED,
                )

                if shutdown_waiter in done:
                    logger.warning(
                        "Shutdown signal received, cancelling active transfers."
                    )
                    for task in pending:
                        if task is not shutdown_waiter:
                            task.cancel()
                    if pending - {shutdown_waiter}:
                        await asyncio.wait(pending - {shutdown_waiter})
                    break

                for task in done:
                    if exc := task.exception():
                        logger.error(
                            f"Worker task failed unexpectedly: {exc}", exc_info=True
                        )
                    progress.update(
                        task_id,
                        advance=1,
                        info=f"Concurrency: {semaphore.limit}",
                    )

                active_workers = pending - {shutdown_waiter}

            shutdown_waiter.cancel()

        if controller_task:
            await controller.stop()
