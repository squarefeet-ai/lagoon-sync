# scripts/create_image_manifest.py
"""
Generates and uploads a Parquet manifest of successful image URIs.

This script connects to a PostgreSQL database, queries the 'images' table for
all successfully processed image storage keys, compiles them into one or more
single-column Parquet files, and uploads them to a specified S3 location.

It is designed to be memory-efficient by streaming results from the database
and creating chunked manifest files. It also supports limiting the of
records for testing and deduplicating against existing manifests in S3.
"""

import asyncio
import io
import logging
import os
import sys
from dataclasses import dataclass, field
from pathlib import PurePath
from typing import Any, AsyncIterator, Dict, List, Optional, Set, Tuple
from urllib.parse import ParseResult, urlparse

import aiobotocore.session
import asyncpg
import click
import polars as pl
from aiobotocore.session import AioSession
from asyncpg.connection import Connection
from asyncpg.cursor import Cursor
from dotenv import load_dotenv
from rich.logging import RichHandler
from types_aiobotocore_s3.client import S3Client
from types_aiobotocore_s3.paginator import ListObjectsV2Paginator
from types_aiobotocore_s3.type_defs import ListObjectsV2OutputTypeDef

logger: logging.Logger = logging.getLogger(__name__)


def _get_env_var(name: str, default: Optional[str] = None) -> str:
    """
    Retrieves a required environment variable.

    Args:
        name (str): The name of the environment variable.
        default (str, optional): A default value to return if not found.

    Returns:
        str: The value of the environment variable.

    Raises:
        ValueError: If the environment variable is not set and no default
            is provided.
    """
    value: Optional[str] = os.environ.get(name, default)
    if value is None:
        raise ValueError(f"Environment variable '{name}' must be set.")
    return value


@dataclass(frozen=True)
class S3Config:
    """
    Configuration for the S3 client connection.

    Attributes:
        endpoint_url (str):
        access_key_id (str):
        secret_access_key (str):
        region (str):
    """

    endpoint_url: str
    access_key_id: str
    secret_access_key: str
    region: str

    def as_boto_dict(self) -> Dict[str, str]:
        """Returns config as a dictionary suitable for aiobotocore clients."""
        return {
            "endpoint_url": self.endpoint_url,
            "aws_access_key_id": self.access_key_id,
            "aws_secret_access_key": self.secret_access_key,
            "region_name": self.region,
        }


@dataclass(frozen=True)
class ScriptConfig:
    """
    Top-level configuration for the manifest creation script.

    Attributes:
        db_dsn (str): The PostgreSQL Data Source Name for database connection.
        output_uri (str): The base S3 URI for the output manifest file(s).
        strip_prefix (str): The prefix to remove from database URIs.
        s3 (S3Config): S3 client configuration.
        db_fetch_size (int): Number of records to fetch from the DB in each batch.
        rows_per_file (int): Maximum number of URIs to include in each manifest file.
        limit (int, optional): Optional total number of URIs to process.
        dedup_uri_prefix (str, optional): Optional S3 prefix of existing manifests
            to dedup against.
    """

    db_dsn: str
    output_uri: str
    strip_prefix: str
    s3: S3Config = field(default_factory=S3Config)
    db_fetch_size: int = 50_000
    rows_per_file: int = 1_000_000
    limit: Optional[int] = None
    dedup_uri_prefix: Optional[str] = None


def setup_logging(level: str) -> None:
    """
    Configure rich-based logging for the application.

    Args:
        level (str): The desired logging level (e.g., "INFO", "DEBUG").
    """
    logging.basicConfig(
        level=level.upper(),
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(rich_tracebacks=True, show_path=False)],
    )
    # Silence noisy loggers
    for logger_name in ["botocore", "s3transfer", "urllib3", "asyncpg"]:
        logging.getLogger(logger_name).setLevel(logging.WARNING)


def parse_s3_uri(uri: str) -> Tuple[str, str]:
    """
    Parse an S3 URI into bucket and key components.

    Args:
        uri (str): The S3 URI (e.g., "s3://my-bucket/path/to/file.parquet").

    Returns:
        Tuple[str, str]: A tuple containing the bucket name and the object key.
    """
    parsed: ParseResult = urlparse(uri)
    if parsed.scheme != "s3":
        raise ValueError("Output URI must be a valid S3 URI (e.g., s3://...).")
    bucket: str = parsed.netloc
    key: str = parsed.path.lstrip("/")
    if not bucket or not key:
        raise ValueError("S3 URI must include both a bucket and a key.")
    return bucket, key


async def show_image_count(db_dsn: str) -> None:
    """
    Connects to the DB, counts images, and prints the total.

    Args:
        db_dsn (str): The PostgreSQL Data Source Name for database connection.
    """
    logger.info("Connecting to PostgreSQL to count images...")
    conn: Optional[Connection] = None
    try:
        conn = await asyncpg.connect(dsn=db_dsn)
        count: Optional[int] = await conn.fetchval("SELECT COUNT(*) FROM images")
        logger.info(f"Total images in database: {count or 0:,}")
    finally:
        if conn:
            await conn.close()
            logger.info("Database connection closed.")


async def _get_existing_uris(
    s3_client: S3Client,
    config: ScriptConfig,
) -> Set[str]:
    """
    Downloads existing manifests from S3 and returns a set of unique URIs.

    Args:
        s3_client (S3Client): An initialized aiobotocore S3 client.
        config (ScriptConfig): The script's configuration, containing S3 details.

    Returns:
        Set[str]: A set of all unique URIs found in the existing manifests.
    """
    if not config.dedup_uri_prefix:
        return set()

    logger.info(f"Scanning for existing manifests under '{config.dedup_uri_prefix}'...")

    bucket: str
    prefix: str
    bucket, prefix = parse_s3_uri(config.dedup_uri_prefix)
    paginator: ListObjectsV2Paginator = s3_client.get_paginator("list_objects_v2")
    pages: AsyncIterator[ListObjectsV2OutputTypeDef] = paginator.paginate(
        Bucket=bucket, Prefix=prefix
    )

    manifest_keys: List[str] = []
    async for page in pages:
        for obj in page.get("Contents", []):
            key: Optional[str] = obj.get("Key")
            if key and key.endswith(".parquet"):
                manifest_keys.append(f"s3://{bucket}/{key}")

    if not manifest_keys:
        logger.warning("No existing manifests found at the specified prefix.")
        return set()

    logger.info(f"Found {len(manifest_keys)} existing manifest files. Reading URIs...")
    storage_options: Dict[str, str] = config.s3.as_boto_dict()

    try:
        lazy_df: pl.LazyFrame = pl.scan_parquet(
            manifest_keys, storage_options=storage_options
        )
        existing_uris_series: pl.Series = lazy_df.select("uri").collect()["uri"]
        unique_uris: Set[str] = set(existing_uris_series.to_list())
        logger.info(f"Loaded {len(unique_uris):,} unique URIs for deduplication.")
        return unique_uris
    except ImportError:
        logger.critical(
            "Deduplication requires 's3fs' and 'pyarrow'. "
            "Please install them: pip install s3fs pyarrow"
        )
        sys.exit(1)
    except Exception as e:
        logger.error(f"Failed to read existing manifests from S3: {e}")
        raise


async def fetch_image_uris(
    conn: asyncpg.Connection,
    fetch_size: int,
    strip_prefix: str,
    existing_uris: Optional[Set[str]] = None,
) -> AsyncIterator[List[str]]:
    """
    Fetches image storage keys from the database, optionally filtering seen URIs.

    Args:
        conn (asyncpg.Connection): An active asyncpg connection.
        fetch_size (int): The number of records to fetch from the cursor per batch.
        strip_prefix (str): The prefix to remove from the start of each URI.
        existing_uris (Set[str], optional): A set of URIs to exclude from the results.

    Yields:
        List[str]: An iterator yielding lists of image keys ('storage_key_thumbnail').
    """
    async with conn.transaction():
        # Use a server-side cursor for memory efficiency
        cursor: Cursor = await conn.cursor("SELECT storage_key_thumbnail FROM images")
        while batch := await cursor.fetch(fetch_size):
            # Strip the prefix from each URI
            stripped_uris: List[str] = [
                r["storage_key_thumbnail"].removeprefix(strip_prefix) for r in batch
            ]
            if existing_uris is not None:
                new_uris: List[str] = [
                    uri for uri in stripped_uris if uri not in existing_uris
                ]
                if new_uris:
                    yield new_uris
            else:
                yield stripped_uris


async def _write_and_upload_chunk(
    uris: List[str],
    s3_client: S3Client,
    bucket: str,
    key: str,
) -> None:
    """
    Builds a Parquet file from a list of URIs and uploads it to S3.

    Args:
        uris (List[str]): A list of URI strings to write to the file.
        s3_client (S3Client): An initialized aiobotocore S3 client.
        bucket (str): The destination S3 bucket name.
        key (str): The destination S3 object key.
    """
    if not uris:
        return

    logger.info(f"Generating Parquet chunk with {len(uris):,} rows for '{key}'...")
    df: pl.DataFrame = pl.DataFrame({"uri": uris})
    buffer: io.BytesIO = io.BytesIO()
    df.write_parquet(buffer, compression="zstd")
    buffer.seek(0)

    logger.info(f"Uploading manifest chunk to 's3://{bucket}/{key}'...")
    await s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=buffer,
        ContentLength=len(buffer.getvalue()),
    )
    logger.debug(f"Successfully uploaded chunk 's3://{bucket}/{key}'.")


async def generate_manifests(config: ScriptConfig) -> None:
    """
    Connects to DB, generates chunked Parquet manifests, and uploads to S3.

    Args:
        config (ScriptConfig): The script's configuration object.
    """
    logger.info("Connecting to PostgreSQL database...")
    conn: Optional[Connection] = None
    try:
        conn = await asyncpg.connect(dsn=config.db_dsn)
        logger.info("Database connection successful.")

        bucket: str
        base_key: str
        bucket, base_key = parse_s3_uri(config.output_uri)

        p: PurePath = PurePath(base_key)
        key_stem: str = p.stem
        key_suffix: str = p.suffix
        key_dir: str = str(p.parent)
        key_dir = "" if key_dir == "." else f"{key_dir}/"

        session: AioSession = aiobotocore.session.get_session()
        async with session.create_client("s3", **config.s3.as_boto_dict()) as s3_client:
            existing_uris: Set[str] = await _get_existing_uris(s3_client, config)
            uri_iterator: AsyncIterator[List[str]] = fetch_image_uris(
                conn,
                config.db_fetch_size,
                config.strip_prefix,
                existing_uris,
            )

            uris_chunk: List[str] = []
            part_num: int
            total_uris_written: int
            limit: Optional[int]
            part_num, total_uris_written = 0, 0
            limit = config.limit
            total_uris_processed: int = 0

            logger.info("Fetching URIs and creating manifest chunks...")
            try:
                async for batch in uri_iterator:
                    uris_to_add: List[str] = batch
                    if limit is not None:
                        remaining_capacity: int = limit - total_uris_processed
                        if remaining_capacity <= 0:
                            break
                        uris_to_add: List[str] = batch[:remaining_capacity]

                    uris_chunk.extend(uris_to_add)
                    total_uris_processed += len(uris_to_add)

                    while len(uris_chunk) >= config.rows_per_file:
                        chunk_to_write: List[str] = uris_chunk[: config.rows_per_file]
                        uris_chunk: List[str] = uris_chunk[config.rows_per_file :]
                        chunk_key: str = (
                            f"{key_dir}{key_stem}_{part_num:04d}{key_suffix}"
                        )
                        await _write_and_upload_chunk(
                            chunk_to_write, s3_client, bucket, chunk_key
                        )
                        total_uris_written += len(chunk_to_write)
                        part_num += 1
            finally:
                await uri_iterator.aclose()

            if uris_chunk:
                chunk_key: str = f"{key_dir}{key_stem}_{part_num:04d}{key_suffix}"
                await _write_and_upload_chunk(uris_chunk, s3_client, bucket, chunk_key)
                total_uris_written += len(uris_chunk)
                part_num += 1

            if total_uris_written == 0:
                logger.warning("No new image records found. No manifest created.")
            else:
                logger.info(
                    f"Finished. Wrote {total_uris_written:,} URIs across "
                    f"{part_num} manifest file(s)."
                )
    finally:
        if conn:
            await conn.close()
            logger.info("Database connection closed.")


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option(
    "--output-uri",
    type=str,
    help="S3 URI for the output manifest file(s) (e.g., s3://bucket/manifest.parquet).",
)
@click.option(
    "--db-dsn-env",
    default="DATABASE_URL",
    show_default=True,
    help="Environment variable name for the PostgreSQL DSN.",
)
@click.option(
    "--strip-prefix",
    type=str,
    help="The prefix to remove from the start of each URI.",
)
@click.option(
    "--count", is_flag=True, help="Only count total images in the DB and exit."
)
@click.option(
    "--rows-per-file",
    default=1_000_000,
    show_default=True,
    type=int,
    help="Maximum number of URIs per manifest file.",
)
@click.option(
    "--limit",
    type=int,
    default=None,
    help="Limit the total number of URIs to process. Useful for testing.",
)
@click.option(
    "--dedup-uri-prefix",
    type=str,
    default=None,
    help="S3 prefix of existing manifests to deduplicate against.",
)
@click.option(
    "--log-level",
    default="INFO",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"], case_sensitive=False),
    help="Set the logging level.",
    show_default=True,
)
def cli(**kwargs: Any) -> None:
    """
    Generates a Parquet manifest of image URIs and uploads it to S3.

    This script queries the 'images' table for all 'storage_key_thumbnail' values,
    compiles them into one or more Parquet files, and uploads them.

    Required environment variables for S3 access:
    - LAGOON_SOURCE_ENDPOINT_URL
    - LAGOON_SOURCE_ACCESS_KEY_ID
    - LAGOON_SOURCE_SECRET_ACCESS_KEY
    - LAGOON_SOURCE_REGION (optional, defaults to us-east-1)

    Required environment variable for database access:
    - DATABASE_URL (or as specified by --db-dsn-env)
    """
    load_dotenv()
    setup_logging(kwargs["log_level"])

    try:
        db_dsn: str = _get_env_var(kwargs["db_dsn_env"])

        if kwargs["count"]:
            asyncio.run(show_image_count(db_dsn))
            return

        output_uri: Optional[str] = kwargs.get("output_uri")
        strip_prefix: Optional[str] = kwargs.get("strip_prefix")
        if not output_uri or not strip_prefix:
            raise click.UsageError(
                "Missing required options. '--output-uri' and '--strip-prefix' "
                "are required when not using '--count'."
            )

        s3_config: S3Config = S3Config(
            endpoint_url=_get_env_var("LAGOON_SOURCE_ENDPOINT_URL"),
            access_key_id=_get_env_var("LAGOON_SOURCE_ACCESS_KEY_ID"),
            secret_access_key=_get_env_var("LAGOON_SOURCE_SECRET_ACCESS_KEY"),
            region=_get_env_var("LAGOON_SOURCE_REGION", "us-east-1"),
        )

        config: ScriptConfig = ScriptConfig(
            db_dsn=db_dsn,
            output_uri=output_uri,
            s3=s3_config,
            strip_prefix=strip_prefix,
            rows_per_file=kwargs["rows_per_file"],
            limit=kwargs["limit"],
            dedup_uri_prefix=kwargs["dedup_uri_prefix"],
        )

        asyncio.run(generate_manifests(config))

        logger.info("✅ Manifest creation script completed successfully.")
    except (ValueError, KeyError, click.UsageError) as e:
        logger.critical(f"Error: {e}")
        sys.exit(1)
    except Exception:
        logger.critical("An unexpected error occurred:", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    cli()
