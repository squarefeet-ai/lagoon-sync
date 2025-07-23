# src/lagoon_sync/cli.py
"""Command-line interface for the lagoon-sync tool."""

import asyncio
import logging
import sys
from pathlib import Path
from typing import Any

import click
from dotenv import load_dotenv
from rich.logging import RichHandler

from lagoon_sync.config import AppConfig, Config
from lagoon_sync.exceptions import LagoonSyncError
from lagoon_sync.signals import GracefulShutdown

logger: logging.Logger = logging.getLogger(__name__)


def setup_logging(level: str) -> None:
    """Configure rich-based logging for the application."""
    logging.basicConfig(
        level=level.upper(),
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(rich_tracebacks=True, show_path=False)],
    )
    # Silence noisy loggers
    for logger_name in ["botocore", "s3transfer", "urllib3"]:
        logging.getLogger(logger_name).setLevel(logging.WARNING)


async def main_async(config: Config) -> None:
    """
    Asynchronously execute the sync pipeline.

    Args:
        config (Config): The application configuration.
    """
    # Lazily import to avoid circular dependency and keep CLI fast
    from lagoon_sync.pipeline import LagoonSyncPipeline

    shutdown_manager: GracefulShutdown = GracefulShutdown()
    async with shutdown_manager as shutdown_event:
        pipeline: LagoonSyncPipeline = LagoonSyncPipeline(config, shutdown_event)
        await pipeline.run()


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option(
    "--data-dir",
    type=click.Path(file_okay=False, dir_okay=True, writable=True, resolve_path=True),
    default="data",
    help="Directory to store persistent progress databases.",
    show_default=True,
)
@click.option(
    "--max-concurrency",
    type=int,
    default=512,
    help="Maximum number of concurrent transfers.",
    show_default=True,
)
@click.option(
    "--no-controller",
    is_flag=True,
    default=False,
    help="Disable the dynamic congestion controller.",
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
    A resumable and auto-tuning S3 replicator.

    This tool streams objects defined in Parquet manifest from a source
    bucket to a destination bucket. It uses a local database to track
    progress, allowing it to be stopped and resumed without re-copying
    completed files.

    For optimal performance, it dynamically adjust concurrency based
    on network conditions.

    Credentials and bucket information must be set via environment variables.
    See the .env.example file for required variables.
    """
    load_dotenv()
    setup_logging(kwargs["log_level"])

    try:
        app_config: AppConfig = AppConfig(
            data_dir=Path(kwargs["data_dir"]),
            max_concurrency=kwargs["max_concurrency"],
            controller_enabled=not kwargs["no_controller"],
        )
        config: Config = Config(app=app_config)

        asyncio.run(main_async(config))
        logger.info("✅ Run completed successfully.")
    except LagoonSyncError as e:
        logger.critical(f"A critical application error occurred: {e}")
        sys.exit(1)
    except asyncio.CancelledError:
        logger.warning("Shutdown signal received. Exiting.")
    except Exception:
        logger.critical(
            "An unexpected error caused the application to fail:", exc_info=True
        )
        sys.exit(1)


if __name__ == "__main__":
    cli()
