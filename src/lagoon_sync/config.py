# src/lagoon_sync/config.py
"""
Configuration for the lagoon-sync pipeline.

This module centralizes all configuration, loading sensitive values from
environment variables and providing typed dataclasses for use throughput
the application.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Optional

from lagoon_sync.exceptions import ConfigError


def _get_env_var(name: str, default: Optional[str] = None) -> str:
    """
    Retrieves a required environment variable.

    Args:
        name (str): The name of the environment variable.
        default (str, optional): The default value if the variable is not set.

    Returns:
        str: The value of the environment variable.
    """
    value: Optional[str] = os.environ.get(name, default)
    if not value:
        raise ConfigError(f"Environment variable '{name}' must be set.")
    return value


@dataclass(frozen=True)
class S3Config:
    """
    Represents the configuration for an S3-compatible endpoint.

    Attributes:
        endpoint_url (str): The S3 endpoint URL.
        access_key_id (str): The access key ID.
        secret_access_key (str): The secret access key.
        bucket (str): The bucket name.
        region (str): The AWS region.
    """

    endpoint_url: str
    access_key_id: str
    secret_access_key: str
    bucket: str
    region: str

    def as_boto_dict(self) -> Dict[str, str]:
        """
        Returns the configuration as a dictionary suitable for boto3/aiobotocore clients.

        Returns:
            Dict[str, str]: A dictionary of client parameters.
        """
        return {
            "endpoint_url": self.endpoint_url,
            "aws_access_key_id": self.access_key_id,
            "aws_secret_access_key": self.secret_access_key,
            "region_name": self.region,
        }


@dataclass(frozen=True)
class AppConfig:
    """
    Defines the application's operational parameters

    Attributes:
        data_dir (Path): Directory to store the LMDB progress database.
        db_map_size_gb (int): The maximum size of the database in gigabytes.
        manifest_prefix (str): S3 prefix to scan for manifest files.
        manifest_column_name (str): Column name for object keys in manifests.
        min_concurrency (int): The minimum number of concurrent transfers.
        max_concurrency (int): The maximum number of concurrent transfers.
        transfer_max_attempts (int): Max attempts for a single object transfer.
        controller_enabled (bool): Whether to enabled the congestion controller.
        controller_interval_s (int): Interval for controller adjustments in seconds.
        controller_error_rate_threshold (float): Error rate to trigger decrease.
        controller_rtt_spike_factor (float): RTT multiplier to trigger decrease.
        controller_decrease_factor (float): Multiplier for concurrency decrease.
        controller_increase_amount (int): Additive amount for concurrency increase.
    """

    data_dir: Path = field(default_factory=lambda: Path("data"))
    db_map_size_gb: int = 20
    manifest_prefix: str = "manifests/"
    manifest_column_name: str = "uri"
    min_concurrency: int = 16
    max_concurrency: int = 1024
    transfer_max_attempts: int = 5
    controller_enabled: bool = True
    controller_interval_s: int = 10
    controller_error_rate_threshold: float = 0.1
    controller_rtt_spike_factor: float = 3.0
    controller_decrease_factor: float = 0.95
    controller_increase_amount: int = 32


@dataclass(frozen=True)
class Config:
    """
    Top-level configuration container for the entire application.

    Attributes:
        source (S3Config): Configuration for the source S3-compatible service.
        destination (S3Config): Configuration for the destination S3-compatible service.
        app (AppConfig): General application settings.
    """

    source: S3Config = field(
        default_factory=lambda: S3Config(
            endpoint_url=_get_env_var("LAGOON_SOURCE_ENDPOINT_URL"),
            access_key_id=_get_env_var("LAGOON_SOURCE_ACCESS_KEY_ID"),
            secret_access_key=_get_env_var("LAGOON_SOURCE_SECRET_ACCESS_KEY"),
            bucket=_get_env_var("LAGOON_SOURCE_BUCKET"),
            region=_get_env_var("LAGOON_SOURCE_REGION", "us-east-1"),
        )
    )
    destination: S3Config = field(
        default_factory=lambda: S3Config(
            endpoint_url=_get_env_var("LAGOON_DESTINATION_ENDPOINT_URL"),
            access_key_id=_get_env_var("LAGOON_DESTINATION_ACCESS_KEY_ID"),
            secret_access_key=_get_env_var("LAGOON_DESTINATION_SECRET_ACCESS_KEY"),
            bucket=_get_env_var("LAGOON_DESTINATION_BUCKET"),
            region=_get_env_var("LAGOON_DESTINATION_REGION", "us-east-1"),
        )
    )
    app: AppConfig = field(default_factory=AppConfig)
