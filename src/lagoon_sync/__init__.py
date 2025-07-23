# src/lagoon_sync/__init__.py
"""
lagoon-sync: An efficient, resumable, and auto-tuning S3 to S3 replicator.

This package provides a robust tool for mirroring object from a source S3-compatible
bucket to a destination, with manifest support, persistent progress tracking
and automatic concurrency management.

The primary entry point for programmatic use is the `LagoonSyncPipeline` class.
"""

from typing import List

from lagoon_sync.pipeline import LagoonSyncPipeline

__all__: List[str] = ["LagoonSyncPipeline"]
