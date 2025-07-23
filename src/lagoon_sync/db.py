# src/lagoon_sync/db.py
"""
Handles persistent progress tracking using an LMDB database.

LMDB is chosen for its high performance, transactional integrity, and
concurrency safety, making it ideal for tracking the status of millions
of file transfers without write contention.
"""

import logging
import shutil
from enum import Enum
from pathlib import Path
from typing import Optional

import lmdb

from lagoon_sync.exceptions import LagoonSyncError, LMDBError

logger: logging.Logger = logging.getLogger(__name__)


def _check_disk_space(check_path: Path, required_bytes: int) -> None:
    """
    Verify that there is enough disk space fro the LMDB map size.

    Args:
        check_path (Path): The path to the directory to check for space.
        required_bytes (int): The configured map size in bytes.
    """
    # Accept either a directory or a file path and test the correct location
    path_to_check: Path = check_path if check_path.is_dir() else check_path.parent
    free_space: int = shutil.disk_usage(path_to_check).free
    if free_space < required_bytes:
        raise LagoonSyncError(
            f"Insufficient disk space for LMDB. Required: "
            f"{required_bytes / 1024**3:.2f} GiB, "
            f"Available: {free_space / 1024**3:.2f} GiB on '{check_path}'. "
        )


class ObjectStatus(Enum):
    """Enumeration for the transfer status of an object."""

    COMPLETED = b"C"
    FAILED = b"F"


class ProgressDB:
    """
    A wrapper around an LMDB environment for tracking object transfer status.

    This class provides a simple, typed interface for setting and getting
    the status of individual objects, abstracting the underlying key-value
    store operations.
    """

    def __init__(self, db_path: Path, map_size_gb=20) -> None:
        """
        Initializes and opens the LMDB environment.

        Args:
            db_path (Path): The file path for the LMDB database.
            map_size_gb (int): The maximum size of the database in gigabytes.
        """
        self._env: Optional[lmdb.Environment] = None
        map_size: int = map_size_gb * 1024**3
        try:
            db_dir: Path = db_path.parent
            db_dir.mkdir(parents=True, exist_ok=True)
            _check_disk_space(db_dir, map_size)
            self._env = lmdb.open(str(db_path), map_size=map_size, writemap=True)
            logger.info(f"Progress database opened at '{db_path}'")
        except lmdb.Error as e:
            logger.error(f"Failed to open LMDB database at '{db_path}': {e}")
            raise LagoonSyncError(f"LMDB initialization failed: {e}") from e

    def get_status(self, key: str) -> Optional[ObjectStatus]:
        """
        Retrieves the status of an object from the database.

        Args:
            key (str): The object key to look up.

        Returns:
            Optional[ObjectStatus]: The status if the key exists, else None.
        """
        if not self._env:
            raise LMDBError("LMDB environment is not open.")
        with self._env.begin() as txn:
            value: Optional[bytes] = txn.get(key.encode("utf-8"))
            return ObjectStatus(value) if value else None

    def set_status(self, key: str, status: ObjectStatus) -> None:
        """
        Sets the status of an object in the database.

        Args:
            key (str): The object key to update.
            status (ObjectStatus): The new status to set.
        """
        if not self._env:
            raise LMDBError("LMDB environment is not open.")
        try:
            with self._env.begin(write=True) as txn:
                txn.put(key.encode("utf-8"), status.value)
        except lmdb.MapFullError as e:
            raise LMDBError("LMDB database is full.") from e

    def is_completed(self, key: str) -> bool:
        """
        Checks if an object's transfer is marked as completed.

        Args:
            key (str): The object key to check.

        Returns:
            bool: True if the object is marked as `COMPLETED`, False otherwise.
        """
        return self.get_status(key) == ObjectStatus.COMPLETED

    def close(self) -> None:
        """Closes the LMDB environment."""
        if self._env:
            db_path: str = self._env.path()
            self._env.sync(True)  # Force flush to disk for durability
            self._env.close()
            self._env = None
            logger.info(f"Progress database closed at '{db_path}'.")
