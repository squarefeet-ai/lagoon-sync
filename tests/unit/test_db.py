# tests/unit/test_db.py
"""
Unit tests for the `ProgressDB` component.

These tests validate the functionality of the `ProgressDB` class by interacting
with a real LMDB database created in a temporary directory.
"""

from pathlib import Path

import pytest

from lagoon_sync.db import ObjectStatus, ProgressDB
from lagoon_sync.exceptions import LMDBError


def test_db_initialization_and_cleanup(tmp_path: Path) -> None:
    """
    Tests that a `ProgressDB` can be created, its file exist, and it closes cleanly.

    Arrange:
        - Define a path for the database within a temporary.
    Act:
        - Initialize `ProgressDB`.
    Assert:
        - The LMDB data and lock files are created.
    Act (Part 2):
        - Close the database.
    Assert (Part 2):
        - The internal environment handle is cleared.

    Args:
        tmp_path (Path): The temporary Path to use.
    """
    db_path: Path = tmp_path / "test.lmdb"
    db: ProgressDB = ProgressDB(db_path)
    assert db_path.exists()
    assert (db_path / "lock.mdb").exists()

    db.close()
    assert db._env is None


def test_db_set_get_status(tmp_path: Path) -> None:
    """
    Tests setting and retrieving various object statuses.

    Arrange:
        - Create a `ProgressDB` instance.
    Act:
        - Set the status for several keys.
    Assert:
        - Retrieving each key returns the correct `ObjectStatus` enum.

    Args:
        tmp_path (Path): The temporary Path to use.
    """
    db: ProgressDB = ProgressDB(tmp_path / "progress.lmdb")
    try:
        db.set_status("key/completed", ObjectStatus.COMPLETED)
        db.set_status("key/failed", ObjectStatus.FAILED)

        assert db.get_status("key/completed") == ObjectStatus.COMPLETED
        assert db.get_status("key/failed") == ObjectStatus.FAILED
    finally:
        db.close()


def test_db_get_status_nonexistent(tmp_path: Path) -> None:
    """
    Tests are retrieving a non-existent key returns None.

    Arrange:
        - Create an empty `ProgressDB` instance.
    Act:
        - Attempt to get the status of a key that was never set.
    Assert:
        - The result is None.

    Args:
        tmp_path (Path): The temporary Path to use.
    """
    db: ProgressDB = ProgressDB(tmp_path / "progress.lmdb")
    try:
        assert db.get_status("non/existent/key") is None
    finally:
        db.close()


def test_db_is_completed(tmp_path: Path) -> None:
    """
    Tests the `is_completed` helper method for correctness.

    Arrange:
        - Create a `ProgressDB` instance.
        - Set one key to `COMPLETED` and another to `FAILED`.
    Act/Assert:
        - Check that `is_completed` returns True for the completed key.
        - Check that `is_completed` returns False for the failed key.
        - Check that `is_completed` returns False for a non-existent key.

    Args:
        tmp_path (Path): The temporary Path to use.
    """
    db: ProgressDB = ProgressDB(tmp_path / "progress.lmdb")
    try:
        db.set_status("path/to/success", ObjectStatus.COMPLETED)
        db.set_status("path/to/failure", ObjectStatus.FAILED)

        assert db.is_completed("path/to/success") is True
        assert db.is_completed("path/to/failure") is False
        assert db.is_completed("path/to/nowhere") is False
    finally:
        db.close()


def test_db_overwrite_status(tmp_path: Path) -> None:
    """
    Tests that setting a status for an existing key overwrites the old one.

    Arrange:
        - Create a `ProgressDB` instance and set an initial status for a key.
    Act:
        - Set a new status for the same key.
    Assert:
        - Retrieving the key returns the new, overwritten status.

    Args:
        tmp_path (Path): The temporary Path to use.
    """
    db: ProgressDB = ProgressDB(tmp_path / "progress.lmdb")
    try:
        key: str = "object/to/overwrite"
        db.set_status(key, ObjectStatus.FAILED)
        assert db.get_status(key) == ObjectStatus.FAILED

        db.set_status(key, ObjectStatus.COMPLETED)
        assert db.get_status(key) == ObjectStatus.COMPLETED
    finally:
        db.close()


def test_db_raises_on_closed_env(tmp_path: Path) -> None:
    """
    Tests that operations on a closed database raise an `LMDBError`.

    Arrange:
        - Create and then close a `ProgressDB` instance.
    Act/Assert:
        - Verify that calling `get_status`, `set_status`, and `is_completed`
          on the closed instance raises an LMDBError.

    Args:
        tmp_path (Path): The temporary Path to use.
    """
    db: ProgressDB = ProgressDB(tmp_path / "progress.lmdb")
    db.close()

    with pytest.raises(LMDBError, match="LMDB environment is not open."):
        db.get_status("any_key")

    with pytest.raises(LMDBError, match="LMDB environment is not open."):
        db.set_status("any_key", ObjectStatus.COMPLETED)

    with pytest.raises(LMDBError, match="LMDB environment is not open."):
        db.is_completed("any_key")
