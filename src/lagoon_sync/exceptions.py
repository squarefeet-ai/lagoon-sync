# src/lagoon_sync/exceptions.py
"""Custom exceptions for the lagoon-sync application."""


class LagoonSyncError(Exception):
    """Base exception for all application-specific errors."""

    pass


class ConfigError(LagoonSyncError):
    """Raised for configuration-related issues."""

    pass


class TransferError(LagoonSyncError):
    """Raised when an object transfer fails permanently."""

    pass


class LMDBError(LagoonSyncError):
    """Raised for LMDB-specific errors, like the database being full."""

    pass
