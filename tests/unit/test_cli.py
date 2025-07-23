# tests/unit/test_cli.py
"""Unit tests for the command-line interface."""

import os
from unittest.mock import patch

import pytest
from click.testing import CliRunner, Result

from lagoon_sync.cli import cli


def test_cli_config_error_on_missing_env_var(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """
    Tests that the CLI exits with a status code of 1 on a `ConfigError`.

    Arrange:
        - Ensure a required environment variable is not set.
        - Use a `CliRunner` to invoke the command.
    Act:
        - Run the `cli` command.
    Assert:
        - The exit code is 1.
        - The ouput contains the expected error message about the missing variable.

    Args:
        caplog (pytest.LogCaptureFixture): Pytest fixture to capture log output.
    """
    # Arrange
    runner: CliRunner = CliRunner()
    # Ensure env is clean for this test
    with patch.dict(os.environ):
        if "LAGOON_SOURCE_ENDPOINT_URL" in os.environ:
            del os.environ["LAGOON_SOURCE_ENDPOINT_URL"]

        # Act
        result: Result = runner.invoke(cli)

        # Assert
        assert result.exit_code == 1
        assert (
            "A critical application error occurred: Environment variable "
            "'LAGOON_SOURCE_ENDPOINT_URL' must be set." in caplog.text
        )
