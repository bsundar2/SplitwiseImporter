import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
import sys
from src.export.splitwise_export import (
    main,
)

@patch("src.export.splitwise_export.fetch_and_write")
def test_main_cli_api(mock_fetch):
    mock_fetch.return_value = (pd.DataFrame([{"id": 1}]), "http://test")
    with patch("sys.argv", ["script", "--start-date", "2026-01-01", "--end-date", "2026-12-31", "--sheet-key", "test"]):
        assert main() == 0
        mock_fetch.assert_called_once()

@patch("src.export.splitwise_export.fetch_and_write")
def test_main_cli_db(mock_fetch):
    mock_fetch.return_value = (pd.DataFrame([{"id": 1}]), "http://test")
    # SOURCE_DATABASE is "database"
    with patch("sys.argv", ["script", "--source", "database", "--year", "2026", "--sheet-key", "test"]):
        assert main() == 0
        mock_fetch.assert_called_once()

@patch("src.export.splitwise_export.fetch_and_write")
def test_main_cli_dry_run(mock_fetch):
    mock_fetch.return_value = (pd.DataFrame([{"id": 1}]), None)
    with patch("sys.argv", ["script", "--year", "2026", "--dry-run"]):
        assert main() == 0
        mock_fetch.assert_called_once()
        # Should work even without sheet-key in dry-run
