import pytest
from unittest.mock import patch, MagicMock
from src.export.monthly_export_pipeline import (
    run_import_statement,
    run_sync_database,
    run_export_to_sheets,
    run_generate_summaries,
    main
)

@patch("src.export.monthly_export_pipeline.import_main")
def test_run_import_statement(mock_import_main):
    mock_import_main.return_value = 0
    with patch("sys.argv", ["pipeline.py"]):
        assert run_import_statement("data.csv", "2026-01-01", "2026-01-31", dry_run=True) is True

@patch("src.export.monthly_export_pipeline.sync_from_splitwise")
def test_run_sync_database(mock_sync):
    mock_sync.return_value = {"updated": 1, "inserted": 1, "marked_deleted": 0}
    assert run_sync_database(2026) is True
    mock_sync.assert_called_once()

@patch("src.export.monthly_export_pipeline.export_main")
def test_run_export_to_sheets(mock_export):
    mock_export.return_value = 0
    with patch("sys.argv", ["splitwise_export.py"]):
        assert run_export_to_sheets(2026, append_only=True) is True

@patch("src.export.monthly_export_pipeline.summaries_main")
def test_run_generate_summaries(mock_summaries):
    mock_summaries.return_value = 0
    with patch("sys.argv", ["generate_summaries.py"]):
        assert run_generate_summaries(2026) is True

@patch("src.export.monthly_export_pipeline.run_sync_database")
@patch("src.export.monthly_export_pipeline.run_export_to_sheets")
@patch("src.export.monthly_export_pipeline.run_generate_summaries")
def test_main_sync_only(mock_summaries, mock_export, mock_sync):
    mock_sync.return_value = True
    mock_export.return_value = True
    mock_summaries.return_value = True
    
    with patch("sys.argv", ["script", "--year", "2026", "--sync-only"]):
        assert main() == 0
        mock_sync.assert_called_once()
        mock_export.assert_called_once()
        mock_summaries.assert_called_once()
