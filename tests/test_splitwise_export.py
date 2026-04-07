import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from src.export.splitwise_export import fetch_from_database, fetch_and_write, SOURCE_DATABASE, SOURCE_SPLITWISE
from src.database.models import Transaction

@patch("src.export.splitwise_export.get_current_user_name")
def test_fetch_from_database(mock_user):
    mock_user.return_value = ""
    with patch("src.export.splitwise_export.DatabaseManager") as MockDB:
        mock_db = MagicMock()
        MockDB.return_value = mock_db
        
        txn = Transaction(date="2026-04-01", amount=50.0, merchant="Merch", cc_reference_id="ref123", imported_at="2026-04-01", source="amex")
        txn.category = "Test"
        txn.description = "Test Desc"
        txn.split_type = "split"
        txn.notes = "Paid: $50.0 | Owe: $25.0"
        
        mock_db.get_unwritten_transactions.return_value = [txn]
        
        df = fetch_from_database("2026-01-01", "2026-12-31", year=2026, include_written=False)
        assert len(df) == 1
        
        from src.constants.export_columns import ExportColumns
        assert df.iloc[0][ExportColumns.AMOUNT] == 50.0
        assert df.iloc[0][ExportColumns.MY_PAID] == 50.0
        assert df.iloc[0][ExportColumns.MY_OWED] == 25.0
        assert df.iloc[0][ExportColumns.MY_NET] == 25.0

@patch("src.export.splitwise_export.fetch_from_database")
def test_fetch_and_write_database_dry_run(mock_fetch):
    mock_df = pd.DataFrame([{"Amount": 50.0, "Date": "2026-04-01"}])
    mock_fetch.return_value = mock_df
    
    df, url = fetch_and_write("2026-01-01", "2026-12-31", source=SOURCE_DATABASE, dry_run=True, append_only=True)
    assert len(df) == 1
    assert url is None

@patch("src.export.splitwise_export.SplitwiseClient")
@patch("src.export.splitwise_export.load_exported_state")
def test_fetch_and_write_splitwise_dry_run(mock_load_state, MockClient):
    from src.constants.export_columns import ExportColumns
    mock_load_state.return_value = (set(), set())
    mock_client = MagicMock()
    MockClient.return_value = mock_client
    
    mock_df = pd.DataFrame([{
        ExportColumns.DATE: "2026-04-01",
        ExportColumns.AMOUNT: 10.0,
        ExportColumns.DESCRIPTION: "Uber",
        ExportColumns.MY_PAID: 10.0,
        ExportColumns.MY_OWED: 10.0,
        ExportColumns.ID: 101,
        ExportColumns.CATEGORY: "Transportation"
    }])
    mock_client.get_my_expenses_by_date_range.return_value = mock_df
    
    df, url = fetch_and_write("2026-01-01", "2026-12-31", source=SOURCE_SPLITWISE, dry_run=True)
    assert len(df) == 1
    assert ExportColumns.FINGERPRINT in df.columns
