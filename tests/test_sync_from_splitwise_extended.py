import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
import sys
from src.db_sync.sync_from_splitwise import (
    parse_expense_to_transaction,
    sync_from_splitwise,
    main,
)
from src.constants.export_columns import ExportColumns

@pytest.fixture
def sample_sw_df():
    return pd.DataFrame([
        {
            ExportColumns.ID: 101,
            ExportColumns.DATE: "2026-04-01T00:00:00Z",
            ExportColumns.DESCRIPTION: "Amazon",
            ExportColumns.AMOUNT: 100.0,
            ExportColumns.MY_PAID: 100.0,
            ExportColumns.MY_OWED: 100.0,
            ExportColumns.MY_NET: 0.0,
            ExportColumns.CATEGORY: "Shopping",
            ExportColumns.SPLIT_TYPE: "self",
            ExportColumns.DETAILS: "ref123",
            ExportColumns.PARTICIPANT_NAMES: "Me",
        }
    ])

def test_parse_expense_to_transaction(sample_sw_df):
    row = sample_sw_df.iloc[0].to_dict()
    txn = parse_expense_to_transaction(row)
    assert txn.splitwise_id == 101
    assert txn.cc_reference_id == "ref123"
    assert txn.amount == 0.0
    assert txn.is_refund is False

@patch("src.db_sync.sync_from_splitwise.DatabaseManager")
@patch("src.db_sync.sync_from_splitwise.SplitwiseClient")
def test_sync_from_splitwise_new_and_deleted(mock_client_cls, mock_db_cls, sample_sw_df):
    mock_db = mock_db_cls.return_value
    # No existing transactions in DB
    mock_db.get_transactions_with_splitwise_ids.return_value = []
    
    mock_client = mock_client_cls.return_value
    mock_client.get_my_expenses_by_date_range.return_value = sample_sw_df
    
    stats = sync_from_splitwise("2026-01-01", "2026-12-31", dry_run=False)
    assert stats["inserted"] == 1
    assert stats["marked_deleted"] == 0
    mock_db.insert_transactions_batch.assert_called_once()

@patch("src.db_sync.sync_from_splitwise.DatabaseManager")
@patch("src.db_sync.sync_from_splitwise.SplitwiseClient")
def test_sync_from_splitwise_updated(mock_client_cls, mock_db_cls, sample_sw_df):
    mock_db = mock_db_cls.return_value
    # Existing transaction in DB with different amount
    from src.database.models import Transaction
    existing_txn = Transaction(
        id=1, splitwise_id=101, amount=50.0, merchant="Amazon", date="2026-04-01",
        source="splitwise", imported_at="2026-04-01T00:00:00Z",
        category="Old", split_type="self", cc_reference_id="ref123", notes="Old notes"
    )
    mock_db.get_transactions_with_splitwise_ids.return_value = [existing_txn]
    
    mock_client = mock_client_cls.return_value
    mock_client.get_my_expenses_by_date_range.return_value = sample_sw_df
    
    stats = sync_from_splitwise("2026-01-01", "2026-12-31", dry_run=False)
    assert stats["updated"] == 1
    mock_db.update_transaction.assert_called_once()

@patch("src.db_sync.sync_from_splitwise.sync_from_splitwise")
def test_main_cli(mock_sync):
    mock_sync.return_value = {
        "checked": 1, "inserted": 0, "updated": 0,
        "marked_deleted": 0, "unchanged": 1, "errors": 0
    }
    with patch("sys.argv", ["script", "--year", "2026", "--live"]):
        main()
        mock_sync.assert_called_once()
