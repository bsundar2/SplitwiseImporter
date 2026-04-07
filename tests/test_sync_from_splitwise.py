import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
from datetime import datetime

from src.db_sync.sync_from_splitwise import parse_expense_to_transaction, sync_from_splitwise
from src.constants.export_columns import ExportColumns
from src.constants.splitwise import SPLIT_TYPE_SPLIT
from src.database.models import Transaction

def test_parse_expense_to_transaction():
    row = {
        ExportColumns.ID: 999,
        ExportColumns.DATE: "2026-04-01T12:00:00Z",
        ExportColumns.DESCRIPTION: "Netflix",
        ExportColumns.DETAILS: "ref123",
        ExportColumns.AMOUNT: 15.0,
        ExportColumns.MY_PAID: 15.0,
        ExportColumns.MY_OWED: 7.5,
        ExportColumns.MY_NET: 7.5,
        ExportColumns.CATEGORY: "Entertainment",
        ExportColumns.SPLIT_TYPE: SPLIT_TYPE_SPLIT,
        ExportColumns.PARTICIPANT_NAMES: "Me, You"
    }

    txn = parse_expense_to_transaction(row)
    assert txn.splitwise_id == 999
    assert txn.date == "2026-04-01"
    assert txn.merchant == "Netflix"
    assert txn.cc_reference_id == "ref123"
    assert txn.amount == 7.5  # my_net
    assert txn.raw_amount == 15.0
    assert txn.category == "Entertainment"
    assert "Paid: $15.00 | Owe: $7.50 | With: Me, You" in txn.notes

@patch("src.db_sync.sync_from_splitwise.DatabaseManager")
@patch("src.db_sync.sync_from_splitwise.SplitwiseClient")
def test_sync_from_splitwise(MockClient, MockDB):
    # Setup mock Client
    mock_client = MagicMock()
    MockClient.return_value = mock_client
    
    mock_df = pd.DataFrame([{
        ExportColumns.ID: 101,
        ExportColumns.DATE: "2026-04-01",
        ExportColumns.DESCRIPTION: "Uber",
        ExportColumns.MY_NET: 10.0,
    }])
    mock_client.get_my_expenses_by_date_range.return_value = mock_df

    # Setup mock DB
    mock_db = MagicMock()
    MockDB.return_value = mock_db
    
    # Returning empty DB, should insert 1
    mock_db.get_transactions_with_splitwise_ids.return_value = []
    
    stats = sync_from_splitwise("2026-04-01", "2026-04-30", dry_run=False)
    
    assert stats["inserted"] == 1
    assert stats["updated"] == 0
    assert stats["marked_deleted"] == 0
    mock_db.insert_transactions_batch.assert_called_once()
    assert len(mock_db.insert_transactions_batch.call_args[0][0]) == 1

@patch("src.db_sync.sync_from_splitwise.DatabaseManager")
@patch("src.db_sync.sync_from_splitwise.SplitwiseClient")
def test_sync_from_splitwise_update_and_delete(MockClient, MockDB):
    mock_client = MagicMock()
    MockClient.return_value = mock_client
    
    # 1. Provide an updated item from Splitwise (amount changed to 20.0)
    mock_df = pd.DataFrame([{
        ExportColumns.ID: 101,
        ExportColumns.DATE: "2026-04-01",
        ExportColumns.DESCRIPTION: "Uber",
        ExportColumns.MY_NET: 20.0, # changed
        ExportColumns.AMOUNT: 40.0
    }])
    mock_client.get_my_expenses_by_date_range.return_value = mock_df

    # 2. Existing DB txns: 101 (Uber for 10.0), 102 (Deleted item)
    existing_101 = Transaction(date="2026-04-01", amount=10.0, merchant="Uber", source="splitwise", splitwise_id=101, imported_at="2026-04-01")
    existing_102 = Transaction(date="2026-04-01", amount=5.0, merchant="Deleted", source="splitwise", splitwise_id=102, imported_at="2026-04-01")
    # need an ID for DB updates logic to work
    existing_101.id = 1
    existing_102.id = 2

    mock_db = MagicMock()
    MockDB.return_value = mock_db
    mock_db.get_transactions_with_splitwise_ids.return_value = [existing_101, existing_102]
    
    stats = sync_from_splitwise("2026-04-01", "2026-04-30", dry_run=False)
    
    assert stats["inserted"] == 0
    assert stats["updated"] == 1
    assert stats["marked_deleted"] == 1
    
    mock_db.update_transaction.assert_called_once()
    mock_db.mark_deleted_by_splitwise_id.assert_called_once_with(102)
