"""Comprehensive tests for splitwise_export.py."""
import pytest
import json
from unittest.mock import patch, MagicMock
import pandas as pd
from datetime import datetime, date

from src.export.splitwise_export import (
    load_exported_state,
    save_exported_state,
    _read_existing_fingerprints,
    fetch_from_database,
    fetch_and_write,
    SOURCE_SPLITWISE,
    SOURCE_DATABASE,
)
from src.constants.export_columns import ExportColumns
from src.database.models import Transaction


# === load/save exported state ===
def test_load_exported_state_empty():
    with patch("src.export.splitwise_export.load_state", return_value={}):
        ids, fps = load_exported_state()
        assert ids == set()
        assert fps == set()

def test_load_exported_state_with_data():
    with patch("src.export.splitwise_export.load_state", return_value={
        "exported_ids": [1, 2], "exported_fingerprints": ["fp1"]
    }):
        ids, fps = load_exported_state()
        assert ids == {1, 2}
        assert fps == {"fp1"}

def test_save_exported_state():
    with patch("src.export.splitwise_export.save_state_atomic") as mock_save:
        save_exported_state({1}, {"fp1"})
        mock_save.assert_called_once()


# === _read_existing_fingerprints ===
def test_read_existing_fingerprints_none():
    assert _read_existing_fingerprints(None, "ws") is None
    assert _read_existing_fingerprints("key", None) is None

@patch("src.export.splitwise_export.read_from_sheets")
def test_read_existing_fingerprints_success(mock_read):
    mock_read.return_value = pd.DataFrame({ExportColumns.FINGERPRINT: ["fp1", "fp2", None]})
    result = _read_existing_fingerprints("key", "ws")
    assert result == ["fp1", "fp2"]

@patch("src.export.splitwise_export.read_from_sheets")
def test_read_existing_fingerprints_no_col(mock_read):
    mock_read.return_value = pd.DataFrame({"other": [1]})
    assert _read_existing_fingerprints("key", "ws") is None


# === fetch_from_database (restored + extended) ===
@patch("src.export.splitwise_export.get_current_user_name")
def test_fetch_from_database(mock_user):
    mock_user.return_value = ""
    with patch("src.export.splitwise_export.DatabaseManager") as MockDB:
        mock_db = MagicMock()
        MockDB.return_value = mock_db
        
        txn = Transaction(date="2026-04-01", amount=50.0, merchant="Merch",
                          cc_reference_id="ref123", imported_at="2026-04-01", source="amex")
        txn.category = "Test"
        txn.description = "Test Desc"
        txn.split_type = "split"
        txn.notes = "Paid: $50.0 | Owe: $25.0"
        
        mock_db.get_unwritten_transactions.return_value = [txn]
        
        df = fetch_from_database("2026-01-01", "2026-12-31", year=2026, include_written=False)
        assert len(df) == 1
        assert df.iloc[0][ExportColumns.AMOUNT] == 50.0
        assert df.iloc[0][ExportColumns.MY_PAID] == 50.0
        assert df.iloc[0][ExportColumns.MY_OWED] == 25.0
        assert df.iloc[0][ExportColumns.MY_NET] == 25.0

@patch("src.export.splitwise_export.get_current_user_name", return_value="")
@patch("src.export.splitwise_export.DatabaseManager")
def test_fetch_from_database_empty(mock_db_cls, mock_user):
    mock_db = MagicMock()
    mock_db_cls.return_value = mock_db
    mock_db.get_transactions_by_date_range.return_value = []
    result = fetch_from_database("2026-01-01", "2026-12-31")
    assert result.empty

@patch("src.export.splitwise_export.get_current_user_name", return_value="")
@patch("src.export.splitwise_export.DatabaseManager")
def test_fetch_from_database_filters_deleted(mock_db_cls, mock_user):
    mock_db = MagicMock()
    mock_db_cls.return_value = mock_db
    txn = Transaction(id=1, date="2026-04-01", amount=50.0, merchant="Del",
                      source="amex", imported_at="now", splitwise_id=1,
                      splitwise_deleted_at="2026-04-05")
    mock_db.get_transactions_by_date_range.return_value = [txn]
    result = fetch_from_database("2026-01-01", "2026-12-31")
    assert result.empty

@patch("src.export.splitwise_export.get_current_user_name", return_value="")
@patch("src.export.splitwise_export.DatabaseManager")
def test_fetch_from_database_refund(mock_db_cls, mock_user):
    mock_db = MagicMock()
    mock_db_cls.return_value = mock_db
    txn = Transaction(id=2, date="2026-04-01", amount=-25.0, merchant="Amazon Refund",
                      source="amex", imported_at="now", description="Return", splitwise_id=2,
                      notes="Paid: $25.0 | Owe: $25.0", is_refund=True, split_type="self")
    mock_db.get_transactions_by_date_range.return_value = [txn]
    result = fetch_from_database("2026-01-01", "2026-12-31")
    assert not result.empty
    assert result.iloc[0][ExportColumns.AMOUNT] < 0

@patch("src.export.splitwise_export.get_current_user_name", return_value="")
@patch("src.export.splitwise_export.DatabaseManager")
def test_fetch_from_database_year_overwrite(mock_db_cls, mock_user):
    mock_db = MagicMock()
    mock_db_cls.return_value = mock_db
    mock_db.get_transactions_by_date_range.return_value = []
    fetch_from_database("2026-01-01", "2026-12-31", year=2026, include_written=True)
    mock_db.get_transactions_by_date_range.assert_called_once()

@patch("src.export.splitwise_export.get_current_user_name", return_value="Me")
@patch("src.export.splitwise_export.DatabaseManager")
def test_fetch_from_database_no_payment_info(mock_db_cls, mock_user):
    mock_db = MagicMock()
    mock_db_cls.return_value = mock_db
    txn = Transaction(id=3, date="2026-04-01", amount=100.0, merchant="Store",
                      source="amex", imported_at="now", description="Purchase",
                      splitwise_id=3, is_shared=False, split_type="self",
                      notes="With: Me")
    mock_db.get_transactions_by_date_range.return_value = [txn]
    result = fetch_from_database("2026-01-01", "2026-12-31")
    assert not result.empty
    # Should default to self: my_paid == my_owed == amount
    assert result.iloc[0][ExportColumns.MY_PAID] == 100.0
    assert result.iloc[0][ExportColumns.MY_OWED] == 100.0

@patch("src.export.splitwise_export.get_current_user_name", return_value="Me")
@patch("src.export.splitwise_export.DatabaseManager")
def test_fetch_from_database_shared_no_notes(mock_db_cls, mock_user):
    mock_db = MagicMock()
    mock_db_cls.return_value = mock_db
    txn = Transaction(id=4, date="2026-04-01", amount=100.0, merchant="Restaurant",
                      source="amex", imported_at="now", description="Dinner",
                      splitwise_id=4, is_shared=True, split_type="split",
                      notes="With: Me, Partner")
    mock_db.get_transactions_by_date_range.return_value = [txn]
    result = fetch_from_database("2026-01-01", "2026-12-31")
    assert not result.empty
    # Shared defaults: 50/50 split
    assert result.iloc[0][ExportColumns.MY_PAID] == 50.0
    assert result.iloc[0][ExportColumns.MY_OWED] == 50.0


# === fetch_and_write (restored + extended) ===
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

@patch("src.export.splitwise_export.fetch_from_database")
def test_fetch_and_write_database_empty(mock_fetch):
    mock_fetch.return_value = pd.DataFrame()
    df, url = fetch_and_write("2026-01-01", "2026-12-31", source=SOURCE_DATABASE, dry_run=True)
    assert df.empty
    assert url is None

@patch("src.export.splitwise_export.SplitwiseClient")
def test_fetch_and_write_splitwise_empty(MockClient):
    mock_client = MagicMock()
    MockClient.return_value = mock_client
    mock_client.get_my_expenses_by_date_range.return_value = pd.DataFrame()
    df, url = fetch_and_write("2026-01-01", "2026-12-31", source=SOURCE_SPLITWISE, dry_run=True)
    assert df.empty

@patch("src.export.splitwise_export.SplitwiseClient")
@patch("src.export.splitwise_export.load_exported_state")
def test_fetch_and_write_filters_settle(mock_load_state, MockClient):
    mock_load_state.return_value = (set(), set())
    mock_client = MagicMock()
    MockClient.return_value = mock_client
    
    mock_df = pd.DataFrame([
        {ExportColumns.DATE: "2026-04-01", ExportColumns.AMOUNT: 10.0,
         ExportColumns.DESCRIPTION: "Settle all balances", ExportColumns.MY_PAID: 10.0,
         ExportColumns.MY_OWED: 10.0, ExportColumns.ID: 1, ExportColumns.CATEGORY: "General"},
        {ExportColumns.DATE: "2026-04-02", ExportColumns.AMOUNT: 20.0,
         ExportColumns.DESCRIPTION: "Groceries", ExportColumns.MY_PAID: 20.0,
         ExportColumns.MY_OWED: 20.0, ExportColumns.ID: 2, ExportColumns.CATEGORY: "Food"},
    ])
    mock_client.get_my_expenses_by_date_range.return_value = mock_df
    
    df, url = fetch_and_write("2026-01-01", "2026-12-31", source=SOURCE_SPLITWISE, dry_run=True)
    assert len(df) == 1
    assert df.iloc[0][ExportColumns.DESCRIPTION] == "Groceries"

@patch("src.export.splitwise_export.SplitwiseClient")
@patch("src.export.splitwise_export.load_exported_state")
def test_fetch_and_write_filters_payment(mock_load_state, MockClient):
    mock_load_state.return_value = (set(), set())
    mock_client = MagicMock()
    MockClient.return_value = mock_client
    
    mock_df = pd.DataFrame([
        {ExportColumns.DATE: "2026-04-01", ExportColumns.AMOUNT: 10.0,
         ExportColumns.DESCRIPTION: "Payment", ExportColumns.MY_PAID: 10.0,
         ExportColumns.MY_OWED: 10.0, ExportColumns.ID: 1, ExportColumns.CATEGORY: "General"},
        {ExportColumns.DATE: "2026-04-02", ExportColumns.AMOUNT: 20.0,
         ExportColumns.DESCRIPTION: "Lunch", ExportColumns.MY_PAID: 20.0,
         ExportColumns.MY_OWED: 20.0, ExportColumns.ID: 2, ExportColumns.CATEGORY: "Food"},
    ])
    mock_client.get_my_expenses_by_date_range.return_value = mock_df
    
    df, url = fetch_and_write("2026-01-01", "2026-12-31", source=SOURCE_SPLITWISE, dry_run=True)
    assert len(df) == 1

@patch("src.export.splitwise_export.SplitwiseClient")
@patch("src.export.splitwise_export.load_exported_state")
def test_fetch_and_write_filters_no_participation(mock_load_state, MockClient):
    mock_load_state.return_value = (set(), set())
    mock_client = MagicMock()
    MockClient.return_value = mock_client
    
    mock_df = pd.DataFrame([
        {ExportColumns.DATE: "2026-04-01", ExportColumns.AMOUNT: 10.0,
         ExportColumns.DESCRIPTION: "Other", ExportColumns.MY_PAID: 0.0,
         ExportColumns.MY_OWED: 0.0, ExportColumns.ID: 1, ExportColumns.CATEGORY: "Food"},
    ])
    mock_client.get_my_expenses_by_date_range.return_value = mock_df
    
    df, url = fetch_and_write("2026-01-01", "2026-12-31", source=SOURCE_SPLITWISE, dry_run=True)
    assert df.empty

@patch("src.export.splitwise_export.SplitwiseClient")
@patch("src.export.splitwise_export.load_exported_state")
def test_fetch_and_write_overwrite_mode(mock_load_state, MockClient):
    mock_load_state.return_value = ({1}, {"old_fp"})
    mock_client = MagicMock()
    MockClient.return_value = mock_client
    
    mock_df = pd.DataFrame([{
        ExportColumns.DATE: "2026-04-01", ExportColumns.AMOUNT: 10.0,
        ExportColumns.DESCRIPTION: "Test", ExportColumns.MY_PAID: 10.0,
        ExportColumns.MY_OWED: 10.0, ExportColumns.ID: 1, ExportColumns.CATEGORY: "Food",
    }])
    mock_client.get_my_expenses_by_date_range.return_value = mock_df
    
    # Overwrite mode (append=False) should NOT filter by previous IDs
    df, url = fetch_and_write("2026-01-01", "2026-12-31", source=SOURCE_SPLITWISE, dry_run=True, append=False)
    assert len(df) == 1  # Not filtered out despite being in exported_ids

@patch("src.export.splitwise_export.SplitwiseClient")
@patch("src.export.splitwise_export.load_exported_state")
def test_fetch_and_write_dedupes_by_id(mock_load_state, MockClient):
    mock_load_state.return_value = ({"1"}, set())
    mock_client = MagicMock()
    MockClient.return_value = mock_client
    
    mock_df = pd.DataFrame([
        {ExportColumns.DATE: "2026-04-01", ExportColumns.AMOUNT: 10.0,
         ExportColumns.DESCRIPTION: "Old", ExportColumns.MY_PAID: 10.0,
         ExportColumns.MY_OWED: 10.0, ExportColumns.ID: 1, ExportColumns.CATEGORY: "Food"},
        {ExportColumns.DATE: "2026-04-02", ExportColumns.AMOUNT: 20.0,
         ExportColumns.DESCRIPTION: "New", ExportColumns.MY_PAID: 20.0,
         ExportColumns.MY_OWED: 20.0, ExportColumns.ID: 2, ExportColumns.CATEGORY: "Food"},
    ])
    mock_client.get_my_expenses_by_date_range.return_value = mock_df
    
    df, url = fetch_and_write("2026-01-01", "2026-12-31", source=SOURCE_SPLITWISE, dry_run=True)
    assert len(df) == 1
    assert df.iloc[0][ExportColumns.DESCRIPTION] == "New"

@patch("src.export.splitwise_export.write_to_sheets", return_value="https://sheet.url")
@patch("src.export.splitwise_export.save_exported_state")
@patch("src.export.splitwise_export.SplitwiseClient")
@patch("src.export.splitwise_export.load_exported_state")
def test_fetch_and_write_live_splitwise(mock_load_state, MockClient, mock_save_state, mock_write):
    mock_load_state.return_value = (set(), set())
    mock_client = MagicMock()
    MockClient.return_value = mock_client
    
    mock_df = pd.DataFrame([{
        ExportColumns.DATE: "2026-04-01", ExportColumns.AMOUNT: 10.0,
        ExportColumns.DESCRIPTION: "Test", ExportColumns.MY_PAID: 10.0,
        ExportColumns.MY_OWED: 10.0, ExportColumns.ID: 101, ExportColumns.CATEGORY: "Food",
    }])
    mock_client.get_my_expenses_by_date_range.return_value = mock_df
    
    df, url = fetch_and_write("2026-01-01", "2026-12-31", source=SOURCE_SPLITWISE,
                               dry_run=False, sheet_key="test_key")
    assert url == "https://sheet.url"
    mock_save_state.assert_called_once()
    mock_write.assert_called_once()

@patch("src.export.splitwise_export.write_to_sheets", return_value="https://sheet.url")
@patch("src.export.splitwise_export.DatabaseManager")
@patch("src.export.splitwise_export.fetch_from_database")
def test_fetch_and_write_live_database(mock_fetch, mock_db_cls, mock_write):
    mock_df = pd.DataFrame([{
        ExportColumns.DATE: "2026-04-01", ExportColumns.AMOUNT: 50.0,
        ExportColumns.DESCRIPTION: "Test", ExportColumns.MY_PAID: 50.0,
        ExportColumns.MY_OWED: 50.0, ExportColumns.ID: 100,
    }])
    mock_fetch.return_value = mock_df
    mock_db = MagicMock()
    mock_db_cls.return_value = mock_db
    mock_txn = MagicMock()
    mock_txn.id = 1
    mock_db.get_transaction_by_splitwise_id.return_value = mock_txn
    
    df, url = fetch_and_write("2026-01-01", "2026-12-31", source=SOURCE_DATABASE,
                               dry_run=False, sheet_key="test_key", year=2026)
    assert url == "https://sheet.url"
    mock_db.mark_written_to_sheet.assert_called_once()
