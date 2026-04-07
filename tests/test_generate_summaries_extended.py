import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
import sys
from src.export.generate_summaries import (
    fetch_transactions_for_analysis,
    main,
)

@patch("src.export.generate_summaries.DatabaseManager")
def test_fetch_transactions_for_analysis_with_notes(mock_db_cls):
    mock_db = mock_db_cls.return_value
    from src.database.models import Transaction
    txn = Transaction(
        id=1, date="2026-04-01", amount=100.0, description="Test",
        merchant="Test Merchant", source="splitwise", imported_at="2026-04-01T12:00:00Z",
        notes="Paid: $100.00 | Owe: $50.00", category="Food",
        is_refund=False, splitwise_deleted_at=None
    )
    mock_db.get_transactions_with_splitwise_ids.return_value = [txn]
    
    df = fetch_transactions_for_analysis(year=2026)
    assert len(df) == 1
    assert df.iloc[0]["my_paid"] == 100.0
    assert df.iloc[0]["my_owed"] == 50.0
    assert df.iloc[0]["my_net"] == 50.0

@patch("src.export.generate_summaries.DatabaseManager")
def test_fetch_transactions_for_analysis_refund(mock_db_cls):
    mock_db = mock_db_cls.return_value
    from src.database.models import Transaction
    txn = Transaction(
        id=2, date="2026-04-01", amount=20.0, description="Refund",
        merchant="Refund Merchant", source="splitwise", imported_at="2026-04-01T12:00:00Z",
        notes="Paid: $20.00 | Owe: $20.00", category="Food",
        is_refund=True, splitwise_deleted_at=None
    )
    mock_db.get_transactions_with_splitwise_ids.return_value = [txn]
    
    df = fetch_transactions_for_analysis(year=2026)
    assert len(df) == 1
    # Refunds should have negative values
    assert df.iloc[0]["my_paid"] == -20.0
    assert df.iloc[0]["my_owed"] == -20.0

@patch("src.export.generate_summaries.fetch_transactions_for_analysis")
@patch("src.export.generate_summaries.write_to_sheets")
@patch("src.export.generate_summaries.DatabaseManager")
def test_main_dry_run(mock_db_cls, mock_write, mock_fetch):
    mock_fetch.return_value = pd.DataFrame([
        {"date": "2026-01-01", "amount": 10.0, "category": "Food", "description": "T",
         "my_paid": 10.0, "my_owed": 10.0, "my_net": 0.0, "is_shared": False, "is_deleted": False,
         "year_month": pd.Period("2026-01", freq="M"), "month": 1, "month_name": "January"}
    ])
    
    with patch("sys.argv", ["script", "--year", "2026", "--dry-run"]):
        assert main() == 0
        mock_write.assert_not_called()

@patch("src.export.generate_summaries.fetch_transactions_for_analysis")
@patch("src.export.generate_summaries.write_to_sheets")
@patch("src.export.generate_summaries.DatabaseManager")
def test_main_live(mock_db_cls, mock_write, mock_fetch):
    mock_fetch.return_value = pd.DataFrame([
        {"date": "2026-01-01", "amount": 10.0, "category": "Food", "description": "T",
         "my_paid": 10.0, "my_owed": 10.0, "my_net": 0.0, "is_shared": False, "is_deleted": False,
         "year_month": pd.Period("2026-01", freq="M"), "month": 1, "month_name": "January", "Month": "2026-01",
         "Total Spent (Net)": 0.0, "Avg Transaction": 0.0, "Transaction Count": 1, "Total Paid": 10.0,
         "Total Net": 0.0, "Cumulative Spending": 0.0, "MoM Change": 0.0}
    ])
    
    with patch("sys.argv", ["script", "--year", "2026", "--sheet-key", "test_key"]):
        assert main() == 0
        mock_write.assert_called()
