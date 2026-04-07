"""Tests for generate_summaries.py covering summary generation functions."""
import pytest
import json
import os
from unittest.mock import patch, MagicMock
import pandas as pd
import numpy as np
import sys

from src.export.generate_summaries import (
    generate_monthly_summary,
    generate_category_breakdown,
    generate_monthly_trends,
    generate_category_monthly_breakdown,
    load_budget,
    generate_budget_vs_actual,
    fetch_transactions_for_analysis,
    main,
)

@pytest.fixture
def sample_transactions_df():
    """DataFrame mimicking fetch_transactions_for_analysis output."""
    dates = pd.to_datetime(["2026-01-15", "2026-01-20", "2026-02-10", "2026-02-25", "2026-03-05"])
    df = pd.DataFrame({
        "date": dates,
        "amount": [100, 50, 75, 120, 200],
        "category": ["Food", "Transport", "Food", "Shopping", "Entertainment"],
        "description": ["Restaurant", "Uber", "Grocery", "Amazon", "Netflix"],
        "my_paid": [100, 50, 75, 120, 200],
        "my_owed": [100, 50, 75, 120, 200],
        "my_net": [0, 0, 0, 0, 0],
        "is_shared": [False, False, False, False, False],
        "is_deleted": [False, False, False, False, False],
    })
    df["year_month"] = df["date"].dt.to_period("M")
    df["month"] = df["date"].dt.month
    df["month_name"] = df["date"].dt.strftime("%B")
    return df

# === generate_monthly_summary ===
def test_monthly_summary(sample_transactions_df):
    result = generate_monthly_summary(sample_transactions_df, 2026)
    assert not result.empty
    assert "Month" in result.columns
    assert "Total Spent (Net)" in result.columns
    assert "MoM Change" in result.columns
    assert "Cumulative Spending" in result.columns
    assert len(result) == 3  # Jan, Feb, Mar

def test_monthly_summary_empty():
    result = generate_monthly_summary(pd.DataFrame(), 2026)
    assert result.empty

# === generate_category_breakdown ===
def test_category_breakdown(sample_transactions_df):
    result = generate_category_breakdown(sample_transactions_df, 2026)
    assert not result.empty
    assert "Category" in result.columns
    assert "% of Total" in result.columns
    assert result.iloc[-1]["Category"] == "TOTAL"
    assert result.iloc[-1]["% of Total"] == 100.0

def test_category_breakdown_empty():
    result = generate_category_breakdown(pd.DataFrame(), 2026)
    assert result.empty

# === generate_monthly_trends ===
def test_monthly_trends(sample_transactions_df):
    result = generate_monthly_trends(sample_transactions_df, 2026)
    assert not result.empty
    assert "3-Month Avg" in result.columns
    assert "YTD Avg" in result.columns

def test_monthly_trends_empty():
    result = generate_monthly_trends(pd.DataFrame(), 2026)
    assert result.empty

# === generate_category_monthly_breakdown ===
def test_category_monthly_breakdown(sample_transactions_df):
    result = generate_category_monthly_breakdown(sample_transactions_df, 2026)
    assert not result.empty
    assert "Total" in result.columns
    assert result.iloc[-1].iloc[0] == "TOTAL"

def test_category_monthly_breakdown_empty():
    result = generate_category_monthly_breakdown(pd.DataFrame(), 2026)
    assert result.empty

# === load_budget ===
def test_load_budget(tmp_path):
    budget_file = tmp_path / "budget.json"
    budget_file.write_text(json.dumps({"Food": 500, "Transport": 200}))
    result = load_budget(str(budget_file))
    assert result == {"Food": 500, "Transport": 200}

def test_load_budget_missing():
    result = load_budget("/nonexistent/budget.json")
    assert result == {}

# === generate_budget_vs_actual ===
def test_budget_vs_actual(sample_transactions_df):
    budget = {"Food": 200, "Transport": 100, "Shopping": 150}
    result = generate_budget_vs_actual(sample_transactions_df, 2026, budget)
    assert not result.empty
    assert "Budget" in result.columns
    assert "Actual" in result.columns
    assert "Variance" in result.columns

def test_budget_vs_actual_empty():
    result = generate_budget_vs_actual(pd.DataFrame(), 2026, {})
    assert result.empty

# === fetch_transactions_for_analysis ===
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
    assert df.iloc[0]["my_paid"] == -20.0
    assert df.iloc[0]["my_owed"] == -20.0

# === main CLI ===
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
