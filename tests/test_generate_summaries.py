"""Tests for generate_summaries.py covering summary generation functions."""
import pytest
import json
import os
from unittest.mock import patch, MagicMock
import pandas as pd
import numpy as np

from src.export.generate_summaries import (
    generate_monthly_summary,
    generate_category_breakdown,
    generate_monthly_trends,
    generate_category_monthly_breakdown,
    load_budget,
    generate_budget_vs_actual,
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
    # Should have TOTAL row
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
    # Should have TOTAL row
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
