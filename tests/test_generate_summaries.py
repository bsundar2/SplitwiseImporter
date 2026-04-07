import pytest
import pandas as pd
from src.export.generate_summaries import (
    generate_monthly_summary,
    generate_category_breakdown,
    generate_monthly_trends,
    generate_category_monthly_breakdown,
    generate_budget_vs_actual
)

@pytest.fixture
def sample_df():
    data = [
        {"date": "2026-01-01", "my_owed": 100.0, "my_paid": 100.0, "my_net": 0.0, "category": "Food and drink - Dining out", "description": "Restaurant"},
        {"date": "2026-01-15", "my_owed": 50.0, "my_paid": 50.0, "my_net": 0.0, "category": "Home - Rent", "description": "Landlord"},
        {"date": "2026-02-01", "my_owed": 150.0, "my_paid": 150.0, "my_net": 0.0, "category": "Food and drink - Dining out", "description": "Restaurant 2"}
    ]
    df = pd.DataFrame(data)
    df["date"] = pd.to_datetime(df["date"])
    df["year_month"] = df["date"].dt.to_period("M")
    df["month"] = df["date"].dt.month
    df["month_name"] = df["date"].dt.strftime("%B")
    return df

def test_generate_monthly_summary(sample_df):
    summary = generate_monthly_summary(sample_df, 2026)
    assert len(summary) == 2
    assert summary.iloc[0]["Month"] == "2026-01"
    assert summary.iloc[0]["Total Spent (Net)"] == 150.0
    assert summary.iloc[1]["Month"] == "2026-02"
    assert summary.iloc[1]["Total Spent (Net)"] == 150.0

def test_generate_category_breakdown(sample_df):
    breakdown = generate_category_breakdown(sample_df, 2026)
    # 2 categories + 1 total row
    assert len(breakdown) == 3
    assert breakdown.iloc[0]["Category"] == "Food and drink - Dining out"
    assert "TOTAL" in breakdown["Category"].values

def test_generate_monthly_trends(sample_df):
    trends = generate_monthly_trends(sample_df, 2026)
    assert len(trends) == 2
    assert "3-Month Avg" in trends.columns

def test_generate_category_monthly_breakdown(sample_df):
    breakdown = generate_category_monthly_breakdown(sample_df, 2026)
    assert "January" in breakdown.columns
    assert "February" in breakdown.columns
    assert "Total" in breakdown.columns
    # 2 categories + TOTAL row
    assert len(breakdown) == 3
    
def test_generate_budget_vs_actual(sample_df):
    # my_net is 0.0 in the sample data which is what's queried in budget vs actual.
    # updating sample_df directly for this test
    sample_df["my_net"] = sample_df["my_owed"]
    
    budget = {
        "Food and drink - Dining out": 300.0,
        "Home - Rent": 50.0
    }
    
    bva = generate_budget_vs_actual(sample_df, 2026, budget)
    # categories: Food, Rent, TOTAL => 3
    assert len(bva) == 3
    
    food_row = bva[bva["Category"] == "Food and drink - Dining out"].iloc[0]
    assert food_row["Actual"] == 250.0  # 100 + 150
    assert food_row["Budget"] == 300.0
    assert food_row["Status"] == "Under Budget"
