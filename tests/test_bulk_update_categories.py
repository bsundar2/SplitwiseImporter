import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from datetime import datetime
from src.update.bulk_update_categories import find_expenses_to_update, update_expenses, main

def test_find_expenses_to_update():
    from src.constants.export_columns import ExportColumns
    client = MagicMock()
    df = pd.DataFrame([
        {ExportColumns.DESCRIPTION: "Amazon Mktp", ExportColumns.CATEGORY: "Uncategorized"},
        {ExportColumns.DESCRIPTION: "Uber Ride", ExportColumns.CATEGORY: "Transportation - Taxi"},
        {ExportColumns.DESCRIPTION: "Whole Foods", ExportColumns.CATEGORY: "Food and drink - Groceries"}
    ])
    client.get_my_expenses_by_date_range.return_value = df
    
    res = find_expenses_to_update(client, datetime(2026,1,1), datetime(2026,1,31), merchant_filter="Amazon")
    assert len(res) == 1
    assert "Amazon" in res.iloc[0]["Description"]
    
    res = find_expenses_to_update(client, datetime(2026,1,1), datetime(2026,1,31), current_category_filter="Transportation - Taxi")
    assert len(res) == 1
    assert "Uber" in res.iloc[0]["Description"]

def test_update_expenses():
    from src.constants.export_columns import ExportColumns
    client = MagicMock()
    mock_exp = MagicMock()
    client.sObj.getExpense.return_value = mock_exp
    df = pd.DataFrame([{ExportColumns.ID: 101, ExportColumns.DESCRIPTION: "Test", ExportColumns.DATE: "2026-01-01"}])
    
    # Dry run
    assert update_expenses(client, df, 15, dry_run=True) == 0
    client.sObj.updateExpense.assert_not_called()
    
    # Live run
    assert update_expenses(client, df, 15, dry_run=False) == 1
    client.sObj.updateExpense.assert_called_once()

@patch("src.update.bulk_update_categories.update_expenses")
@patch("src.update.bulk_update_categories.find_expenses_to_update")
@patch("src.update.bulk_update_categories.SplitwiseClient")
def test_main(mock_client, mock_find, mock_update):
    mock_find.return_value = pd.DataFrame([{"Date": "2026-01-01", "Description": "Amazon", "Category": "Cat", "Amount": 10.0}])
    mock_update.return_value = 1
    
    with patch("sys.argv", ["script", "--merchant", "Amazon", "--subcategory-id", "14", "--yes"]):
        assert main() == 1
