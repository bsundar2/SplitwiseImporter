import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
import datetime
from src.common.splitwise_client import SplitwiseClient

@patch("os.getenv")
def test_get_my_expenses_pagination_final(mock_getenv):
    mock_getenv.return_value = "dummy_val"
    client = SplitwiseClient()
    mock_sw = MagicMock()
    client.sObj = mock_sw
    
    # Mocking getExpenses with pagination
    expense1 = MagicMock()
    expense1.getId.return_value = 1
    expense1.getCost.return_value = "10.0"
    expense1.getDate.return_value = "2026-01-01T00:00:00Z"
    expense1.getDescription.return_value = "Test1"
    expense1.getDetails.return_value = "details"
    expense1.getCurrencyCode.return_value = "USD"
    # SUCCESS: Splitwise object attribute must be None for non-deleted expenses
    expense1.deleted_at = None
    expense1.getDeletedAt.return_value = None
    expense1.getUpdatedAt.return_value = "2026-01-01T00:00:00Z"
    expense1.getCreatedAt.return_value = "2026-01-01T00:00:00Z"
    expense1.getCategory.return_value.getName.return_value = "Food"
    
    user1 = MagicMock()
    user1.getId.return_value = 12345
    user1.getPaidShare.return_value = "10.0"
    user1.getOwedShare.return_value = "5.0"
    user1.getFirstName.return_value = "Me"
    user1.getLastName.return_value = "Self"
    expense1.getUsers.return_value = [user1]
    
    mock_sw.getExpenses.side_effect = [[expense1], []]
    mock_sw.getCurrentUser.return_value.getId.return_value = 12345
    
    df = client.get_my_expenses_by_date_range(
        datetime.date(2026, 1, 1), 
        datetime.date(2026, 1, 2)
    )
    
    assert len(df) == 1

@patch("os.getenv")
def test_get_my_expenses_error_handling_final(mock_getenv):
    mock_getenv.return_value = "dummy_val"
    client = SplitwiseClient()
    mock_sw = MagicMock()
    client.sObj = mock_sw
    mock_sw.getExpenses.side_effect = Exception("API Error")
    
    # SplitwiseClient._fetch_expenses_paginated re-raises exceptions.
    with pytest.raises(Exception, match="API Error"):
        client.get_my_expenses_by_date_range(
            datetime.date(2026, 1, 1), 
            datetime.date(2026, 1, 2)
        )
