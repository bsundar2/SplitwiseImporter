import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime, date
import pandas as pd

from src.common.splitwise_client import SplitwiseClient

@pytest.fixture
def mock_env():
    with patch.dict("os.environ", {
        "SPLITWISE_CONSUMER_KEY": "fake_key",
        "SPLITWISE_CONSUMER_SECRET": "fake_secret",
        "SPLITWISE_API_KEY": "fake_api"
    }):
        yield

@pytest.fixture
def splitwise_client(mock_env):
    with patch("src.common.splitwise_client.Splitwise"):
        return SplitwiseClient()

def test_init_raises_value_error_if_missing_credentials():
    with patch.dict("os.environ", {}, clear=True):
        with pytest.raises(ValueError):
            SplitwiseClient()

def test_get_current_user_id(splitwise_client):
    mock_user = MagicMock()
    mock_user.getId.return_value = 12345
    splitwise_client.sObj.getCurrentUser.return_value = mock_user
    
    assert splitwise_client.get_current_user_id() == 12345

def test_fetch_expenses_paginated(splitwise_client):
    # Mocking expenses
    mock_exp1 = MagicMock()
    mock_exp1.deleted_at = None
    
    mock_exp2 = MagicMock()
    mock_exp2.deleted_at = "deleted_timestamp"
    
    # getExpenses first call returns list of 1 expense
    splitwise_client.sObj.getExpenses.side_effect = [[mock_exp1, mock_exp2], []]
    
    res = splitwise_client._fetch_expenses_paginated("2026-01-01", "2026-01-31")
    
    # The deleted expense should be removed
    assert len(res) == 1
    assert res[0] == mock_exp1
    splitwise_client.sObj.getExpenses.assert_called()

def test_find_expense_by_cc_reference(splitwise_client):
    # Testing fuzzy matching via get_my_expenses_by_date_range mock
    mock_df = pd.DataFrame({
        "id": [11, 22],
        "amount": [50.0, 100.0],
        "date": ["2026-04-01T00:00:00Z", "2026-04-02T00:00:00Z"],
        "description": ["Target Purchase", "Amazon.com"],
        "date_updated": ["2026-04-01", "2026-04-02"],
        "Details": ["TXN-11", "TXN-22"]
    })
    
    with patch.object(splitwise_client, "get_my_expenses_by_date_range", return_value=mock_df):
        # Match by exact CC_reference
        match = splitwise_client.find_expense_by_cc_reference(cc_reference_id="TXN-22")
        assert match["id"] == 22
        
        # Match by fuzzy criteria
        match2 = splitwise_client.find_expense_by_cc_reference(amount=50.0, date="2026-04-01")
        assert match2["id"] == 11
