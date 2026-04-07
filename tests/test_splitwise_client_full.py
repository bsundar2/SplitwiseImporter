import pytest
import os
import tempfile
import json
from unittest.mock import patch, MagicMock
from pathlib import Path

import pandas as pd
from src.common.splitwise_client import SplitwiseClient
from src.constants.export_columns import ExportColumns
from src.constants.splitwise import SPLIT_TYPE_SELF, SPLIT_TYPE_PARTNER, SPLIT_TYPE_SPLIT

@pytest.fixture
def mock_env():
    with patch.dict("os.environ", {
        "SPLITWISE_CONSUMER_KEY": "fake",
        "SPLITWISE_CONSUMER_SECRET": "fake",
        "SPLITWISE_API_KEY": "fake"
    }):
        yield

@pytest.fixture
def splitwise_client(mock_env):
    with patch("src.common.splitwise_client.Splitwise"):
        return SplitwiseClient()

def test_fetch_expenses_paginated_full_details(splitwise_client):
    mock_exp1 = MagicMock()
    mock_exp1.getId.return_value = 1
    mock_exp1.deleted_at = None
    
    # sObj response has expenses
    splitwise_client.sObj.getExpenses.side_effect = [[mock_exp1], []]
    
    mock_full_exp1 = MagicMock()
    mock_full_exp1.getId.return_value = 1
    mock_full_exp1.deleted_at = None
    splitwise_client.sObj.getExpense.return_value = mock_full_exp1
    
    res = splitwise_client._fetch_expenses_paginated("2026-01-01", "2026-01-02", fetch_full_details=True)
    assert len(res) == 1
    assert res[0] == mock_full_exp1
    splitwise_client.sObj.getExpense.assert_called_once_with(1)

def test_fetch_expenses_with_details_cache_miss_and_hit(splitwise_client, tmp_path):
    with patch("src.common.splitwise_client.Path") as mock_path:
        # Mock paths
        mock_cache_dir = tmp_path / "data"
        mock_cache_dir.mkdir()
        mock_cache_file = mock_cache_dir / "splitwise_expense_details_2026.json"
        
        # Override _get_expense_cache_path to return our temp file
        splitwise_client._get_expense_cache_path = MagicMock(return_value=mock_cache_file)
        
        # Mock _fetch_expenses_paginated
        mock_exp = MagicMock()
        mock_exp.getId.return_value = 1
        mock_exp.getDate.return_value = "2026-01-01"
        mock_exp.getDescription.return_value = "Test"
        mock_exp.getCost.return_value = "10.0"
        mock_exp.getDetails.return_value = "Details"
        mock_exp.getCategory.return_value.getName.return_value = "Cat"
        splitwise_client._fetch_expenses_paginated = MagicMock(return_value=[mock_exp])
        
        # 1. Cache Miss - runs actual fetch
        res1 = splitwise_client.fetch_expenses_with_details("2026-01-01", "2026-01-02")
        assert 1 in res1
        assert res1[1]["description"] == "Test"
        assert mock_cache_file.exists()
        
        # 2. Cache Hit - load from file
        splitwise_client._fetch_expenses_paginated.reset_mock()
        res2 = splitwise_client.fetch_expenses_with_details("2026-01-01", "2026-01-02")
        splitwise_client._fetch_expenses_paginated.assert_not_called()
        assert res2["1"]["description"] == "Test"

def test_get_my_expenses_by_date_range(splitwise_client):
    # Mocking full expenses response
    splitwise_client.get_current_user_id = MagicMock(return_value=101)
    
    mock_exp = MagicMock()
    mock_exp.getId.return_value = 1
    mock_exp.getDate.return_value = "2026-01-01T00:00:00Z"
    mock_exp.getCreatedAt.return_value = "2026-01-01T00:00:00Z"
    mock_exp.getUpdatedAt.return_value = "2026-01-01T00:00:00Z"
    mock_exp.getDescription.return_value = "Test"
    mock_exp.getCost.return_value = "10.0"
    mock_exp.getDetails.return_value = "Details"
    mock_exp.getCategory.return_value.getName.return_value = "Category"
    mock_exp.getCreationMethod.return_value = "Method"
    
    mock_user_me = MagicMock()
    mock_user_me.getId.return_value = 101
    mock_user_me.getFirstName.return_value = "Me"
    mock_user_me.getPaidShare.return_value = "10.0"
    mock_user_me.getOwedShare.return_value = "10.0"
    
    mock_exp.getUsers.return_value = [mock_user_me]
    
    from datetime import datetime
    splitwise_client._fetch_expenses_paginated = MagicMock(return_value=[mock_exp])
    
    df = splitwise_client.get_my_expenses_by_date_range(datetime(2026, 1, 1), datetime(2026, 1, 2))
    assert not df.empty
    assert len(df) == 1
    assert df.iloc[0][ExportColumns.ID] == 1
    assert df.iloc[0][ExportColumns.AMOUNT] == 10.0
    assert df.iloc[0][ExportColumns.SPLIT_TYPE] == SPLIT_TYPE_SELF
    assert "Me" in df.iloc[0][ExportColumns.PARTICIPANT_NAMES]

