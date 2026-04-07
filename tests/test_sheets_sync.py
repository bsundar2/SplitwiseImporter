import pytest
import pandas as pd
from unittest.mock import patch, MagicMock

import pygsheets

from src.common.sheets_sync import (
    read_from_sheets,
    write_to_sheets,
    _colnum_to_a1,
    _ensure_size_for_append,
)

@patch("src.common.sheets_sync.pygsheets.authorize")
def test_read_from_sheets(mock_authorize):
    # Setup mocks
    mock_gc = MagicMock()
    mock_sheet = MagicMock()
    mock_worksheet = MagicMock()
    
    mock_authorize.return_value = mock_gc
    mock_gc.open_by_key.return_value = mock_sheet
    mock_sheet.worksheet_by_title.return_value = mock_worksheet
    
    # Return a mocked dataframe
    expected_df = pd.DataFrame({"col1": [1, 2]})
    mock_worksheet.get_as_df.return_value = expected_df
    
    df = read_from_sheets("fake_key", "fake_worksheet")
    assert df is not None
    assert df.equals(expected_df)

@patch("src.common.sheets_sync.pygsheets.authorize")
def test_read_from_sheets_worksheet_not_found(mock_authorize):
    mock_gc = MagicMock()
    mock_sheet = MagicMock()
    
    mock_authorize.return_value = mock_gc
    mock_gc.open_by_key.return_value = mock_sheet
    mock_sheet.worksheet_by_title.side_effect = pygsheets.WorksheetNotFound("Not found")
    
    df = read_from_sheets("fake_key", "fake_worksheet")
    assert df is None

def test_colnum_to_a1():
    assert _colnum_to_a1(1) == "A"
    assert _colnum_to_a1(26) == "Z"
    assert _colnum_to_a1(27) == "AA"
    assert _colnum_to_a1(52) == "AZ"

def test_ensure_size_for_append():
    mock_worksheet = MagicMock()
    mock_worksheet.rows = 10
    mock_worksheet.cols = 5
    
    # Needs to add rows (start 10 + 5 rows - 1 = 14 needed)
    _ensure_size_for_append(mock_worksheet, start_row=10, num_rows=5, num_cols=5)
    mock_worksheet.add_rows.assert_called_once_with(4)
    # Shouldn't need to add cols
    mock_worksheet.add_cols.assert_not_called()

@patch("src.common.sheets_sync.pygsheets.authorize")
def test_write_to_sheets_overwrite(mock_authorize):
    mock_gc = MagicMock()
    mock_sheet = MagicMock()
    mock_worksheet = MagicMock()
    
    mock_authorize.return_value = mock_gc
    mock_gc.open_by_key.return_value = mock_sheet
    mock_sheet.worksheets.return_value = [mock_worksheet]
    mock_worksheet.title = "test_sheet"
    mock_sheet.url = "http://fake-url.com"
    
    df = pd.DataFrame({"A": [1], "B": [2]})
    url = write_to_sheets(df, "test_sheet", "fake_key", append=False, skip_formatting=True)
    
    assert url == "http://fake-url.com"
    mock_worksheet.clear.assert_called_once()
    mock_worksheet.set_dataframe.assert_called_once_with(df, (1, 1), copy_index=False, copy_head=True)
