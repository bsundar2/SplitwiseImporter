import pytest
import pandas as pd
from unittest.mock import patch, MagicMock

from src.import_statement.pipeline import process_statement

@patch("src.import_statement.pipeline.parse_statement")
@patch("src.import_statement.pipeline.DatabaseManager")
@patch("src.import_statement.pipeline.SplitwiseClient")
@patch("src.import_statement.pipeline.BankConfig")
def test_process_statement_dry_run(MockBankConfig, MockClient, MockDB, mock_parse_statement):
    mock_parse_statement.return_value = pd.DataFrame([{
        "date": "2026-04-01",
        "description": "Amazon",
        "amount": 10.0,
        "detail": "ref123",
        "cc_reference_id": "ref123",
        "is_credit": False
    }])
    
    mock_db = MagicMock()
    MockDB.return_value = mock_db
    mock_db.get_transaction_by_cc_reference.return_value = None
    
    mock_client = MagicMock()
    MockClient.return_value = mock_client
    mock_client.find_expense_by_cc_reference.return_value = None
    
    with patch("src.import_statement.pipeline.write_to_sheets") as mock_write, patch("src.import_statement.pipeline.mkdir_p"):
        # The function saves the processed file, so mock out_df.to_csv
        with patch("pandas.DataFrame.to_csv"):
            df = process_statement(
                path="data/raw/amex/statement.csv",
                dry_run=True,
                no_sheet=True,
                start_date="2026-01-01",
                end_date="2026-12-31"
            )
        
    assert df is not None
    assert len(df) == 1
    assert df.iloc[0]["status"] == "would_add"

@patch("src.import_statement.pipeline.parse_statement")
@patch("src.import_statement.pipeline.DatabaseManager")
@patch("src.import_statement.pipeline.SplitwiseClient")
@patch("src.import_statement.pipeline.BankConfig")
def test_process_statement_db_exists(MockBankConfig, MockClient, MockDB, mock_parse_statement):
    mock_parse_statement.return_value = pd.DataFrame([{
        "date": "2026-04-01",
        "description": "Uber",
        "amount": 5.0,
        "detail": "ref456",
        "cc_reference_id": "ref456",
        "is_credit": False
    }])
    
    mock_db = MagicMock()
    MockDB.return_value = mock_db
    
    # Mock finding the item in the DB
    mock_txn = MagicMock()
    mock_txn.splitwise_id = 999
    mock_db.get_transaction_by_cc_reference.return_value = mock_txn
    
    mock_client = MagicMock()
    MockClient.return_value = mock_client
    
    with patch("pandas.DataFrame.to_csv"), patch("src.import_statement.pipeline.mkdir_p"):
        df = process_statement(
            path="data/raw/amex/statement.csv",
            dry_run=False,
            no_sheet=True,
            start_date="2026-01-01",
            end_date="2026-12-31"
        )
        
    assert len(df) == 1
    assert df.iloc[0]["status"] == "db_exists"
    assert df.iloc[0]["splitwise_id"] == 999
    mock_client.add_expense_from_txn.assert_not_called()
