import pytest
from unittest.mock import patch, MagicMock
from src.import_statement.parse_statement import parse_statement
import pandas as pd

def test_parse_bofa_custom():
    # Mock bank_config to return BoFA config info
    with patch("src.import_statement.parse_statement.BANK_CONFIG") as mock_cfg:
        mock_cfg.get_bank_config.return_value = {
            "name": "bofa",
            "date_col": "Date",
            "description_col": "Description",
            "amount_col": "Amount",
            "date_format": "%m/%d/%Y",
            "skip_rows": 0
        }
        # Mock pd.read_csv to return our dummy data
        with patch("src.import_statement.parse_statement.pd.read_csv") as mock_read:
            mock_read.return_value = pd.DataFrame([
                {"Date": "04/01/2026", "Description": "STARBUCKS", "Amount": "-10.0"}
            ])
            # Use a path containing 'bofa' to trigger BoFA logic
            df = parse_statement("data/raw/bofa/dummy_bofa.csv")
            assert not df.empty
            assert df.iloc[0]["description"] == "STARBUCKS"
