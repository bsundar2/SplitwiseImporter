import pytest
import json
from pathlib import Path
from unittest.mock import patch, mock_open

from src.import_statement.bank_config import BankConfig

@patch("builtins.open", new_callable=mock_open, read_data='{"banks": {"amex": {"category_mapping_file": "amex_map.json"}}}')
def test_detect_bank_from_path_and_config(mock_file):
    # init will load the config
    config = BankConfig(config_path=Path("dummy_path.json"))
    
    assert config.detect_bank_from_path("data/raw/amex/amex2026.csv") == "amex"
    assert config.get_bank_config("amex")["category_mapping_file"] == "amex_map.json"

    with pytest.raises(ValueError):
        config.detect_bank_from_path("data/raw/chase/chase2026.csv")
        
    with pytest.raises(ValueError):
        config.get_bank_config("chase")

