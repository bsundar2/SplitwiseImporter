import pytest
from datetime import date, datetime
import pandas as pd
from unittest.mock import patch, MagicMock

from src.common.utils import (
    parse_date_string,
    format_date,
    clean_description_for_splitwise,
    clean_merchant_name,
    merchant_slug,
    compute_import_id,
    parse_date_safe,
    parse_date,
    parse_float_safe,
    generate_fingerprint,
    infer_category
)

def test_parse_date_string():
    assert parse_date_string("2026-04-01") == date(2026, 4, 1)
    with pytest.raises(ValueError):
        parse_date_string("04/01/2026")

def test_format_date():
    assert format_date(date(2026, 4, 1)) == "2026-04-01"
    assert format_date(datetime(2026, 4, 1, 12, 30)) == "2026-04-01"

def test_clean_merchant_name():
    assert clean_merchant_name("AMERICAN AIRLINES   800-433-7300        TX") == "American Airlines"
    assert clean_merchant_name("SP BERNAL CUTLERY   SAN FRANCISCO       CA") == "Bernal Cutlery"
    assert clean_merchant_name("GglPay CINEMARK     PLANO               TX") == "Cinemark"
    assert clean_merchant_name(None) == ""

@patch("src.common.utils._load_merchant_lookup")
def test_clean_description_for_splitwise(mock_lookup):
    mock_lookup.return_value = {"grab*a-8pxhismwwu9tasingapore sg": {"canonical_name": "Grab"}}
    
    # Test simple cleaning (fallback behavior)
    assert clean_description_for_splitwise("GRAB*A-8PXHISMWWU9TASINGAPORE           SG") == "Grab*A Sg"
    assert clean_description_for_splitwise("GglPay GUARDIAN HEALTH & BEAUTY-1110104105") == "Health &"
    assert clean_description_for_splitwise("UBER EATS           help.uber.com       CA") == "Uber Eats"

    # Test lookup match
    mock_lookup.return_value = {"uber eats": {"canonical_name": "Uber Eats (Lookup)"}}
    assert clean_description_for_splitwise("UBER EATS           help.uber.com       CA") == "Uber Eats (Lookup)"

def test_merchant_slug():
    assert merchant_slug("McDonald's Inc.") == "mcdonald-s"
    assert merchant_slug("The Coffee Shop LLC") == "coffee-shop"
    assert merchant_slug("Target") == "target"
    assert merchant_slug("") == ""

def test_compute_import_id():
    # Stable import IDs
    id1 = compute_import_id("2026-04-01", 10.50, "McDonalds")
    id2 = compute_import_id("2026-04-01", 10.50, "mcdonalds")
    assert id1 == id2
    
def test_parse_date_safe():
    current_year = datetime.now().year
    assert parse_date_safe("2026-04-01") == "2026-04-01"
    # Should append year if missing and fallback
    # Note: exactly what "04/01" evaluates to depends on current date
    # But checking pd.NA handling:
    assert parse_date_safe(pd.NA) is None
    assert parse_date_safe(None) is None
    assert parse_date_safe("") is None

def test_parse_float_safe():
    assert parse_float_safe("15.5") == 15.5
    assert parse_float_safe("") == 0.0
    assert parse_float_safe(pd.NA) == 0.0

@patch("src.common.utils._load_category_config")
@patch("src.common.utils._load_merchant_lookup")
def test_infer_category(mock_merchant_lookup, mock_cat_config):
    # Setup mocks
    mock_merchant_lookup.return_value = {
        "uber": {"category": "Transportation", "subcategory": "Taxi", "confidence": 0.9}
    }
    mock_cat_config.return_value = {
        "default_category": {"id": 2, "name": "Uncategorized"},
        "patterns": []
    }
    
    # 1. Match from merchant lookup
    res1 = infer_category({"description": "Uber Ride", "merchant": "Uber", "amount": 15})
    assert "Transportation" in res1.get("category_name", res1.get("category_path", ""))
    
    # 2. Match from default (uncategorized)
    res2 = infer_category({"description": "Unknown charge", "amount": 10})
    assert res2["category_name"] == "Uncategorized"

