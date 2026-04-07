import os
from pathlib import Path

import pandas as pd
import pytest

from src.import_statement.parse_statement import (
    _is_credit,
    _is_likely_refund,
    extract_reference_id,
    parse_amount_safe,
    parse_csv,
)

# Base Path
TEST_DATA_DIR = Path(__file__).parent / "data"

def test_parse_amount_safe():
    assert parse_amount_safe("1200.50") == 1200.50
    assert parse_amount_safe("$1,200.50") == 1200.50
    assert parse_amount_safe("(150.50)") == -150.50
    assert parse_amount_safe("($1,500.25)") == -1500.25
    assert parse_amount_safe(-50) == -50.0
    assert parse_amount_safe(pd.NA) == 0.0

def test_extract_reference_id():
    assert extract_reference_id("1234567890123") == "1234567890123"
    assert extract_reference_id("REF: 123456789") == "123456789"
    assert extract_reference_id("Ticket Number: 0987654321") == "0987654321"
    assert extract_reference_id("TXN123ABC456") == "TXN123ABC456"
    assert extract_reference_id("some junk string without a clear ID") is None
    assert extract_reference_id("NaN") is None
    assert extract_reference_id("") is None

def test_is_credit():
    # Amex logic: negative is credit
    assert _is_credit({"_bank": "amex", "amount": -100}) is True
    assert _is_credit({"_bank": "amex", "amount": 100}) is False
    # BoFA logic: positive is credit
    assert _is_credit({"_bank": "bofa", "amount": 50}) is True
    assert _is_credit({"_bank": "bofa", "amount": -50}) is False

def test_is_likely_refund():
    # Must be a credit first
    assert _is_likely_refund({"is_credit": False, "description": "Refund", "category": ""}) is False
    
    # Exclude payment keywords
    assert _is_likely_refund({"is_credit": True, "description": "Autopay Payment - Thank You", "category": ""}) is False
    assert _is_likely_refund({"is_credit": True, "description": "reward points", "category": ""}) is False
    
    # Valid refund
    assert _is_likely_refund({"is_credit": True, "description": "TARGET RETURN", "category": "Shopping"}) is True

def test_parse_csv_amex():
    path = TEST_DATA_DIR / "amex" / "amex_sample.csv"
    
    # Run parse_csv
    df = parse_csv(str(path))
    
    # The sample has 5 rows:
    # 1: DELTA AIR LINES (11.20) -> Purchase -> Kept
    # 2: Platinum Digital Entertainment Credit (-7.01) -> Refund -> Kept
    # 3: AUTOPAY PAYMENT (-1430.73) -> Payment/Null category -> Filtered
    # 4: AIRBNB (-2190.75) -> Refund -> Kept
    # 5: FUTBOL CLUB (175.24) -> Purchase -> Kept
    # Expect 4 rows
    assert len(df) == 4
    
    # Test normalization of amount to positive
    assert all(df["amount"] > 0)
    
    # Verify row 4 (Return) is kept and identified as refund
    refund_row = df[df["description"].str.contains("AIRBNB")].iloc[0]
    assert bool(refund_row["is_refund"]) is True
    assert bool(refund_row["is_credit"]) is True
    assert refund_row["amount"] == 2190.75
    
    # Verify regular purchase
    purchase_row = df[df["description"] == "DELTA AIR LINES"].iloc[0]
    assert bool(purchase_row["is_refund"]) is False
    assert bool(purchase_row["is_credit"]) is False
    assert purchase_row["amount"] == 11.20

def test_parse_csv_bofa():
    path = TEST_DATA_DIR / "bofa" / "bofa_sample.csv"
    
    # Run parse_csv
    df = parse_csv(str(path))
    
    # sample has 5 rows:
    # 1: AMAZON MKTPL (-39.54) -> Kept
    # 2: HEADWAY (-25.00) -> Kept
    # 3: CASH REWARDS STATEMENT CREDIT (417.57) -> Filtered (Payment/Reward)
    # 4: Maya Mobile (-5.99) -> Kept
    # 5: TARGET RETURN (50.00) -> Kept (Refund)
    # Expect 4 rows
    assert len(df) == 4
    
    # Test normalization to positive amount
    assert all(df["amount"] > 0)
    
    refund_row = df[df["description"] == "TARGET RETURN"].iloc[0]
    assert bool(refund_row["is_refund"]) is True
    assert bool(refund_row["is_credit"]) is True
    assert refund_row["amount"] == 50.0

    expense_row = df[df["description"].str.contains("AMAZON")].iloc[0]
    assert bool(expense_row["is_refund"]) is False
    assert bool(expense_row["is_credit"]) is False
    assert expense_row["amount"] == 39.54
