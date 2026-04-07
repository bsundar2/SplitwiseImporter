import pytest
import pandas as pd
from unittest.mock import patch, MagicMock

from src.merchant_review.review_merchants import (
    validate_category_subcategory,
    detect_lodging_in_description,
    interactive_review
)

def test_validate_category_subcategory():
    is_valid, err = validate_category_subcategory("Food and drink", "Dining out")
    assert is_valid is True
    assert err is None
    
    is_valid, err = validate_category_subcategory("Invalid Category", "Dining out")
    assert is_valid is False
    assert "Invalid category" in err
    
    is_valid, err = validate_category_subcategory("Food and drink", "Invalid Sub")
    assert is_valid is False
    assert "not valid for category" in err

def test_detect_lodging_in_description():
    assert detect_lodging_in_description("HILTON LODGING NYC") is True
    assert detect_lodging_in_description("UBER RIDE") is False

@patch("src.merchant_review.review_merchants.get_user_input")
@patch("src.merchant_review.review_merchants.load_review_data")
@patch("src.merchant_review.review_merchants.load_feedback")
@patch("src.merchant_review.review_merchants.save_feedback")
def test_interactive_review_approve(mock_save, mock_feedback, mock_load, mock_input, capsys):
    mock_df = pd.DataFrame([
        {
            "description_raw": "NETFLIX",
            "description": "Netflix",
            "expected_merchant": "Netflix",
            "category_name": "Entertainment",
            "subcategory_name": "Other",
            "date": "2026-04-01",
            "amount": 10.0
        }
    ])
    mock_load.return_value = mock_df
    
    mock_feedback_data = {"approved": [], "corrected": [], "skipped": []}
    mock_feedback.return_value = mock_feedback_data
    
    # User inputs 'a' to approve, then the script naturally moves to end since batch finishes
    mock_input.side_effect = ["a"]
    
    interactive_review(0, 1)
    
    assert len(mock_feedback_data["approved"]) == 1
    assert mock_feedback_data["approved"][0]["expected_merchant"] == "Netflix"
    mock_save.assert_called()

@patch("src.merchant_review.review_merchants.get_user_input")
@patch("src.merchant_review.review_merchants.load_review_data")
@patch("src.merchant_review.review_merchants.load_feedback")
@patch("src.merchant_review.review_merchants.save_feedback")
def test_interactive_review_skip_and_quit(mock_save, mock_feedback, mock_load, mock_input):
    mock_df = pd.DataFrame([
        {
            "description_raw": "NETFLIX", "description": "Netflix", "expected_merchant": "Netflix",
            "category_name": "Entertainment", "subcategory_name": "Other", "date": "2026-04-01", "amount": 10.0
        },
        {
            "description_raw": "UBER", "description": "Uber", "expected_merchant": "Uber",
            "category_name": "Transportation", "subcategory_name": "Taxi", "date": "2026-04-02", "amount": 20.0
        }
    ])
    mock_load.return_value = mock_df
    
    mock_feedback_data = {"approved": [], "corrected": [], "skipped": []}
    mock_feedback.return_value = mock_feedback_data
    
    # User inputs 's' to skip Netflix, then 'q' to quit on Uber
    mock_input.side_effect = ["s", "q"]
    
    interactive_review(0, 2)
    
    assert len(mock_feedback_data["skipped"]) == 1
    assert len(mock_feedback_data["approved"]) == 0
    assert len(mock_feedback_data["corrected"]) == 0
    assert mock_feedback_data["skipped"][0]["expected_merchant"] == "Netflix"
    mock_save.assert_called()
