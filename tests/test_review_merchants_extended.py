import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
from src.merchant_review.review_merchants import (
    load_review_data,
    interactive_review,
    main,
)

@pytest.fixture
def sample_review_df():
    return pd.DataFrame([
        {"description_raw": "NETFLIX.COM", "description": "Netflix", "amount": 15.99, "count": 1, "date": "2026-04-01", "category_name": "Entertainment", "subcategory_name": "Movies", "expected_merchant": "Netflix"},
        {"description_raw": "UBER* RIDE", "description": "Uber", "amount": 25.0, "count": 1, "date": "2026-04-02", "category_name": "Transportation", "subcategory_name": "Taxi", "expected_merchant": "Uber"},
    ])

def test_load_review_data_missing():
    # Path is mocked to avoid real file interaction
    with patch("src.merchant_review.review_merchants.REVIEW_FILE") as mock_file:
        mock_file.exists.return_value = False
        df = load_review_data()
        assert df.empty

@patch("src.merchant_review.review_merchants.input")
@patch("src.merchant_review.review_merchants.load_review_data")
@patch("src.merchant_review.review_merchants.save_feedback")
def test_interactive_review_flow(mock_save, mock_load, mock_input, sample_review_df):
    mock_load.return_value = sample_review_df
    # Mock inputs for 2 merchants:
    # 1. Netflix - Approve (a)
    # 2. Uber - Correct (c) -> New name: Uber Ride -> New category: Transportation -> New subcat: Taxi -> Action stays 'c'
    # Then Quit (q)
    mock_input.side_effect = ["a", "c", "Uber Ride", "Transportation", "Taxi", "q"]
    
    # We need to mock display_transaction to avoid messy stdout in tests
    with patch("src.merchant_review.review_merchants.display_transaction"):
        # We also need to mock load_feedback to start fresh
        with patch("src.merchant_review.review_merchants.load_feedback", return_value={"approved": [], "corrected": [], "skipped": []}):
            interactive_review()
    
    mock_save.assert_called()
    # Check what was saved
    saved_feedback = mock_save.call_args[0][0]
    assert len(saved_feedback["approved"]) == 1
    assert saved_feedback["approved"][0]["description"] == "Netflix"
    assert len(saved_feedback["corrected"]) == 1
    assert saved_feedback["corrected"][0]["corrected_merchant"] == "Uber Ride"

@patch("src.merchant_review.review_merchants.load_review_data")
@patch("src.merchant_review.review_merchants.interactive_review")
def test_main_cli(mock_review, mock_load, sample_review_df):
    mock_load.return_value = sample_review_df
    
    with patch("sys.argv", ["script", "--start", "0"]):
        main()
        mock_review.assert_called_once()
