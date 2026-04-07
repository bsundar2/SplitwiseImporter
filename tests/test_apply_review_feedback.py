import pytest
import pandas as pd
from unittest.mock import patch, MagicMock

from src.merchant_review.apply_review_feedback import main

@patch("src.merchant_review.apply_review_feedback.load_feedback")
@patch("src.merchant_review.apply_review_feedback.load_merchant_lookup")
@patch("src.merchant_review.apply_review_feedback.save_merchant_lookup")
@patch("src.merchant_review.apply_review_feedback.move_reviewed_to_done")
def test_main(mock_move, mock_save, mock_load_lookup, mock_load_feedback):
    # Mock returning something from feedback file
    mock_load_feedback.return_value = {
        "approved": [
            {"description_raw": "RAW_NET", "description": "Netflix", "expected_merchant": "Netflix", "category_name": "Ent", "subcategory_name": "Stream"}
        ],
        "corrected": [
            {"description_raw": "RAW_UBER", "description": "Uber", "expected_merchant": "uber", "corrected_merchant": "Uber", "corrected_category": "Trans", "corrected_subcategory": "Taxi"}
        ],
        "skipped": []
    }
    mock_load_lookup.return_value = {}
    
    with patch("sys.argv", ["script"]):
        main()
        
    mock_save.assert_called_once()
    mock_move.assert_called_once()
    
    # Run with --dry-run
    mock_save.reset_mock()
    mock_move.reset_mock()
    with patch("sys.argv", ["script", "--dry-run"]):
        main()
    mock_save.assert_not_called()
    mock_move.assert_not_called()

    # Run with empty
    mock_load_feedback.return_value = {"approved": [], "corrected": [], "skipped": []}
    mock_save.reset_mock()
    with patch("sys.argv", ["script"]):
        main()
    mock_save.assert_not_called()
