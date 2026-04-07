import pytest
from unittest.mock import patch, MagicMock
from src.merchant_review.review_merchants import main

@patch("src.merchant_review.review_merchants.load_review_data")
@patch("src.merchant_review.review_merchants.load_feedback")
def test_main_stats(mock_load_fb, mock_load_data, capsys):
    mock_load_fb.return_value = {
        "approved": [{"description": "A"}],
        "corrected": [{"expected_merchant": "B", "corrected_merchant": "C", "category_name": "Food", "corrected_category": "T"}],
        "skipped": []
    }
    
    with patch("sys.argv", ["script", "--stats"]):
        main()
        captured = capsys.readouterr()
        assert "REVIEW STATISTICS" in captured.out
        assert "Approved: 1" in captured.out
        assert "Corrected: 1" in captured.out
