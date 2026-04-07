import pytest
from unittest.mock import patch, MagicMock
import sys
from src.merchant_review.apply_review_feedback import (
    main,
)

@patch("src.merchant_review.apply_review_feedback.load_feedback")
@patch("src.merchant_review.apply_review_feedback.apply_corrections")
@patch("src.merchant_review.apply_review_feedback.generate_report")
@patch("src.merchant_review.apply_review_feedback.move_reviewed_to_done")
def test_main_cli_live(mock_move, mock_report, mock_apply, mock_load):
    mock_load.return_value = {"approved": [{"id": 1}], "corrected": [], "skipped": []}
    mock_apply.return_value = {"added": 1, "updated": 0, "unchanged": 0, "changes": []}
    
    with patch("sys.argv", ["script"]):
        main()
        mock_apply.assert_called_once()
        mock_move.assert_called_once()

@patch("src.merchant_review.apply_review_feedback.load_feedback")
@patch("src.merchant_review.apply_review_feedback.apply_corrections")
def test_main_cli_dry_run(mock_apply, mock_load):
    mock_load.return_value = {"approved": [{"id": 1}], "corrected": [], "skipped": []}
    mock_apply.return_value = {"added": 1, "updated": 0, "unchanged": 0, "changes": []}
    
    with patch("sys.argv", ["script", "--dry-run"]):
        main()
        mock_apply.assert_called_once_with(mock_load.return_value, dry_run=True)

@patch("src.merchant_review.apply_review_feedback.load_feedback")
def test_main_cli_no_feedback(mock_load, capsys):
    mock_load.return_value = {"approved": [], "corrected": [], "skipped": []}
    with patch("sys.argv", ["script"]):
        main()
    captured = capsys.readouterr()
    assert "No feedback to apply yet" in captured.out

def test_analyze_correction_patterns(capsys):
    from src.merchant_review.apply_review_feedback import analyze_correction_patterns
    feedback = {
        "corrected": [
            {
                "expected_merchant": "Netflix",
                "corrected_merchant": "Netflix Inc",
                "category_name": "Entertainment",
                "corrected_category": "Services"
            }
        ]
    }
    analyze_correction_patterns(feedback)
    captured = capsys.readouterr()
    assert "CORRECTION PATTERNS ANALYSIS" in captured.out
    assert "Merchant name corrections: 1" in captured.out
