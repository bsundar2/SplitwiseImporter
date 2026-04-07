import pytest
from unittest.mock import patch, MagicMock
from src.import_statement.process_refunds import RefundProcessor
from src.database.models import Transaction

def test_process_refund_dry_run():
    mock_db = MagicMock()
    mock_client = MagicMock()
    processor = RefundProcessor(db=mock_db, client=mock_client)
    
    txn = Transaction(date="2026-04-01", amount=-50.0, merchant="Amazon", source="amex", cc_reference_id="ref123", imported_at="2026-04-01")
    txn.id = 1
    
    result = processor.process_refund(txn, dry_run=True)
    
    assert result["status"] == "would_create"
    mock_client.add_expense_from_txn.assert_not_called()
    mock_db.update_transaction.assert_not_called()

def test_process_refund_live():
    mock_db = MagicMock()
    mock_client = MagicMock()
    mock_client.get_current_user_id.return_value = 10101
    mock_client.add_expense_from_txn.return_value = 999
    
    processor = RefundProcessor(db=mock_db, client=mock_client)
    
    txn = Transaction(date="2026-04-01", amount=-50.0, merchant="Amazon", source="amex", cc_reference_id="ref123", category="Shopping", imported_at="2026-04-01")
    txn.id = 1
    
    result = processor.process_refund(txn, dry_run=False)
    
    assert result["status"] == "created"
    assert result["splitwise_id"] == 999
    mock_client.add_expense_from_txn.assert_called_once()
    mock_db.update_transaction.assert_called_once()
    
def test_process_all_pending_refunds():
    mock_db = MagicMock()
    mock_client = MagicMock()
    
    txn1 = Transaction(date="2026-04-01", amount=-50.0, merchant="Amazon", source="amex", cc_reference_id="ref123", imported_at="2026-04-01")
    txn1.id = 1
    txn2 = Transaction(date="2026-04-02", amount=-10.0, merchant="Uber", source="amex", cc_reference_id="ref456", imported_at="2026-04-02")
    txn2.id = 2
    
    mock_db.get_pending_refunds.return_value = [txn1, txn2]
    
    processor = RefundProcessor(db=mock_db, client=mock_client)
    
    summary = processor.process_all_pending_refunds(dry_run=True)
    assert summary["total"] == 2
    assert summary["would_create"] == 2
    assert summary["created"] == 0
