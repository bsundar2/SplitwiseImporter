import pytest
import os
from datetime import datetime

from src.database.db_manager import DatabaseManager
from src.database.models import Transaction, ImportLog

@pytest.fixture
def db_manager(tmp_path):
    # Use a temporary file for the database path
    db_file = tmp_path / "test_transactions.db"
    return DatabaseManager(db_path=str(db_file))

def create_txn(**kwargs):
    defaults = {
        "source": "test_src",
        "imported_at": "2026-04-01T00:00:00Z"
    }
    defaults.update(kwargs)
    return Transaction(**defaults)

def test_db_initialization(tmp_path):
    db_file = tmp_path / "init_test.db"
    assert not os.path.exists(db_file)
    db = DatabaseManager(str(db_file))
    assert os.path.exists(db_file)
    
    # Check tables created
    conn = db.get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [r["name"] for r in cursor.fetchall()]
    conn.close()
    
    assert "transactions" in tables
    assert "import_log" in tables

def test_insert_and_get_transaction(db_manager):
    txn = create_txn(
        date="2026-04-01",
        amount=100.50,
        merchant="Test Merchant",
        source="amex"
    )
    
    txn_id = db_manager.insert_transaction(txn)
    assert txn_id is not None
    
    retrieved = db_manager.get_transaction_by_id(txn_id)
    assert retrieved is not None
    assert retrieved.merchant == "Test Merchant"
    assert retrieved.amount == 100.50
    assert retrieved.date == "2026-04-01"

def test_batch_insert(db_manager):
    txns = [
        create_txn(date="2026-04-01", amount=10.0, merchant="Merch A"),
        create_txn(date="2026-04-02", amount=20.0, merchant="Merch B")
    ]
    ids = db_manager.insert_transactions_batch(txns)
    assert len(ids) == 2
    
    stats = db_manager.get_stats()
    assert stats["total_transactions"] == 2

def test_update_transaction(db_manager):
    txn = create_txn(date="2026-04-01", amount=50.0, merchant="To Update")
    txn_id = db_manager.insert_transaction(txn)
    
    success = db_manager.update_transaction(txn_id, {"merchant": "Updated Merch", "amount": 55.0})
    assert success is True
    
    retrieved = db_manager.get_transaction_by_id(txn_id)
    assert retrieved.merchant == "Updated Merch"
    assert retrieved.amount == 55.0

def test_get_transactions_by_date_range(db_manager):
    db_manager.insert_transactions_batch([
        create_txn(date="2026-04-01", amount=10.0, merchant="Day 1"),
        create_txn(date="2026-04-15", amount=15.0, merchant="Day 15"),
        create_txn(date="2026-05-01", amount=20.0, merchant="Day 1 May")
    ])
    
    res = db_manager.get_transactions_by_date_range("2026-04-01", "2026-04-30")
    assert len(res) == 2
    assert res[0].merchant == "Day 1"
    assert res[1].merchant == "Day 15"

def test_get_unwritten_transactions(db_manager):
    db_manager.insert_transactions_batch([
        create_txn(date="2026-04-01", amount=10.0, merchant="Unwritten"),
        create_txn(date="2026-04-02", amount=20.0, merchant="Written", written_to_sheet=1)
    ])
    
    unwritten = db_manager.get_unwritten_transactions()
    assert len(unwritten) == 1
    assert unwritten[0].merchant == "Unwritten"

def test_find_original_for_refund(db_manager):
    # Insert original
    db_manager.insert_transaction(
        create_txn(date="2026-04-01", amount=100.0, merchant="Amazon", is_refund=0, cc_reference_id="ref123")
    )
    
    # 1. Exact match by reference
    found1 = db_manager.find_original_for_refund(
        refund_amount=100.0, refund_date="2026-04-10", merchant="Amazon", cc_reference_id="ref123"
    )
    assert found1 is not None
    assert found1.cc_reference_id == "ref123"
    
    # 2. Match by merchant/date window/amount
    found2 = db_manager.find_original_for_refund(
        refund_amount=100.0, refund_date="2026-04-10", merchant="Amazon"
    )
    assert found2 is not None
    assert found2.amount == 100.0

    # 3. No match
    not_found = db_manager.find_original_for_refund(
        refund_amount=500.0, refund_date="2026-04-10", merchant="Walmart"
    )
    assert not_found is None
