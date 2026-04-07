"""Tests for db_manager monthly summary methods and models."""
import pytest
from datetime import datetime
from src.database.db_manager import DatabaseManager
from src.database.models import Transaction, ImportLog


@pytest.fixture
def db(tmp_path):
    db_file = tmp_path / "test.db"
    return DatabaseManager(db_path=str(db_file))


def create_txn(**kwargs):
    defaults = {"source": "test", "imported_at": "2026-04-01T00:00:00Z"}
    defaults.update(kwargs)
    return Transaction(**defaults)


# === Monthly summaries ===
def test_save_and_get_monthly_summary(db):
    db.save_monthly_summary(
        year_month="2026-01",
        total_spent_net=1000.0,
        avg_transaction=50.0,
        transaction_count=20,
        total_paid=1200.0,
        total_owed=200.0,
        cumulative_spending=1000.0,
        mom_change=0.0,
        written_to_sheet=False,
    )
    result = db.get_monthly_summary("2026-01")
    assert result is not None
    assert result["total_spent_net"] == 1000.0
    assert result["transaction_count"] == 20


def test_get_monthly_summary_not_found(db):
    assert db.get_monthly_summary("2099-01") is None


def test_get_all_monthly_summaries(db):
    for m in ["2026-01", "2026-02", "2025-12"]:
        db.save_monthly_summary(m, 100, 10, 10, 100, 0, 100, 0)
    
    all_summaries = db.get_all_monthly_summaries()
    assert len(all_summaries) == 3
    
    year_2026 = db.get_all_monthly_summaries(year=2026)
    assert len(year_2026) == 2


def test_mark_monthly_summary_written(db):
    db.save_monthly_summary("2026-03", 500, 50, 10, 500, 0, 500, 0, written_to_sheet=False)
    db.mark_monthly_summary_written("2026-03")
    result = db.get_monthly_summary("2026-03")
    assert result["written_to_sheet"] == 1


def test_save_monthly_summary_upsert(db):
    db.save_monthly_summary("2026-04", 100, 10, 10, 100, 0, 100, 0)
    db.save_monthly_summary("2026-04", 200, 20, 20, 200, 0, 200, 0)
    result = db.get_monthly_summary("2026-04")
    assert result["total_spent_net"] == 200.0


# === Transaction model methods ===
def test_transaction_mark_written():
    txn = create_txn(date="2026-04-01", amount=10, merchant="T")
    txn.mark_written_to_sheet(2026, row_id=5)
    assert txn.written_to_sheet is True
    assert txn.sheet_year == 2026
    assert txn.sheet_row_id == 5
    assert txn.updated_at is not None


def test_transaction_update_splitwise_id():
    txn = create_txn(date="2026-04-01", amount=10, merchant="T")
    txn.update_splitwise_id(999)
    assert txn.splitwise_id == 999
    assert txn.is_shared is True


def test_transaction_mark_deleted():
    txn = create_txn(date="2026-04-01", amount=10, merchant="T")
    txn.mark_deleted_in_splitwise()
    assert txn.splitwise_deleted_at is not None


# === ImportLog ===
def test_import_log_to_dict():
    log = ImportLog(
        timestamp="2026-04-01T00:00:00Z",
        source_type="amex",
        records_attempted=10,
        records_imported=8,
        records_skipped=2,
        records_failed=0,
    )
    d = log.to_dict()
    assert d["source_type"] == "amex"
    assert "id" not in d  # None fields filtered
