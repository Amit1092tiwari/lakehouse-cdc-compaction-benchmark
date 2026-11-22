import pytest
from benchmark_cdc import generate_cdc_batch

def test_generate_cdc_batch_structure():
    """Verify CDC batch record fields and integrity."""
    records = generate_cdc_batch(batch_id=1, records_per_batch=20)
    assert len(records) == 20
    
    valid_ops = {"INSERT", "UPDATE", "DELETE"}
    for r in records:
        assert r["op"] in valid_ops
        assert r["entity_id"].startswith("user_account_")
        assert isinstance(r["balance_cents"], int)
        assert r["version"] == 1
        assert "commit_timestamp" in r
        assert "payload_tag" in r

def test_cdc_batch_entity_distribution():
    """Verify distinct entity IDs across multiple batches."""
    batch_0 = generate_cdc_batch(batch_id=0, records_per_batch=10)
    batch_1 = generate_cdc_batch(batch_id=1, records_per_batch=10)
    
    assert len(batch_0) == 10
    assert len(batch_1) == 10
    assert batch_0[0]["entity_id"] != batch_1[0]["entity_id"]
