from datetime import datetime, timezone

from mikroflow.models import Flow
from mikroflow.sinks import PostgresFlowSink


def test_write_batch_copies_rows(pool):
    now = datetime(2026, 7, 1, 12, 30, tzinfo=timezone.utc)
    flows = [
        Flow(now, "192.168.1.10", "8.8.8.8", 5000, 443, 6, 1500, 12, "10.0.0.1"),
        Flow(now, "192.168.1.11", "1.1.1.1", 5001, 53, 17, 90, 1, "10.0.0.1"),
    ]
    PostgresFlowSink(pool).write_batch(flows)
    with pool.connection() as conn:
        count = conn.execute("SELECT count(*) FROM flows_raw").fetchone()[0]
        one = conn.execute(
            "SELECT host(src_ip), host(dst_ip), dst_port, bytes FROM flows_raw "
            "WHERE dst_port = 443"
        ).fetchone()
    assert count == 2
    assert one == ("192.168.1.10", "8.8.8.8", 443, 1500)


def test_write_batch_empty_is_noop(pool):
    PostgresFlowSink(pool).write_batch([])
    with pool.connection() as conn:
        assert conn.execute("SELECT count(*) FROM flows_raw").fetchone()[0] == 0
