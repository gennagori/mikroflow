from datetime import datetime, timedelta, timezone

from mikroflow.worker.lease_sync import sync_leases


def _open_leases(pool):
    with pool.connection() as conn:
        return {
            r[0]: r[1]
            for r in conn.execute(
                "SELECT host(ip), hostname FROM dhcp_leases WHERE valid_to IS NULL"
            ).fetchall()
        }


def test_sync_opens_updates_and_closes(pool):
    t0 = datetime(2026, 7, 1, 10, 0, tzinfo=timezone.utc)
    sync_leases(pool, [("192.168.1.10", "AA", "laptop")], now=t0)
    assert _open_leases(pool) == {"192.168.1.10": "laptop"}

    # hostname change -> old row closed, new open row
    t1 = t0 + timedelta(hours=1)
    sync_leases(pool, [("192.168.1.10", "AA", "laptop-renamed")], now=t1)
    assert _open_leases(pool) == {"192.168.1.10": "laptop-renamed"}
    with pool.connection() as conn:
        total = conn.execute("SELECT count(*) FROM dhcp_leases").fetchone()[0]
    assert total == 2

    # lease disappears -> closed, no open rows
    t2 = t1 + timedelta(hours=1)
    sync_leases(pool, [], now=t2)
    assert _open_leases(pool) == {}
