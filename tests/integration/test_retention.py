from mikroflow.config import Settings
from mikroflow.worker.retention import run_maintenance


def _partition_names(pool, prefix):
    with pool.connection() as conn:
        return {
            r[0]
            for r in conn.execute(
                "SELECT tablename FROM pg_tables WHERE tablename LIKE %s",
                (prefix + "%",),
            ).fetchall()
        }


def test_maintenance_drops_old_and_keeps_recent(pool):
    # create an old daily partition well outside the retention window
    with pool.connection() as conn:
        conn.execute("SELECT ensure_daily_partition('2000-01-01')")
    assert "flows_raw_20000101" in _partition_names(pool, "flows_raw_")

    run_maintenance(pool, Settings())

    names = _partition_names(pool, "flows_raw_")
    assert "flows_raw_20000101" not in names   # dropped
    assert len(names) >= 1                       # recent window still present
