def test_tables_and_partitions_exist(pool):
    with pool.connection() as conn:
        tables = {
            r[0]
            for r in conn.execute(
                "SELECT tablename FROM pg_tables WHERE schemaname='public'"
            ).fetchall()
        }
    assert {"flows_raw", "dhcp_leases", "ip_domain", "flows_processed", "agg_state"} <= tables
    # a daily partition for today and a monthly partition for this month exist
    assert any(t.startswith("flows_raw_2") for t in tables)
    assert any(t.startswith("flows_processed_2") for t in tables)


def test_view_is_queryable(pool):
    with pool.connection() as conn:
        rows = conn.execute("SELECT * FROM v_connections").fetchall()
    assert rows == []


def test_migrates_legacy_flows_hourly(pool):
    from mikroflow.db import apply_schema

    with pool.connection() as conn:
        conn.execute("DROP TABLE IF EXISTS flows_hourly CASCADE")
        conn.execute(
            "CREATE TABLE flows_hourly (hour timestamptz NOT NULL, "
            "src_ip inet NOT NULL, dst_ip inet NOT NULL) PARTITION BY RANGE (hour)"
        )
        conn.execute(
            "INSERT INTO agg_state (name, last_hour) VALUES ('hourly', now())"
        )
    apply_schema(pool)  # DO-block should drop the legacy table entirely
    with pool.connection() as conn:
        exists = conn.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_name = 'flows_hourly'"
        ).fetchone()
        legacy_state = conn.execute(
            "SELECT 1 FROM agg_state WHERE name = 'hourly'"
        ).fetchone()
        cols = {
            r[0]
            for r in conn.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'flows_processed'"
            ).fetchall()
        }
    assert exists is None
    assert legacy_state is None
    assert {"ts", "src_ip", "dst_ip", "device_name", "mac", "remote_domain"} <= cols
