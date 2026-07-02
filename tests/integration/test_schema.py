def test_tables_and_partitions_exist(pool):
    with pool.connection() as conn:
        tables = {
            r[0]
            for r in conn.execute(
                "SELECT tablename FROM pg_tables WHERE schemaname='public'"
            ).fetchall()
        }
    assert {"flows_raw", "dhcp_leases", "ip_domain", "flows_hourly", "agg_state"} <= tables
    # a daily partition for today and a monthly partition for this month exist
    assert any(t.startswith("flows_raw_2") for t in tables)
    assert any(t.startswith("flows_hourly_2") for t in tables)


def test_view_is_queryable(pool):
    with pool.connection() as conn:
        rows = conn.execute("SELECT * FROM v_connections").fetchall()
    assert rows == []
