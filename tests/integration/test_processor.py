from datetime import datetime, timedelta, timezone

from mikroflow.worker.processor import process

HOUR = datetime(2026, 7, 1, 10, 0, tzinfo=timezone.utc)


def _lease(conn, ip, mac, hostname, valid_from):
    conn.execute(
        "INSERT INTO dhcp_leases (ip, mac, hostname, valid_from) VALUES (%s,%s,%s,%s)",
        (ip, mac, hostname, valid_from),
    )


def _flow(conn, src_ip, dst_ip, src_port, dst_port, bytes_, ts=None):
    conn.execute(
        "INSERT INTO flows_raw (ts, src_ip, dst_ip, src_port, dst_port, "
        "protocol, bytes, packets, exporter_ip) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (ts or HOUR + timedelta(minutes=5), src_ip, dst_ip, src_port, dst_port,
         6, bytes_, 1, "10.0.0.1"),
    )


def test_each_flow_keeps_its_own_timestamp_and_device_info(pool):
    # This is the whole point of flows_processed: unlike the old hourly
    # aggregator, every flow keeps its exact original ts instead of being
    # collapsed into a single per-hour bucket.
    t1 = HOUR + timedelta(minutes=5)
    t2 = HOUR + timedelta(minutes=47, seconds=13)
    with pool.connection() as conn:
        _lease(conn, "10.59.0.69", "BC:5F:F4:51:6B:F8", "DESKTOP-7N3LP6O",
               HOUR - timedelta(days=1))
        conn.execute(
            "INSERT INTO ip_domain (ip, domain, status, resolved_at, ttl) "
            "VALUES (%s,%s,%s,%s,%s)",
            ("8.8.8.8", "dns.google", "ok", HOUR, 86400),
        )
        # outbound: LAN client -> server:443
        _flow(conn, "10.59.0.69", "8.8.8.8", 52000, 443, 100, ts=t1)
        # inbound (return): server:443 -> LAN client, at a different moment
        _flow(conn, "8.8.8.8", "10.59.0.69", 443, 52000, 900, ts=t2)
    process(pool, now=HOUR + timedelta(hours=2))
    with pool.connection() as conn:
        rows = conn.execute(
            "SELECT ts, device_name, mac, host(device_ip), remote_domain, "
            "host(remote_ip), remote_port, bytes "
            "FROM v_connections ORDER BY ts"
        ).fetchall()
    # two separate rows (nothing folded/summed), each with its own exact ts
    assert rows == [
        (t1, "DESKTOP-7N3LP6O", "BC:5F:F4:51:6B:F8", "10.59.0.69",
         "dns.google", "8.8.8.8", 443, 100),
        (t2, "DESKTOP-7N3LP6O", "BC:5F:F4:51:6B:F8", "10.59.0.69",
         "dns.google", "8.8.8.8", 443, 900),
    ]


def test_mac_used_when_hostname_missing(pool):
    with pool.connection() as conn:
        _lease(conn, "10.59.0.98", "0E:3E:8C:C8:B7:5A", "", HOUR - timedelta(days=1))
        _flow(conn, "10.59.0.98", "1.1.1.1", 40000, 443, 100)
    process(pool, now=HOUR + timedelta(hours=2))
    with pool.connection() as conn:
        row = conn.execute(
            "SELECT device_name, mac FROM flows_processed WHERE ts = %s",
            (HOUR + timedelta(minutes=5),),
        ).fetchone()
    assert row == (None, "0E:3E:8C:C8:B7:5A")


def test_mac_from_arp_when_no_dhcp_lease(pool):
    # static host: no DHCP lease, but present in the ARP snapshot
    with pool.connection() as conn:
        conn.execute(
            "INSERT INTO arp (ip, mac, updated_at) VALUES (%s,%s, now())",
            ("10.59.0.99", "BC:24:11:DB:51:16"),
        )
        _flow(conn, "10.59.0.99", "8.8.8.8", 40000, 443, 100)
    process(pool, now=HOUR + timedelta(hours=2))
    with pool.connection() as conn:
        row = conn.execute(
            "SELECT device_name, mac FROM flows_processed WHERE ts = %s",
            (HOUR + timedelta(minutes=5),),
        ).fetchone()
    assert row == (None, "BC:24:11:DB:51:16")


def test_alias_overrides_hostname(pool):
    with pool.connection() as conn:
        _lease(conn, "10.59.0.69", "BC:5F:F4:51:6B:F8", "DESKTOP-7N3LP6O",
               HOUR - timedelta(days=1))
        # alias entered in lowercase to prove case-insensitive matching
        conn.execute(
            "INSERT INTO device_alias (mac, name) VALUES (%s,%s)",
            ("bc:5f:f4:51:6b:f8", "CEO Laptop"),
        )
        _flow(conn, "10.59.0.69", "8.8.8.8", 52000, 443, 100)
    process(pool, now=HOUR + timedelta(hours=2))
    with pool.connection() as conn:
        row = conn.execute(
            "SELECT device_name, mac FROM flows_processed WHERE ts = %s",
            (HOUR + timedelta(minutes=5),),
        ).fetchone()
    assert row == ("CEO Laptop", "BC:5F:F4:51:6B:F8")


def test_alias_matches_arp_only_mac(pool):
    with pool.connection() as conn:
        # static host: no DHCP lease, MAC only known via ARP
        conn.execute(
            "INSERT INTO arp (ip, mac, updated_at) VALUES (%s,%s, now())",
            ("10.59.0.99", "BC:24:11:DB:51:16"),
        )
        conn.execute(
            "INSERT INTO device_alias (mac, name) VALUES (%s,%s)",
            ("BC:24:11:DB:51:16", "srv-backup"),
        )
        _flow(conn, "10.59.0.99", "8.8.8.8", 40000, 443, 100)
    process(pool, now=HOUR + timedelta(hours=2))
    with pool.connection() as conn:
        row = conn.execute(
            "SELECT device_name, mac FROM flows_processed WHERE ts = %s",
            (HOUR + timedelta(minutes=5),),
        ).fetchone()
    assert row == ("srv-backup", "BC:24:11:DB:51:16")


def test_nearest_lease_used_when_none_covers_flow_ts(pool):
    with pool.connection() as conn:
        # lease first seen after the flow (bootstrap window)
        _lease(conn, "10.59.1.88", "D6:3B:60:D6:2F:11", "iPhone",
               HOUR + timedelta(minutes=54))
        _flow(conn, "10.59.1.88", "9.9.9.9", 33000, 443, 50)
    process(pool, now=HOUR + timedelta(hours=2))
    with pool.connection() as conn:
        row = conn.execute(
            "SELECT device_name FROM flows_processed WHERE ts = %s",
            (HOUR + timedelta(minutes=5),),
        ).fetchone()
    assert row == ("iPhone",)


def test_process_is_idempotent(pool):
    with pool.connection() as conn:
        _lease(conn, "10.59.0.69", "AA", "laptop", HOUR - timedelta(days=1))
        _flow(conn, "10.59.0.69", "8.8.8.8", 52000, 443, 100)
        _flow(conn, "8.8.8.8", "10.59.0.69", 443, 52000, 100)
    now = HOUR + timedelta(hours=2)
    process(pool, now=now)
    process(pool, now=now)  # second run must not duplicate rows
    with pool.connection() as conn:
        count = conn.execute("SELECT count(*) FROM flows_processed").fetchone()[0]
    assert count == 2
