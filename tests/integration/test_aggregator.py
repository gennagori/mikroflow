from datetime import datetime, timedelta, timezone

from mikroflow.worker.aggregator import aggregate

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


def test_both_directions_fold_into_one_connection_on_lan_device(pool):
    with pool.connection() as conn:
        _lease(conn, "10.59.0.69", "BC:5F:F4:51:6B:F8", "DESKTOP-7N3LP6O",
               HOUR - timedelta(days=1))
        conn.execute(
            "INSERT INTO ip_domain (ip, domain, status, resolved_at, ttl) "
            "VALUES (%s,%s,%s,%s,%s)",
            ("8.8.8.8", "dns.google", "ok", HOUR, 86400),
        )
        # outbound: LAN client -> server:443
        _flow(conn, "10.59.0.69", "8.8.8.8", 52000, 443, 100)
        # inbound (return): server:443 -> LAN client
        _flow(conn, "8.8.8.8", "10.59.0.69", 443, 52000, 900)
    aggregate(pool, now=HOUR + timedelta(hours=2))
    with pool.connection() as conn:
        rows = conn.execute(
            "SELECT device_name, mac, host(device_ip), remote_domain, "
            "host(remote_ip), remote_port, bytes, flow_count "
            "FROM flows_hourly WHERE hour = %s", (HOUR,)
        ).fetchall()
    # a single folded row, device = the LAN host, bytes summed both directions
    assert rows == [
        ("DESKTOP-7N3LP6O", "BC:5F:F4:51:6B:F8", "10.59.0.69", "dns.google",
         "8.8.8.8", 443, 1000, 2),
    ]


def test_mac_used_when_hostname_missing(pool):
    with pool.connection() as conn:
        _lease(conn, "10.59.0.98", "0E:3E:8C:C8:B7:5A", "", HOUR - timedelta(days=1))
        _flow(conn, "10.59.0.98", "1.1.1.1", 40000, 443, 100)
    aggregate(pool, now=HOUR + timedelta(hours=2))
    with pool.connection() as conn:
        row = conn.execute(
            "SELECT device_name, mac FROM flows_hourly WHERE hour = %s", (HOUR,)
        ).fetchone()
    assert row == (None, "0E:3E:8C:C8:B7:5A")


def test_nearest_lease_used_when_none_covers_hour(pool):
    with pool.connection() as conn:
        # lease first seen after the flow hour (bootstrap window)
        _lease(conn, "10.59.1.88", "D6:3B:60:D6:2F:11", "iPhone",
               HOUR + timedelta(minutes=54))
        _flow(conn, "10.59.1.88", "9.9.9.9", 33000, 443, 50)
    aggregate(pool, now=HOUR + timedelta(hours=2))
    with pool.connection() as conn:
        row = conn.execute(
            "SELECT device_name FROM flows_hourly WHERE hour = %s", (HOUR,)
        ).fetchone()
    assert row == ("iPhone",)


def test_aggregate_is_idempotent(pool):
    with pool.connection() as conn:
        _lease(conn, "10.59.0.69", "AA", "laptop", HOUR - timedelta(days=1))
        _flow(conn, "10.59.0.69", "8.8.8.8", 52000, 443, 100)
        _flow(conn, "8.8.8.8", "10.59.0.69", 443, 52000, 100)
    now = HOUR + timedelta(hours=2)
    aggregate(pool, now=now)
    aggregate(pool, now=now)  # second run must not double count
    with pool.connection() as conn:
        row = conn.execute(
            "SELECT bytes, flow_count FROM flows_hourly WHERE hour = %s", (HOUR,)
        ).fetchone()
    assert row == (200, 2)
