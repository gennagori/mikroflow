from datetime import datetime, timezone

from mikroflow.config import Settings
from mikroflow.worker.rdns import resolve_pending


def _seed_flow(pool, dst_ip):
    now = datetime(2026, 7, 1, 12, 0, tzinfo=timezone.utc)
    with pool.connection() as conn:
        conn.execute(
            "INSERT INTO flows_raw (ts, src_ip, dst_ip, src_port, dst_port, "
            "protocol, bytes, packets, exporter_ip) VALUES "
            "(%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (now, "192.168.1.10", dst_ip, 5000, 443, 6, 1000, 5, "10.0.0.1"),
        )


def test_resolves_and_caches_ok_and_nxdomain(pool):
    _seed_flow(pool, "8.8.8.8")
    _seed_flow(pool, "203.0.113.5")
    now = datetime(2026, 7, 1, 12, 5, tzinfo=timezone.utc)

    def fake_resolver(ip, timeout):
        return "dns.google" if ip == "8.8.8.8" else None

    resolve_pending(pool, Settings(), now=now, resolver=fake_resolver)

    with pool.connection() as conn:
        rows = {
            r[0]: (r[1], r[2])
            for r in conn.execute(
                "SELECT host(ip), domain, status FROM ip_domain"
            ).fetchall()
        }
    assert rows["8.8.8.8"] == ("dns.google", "ok")
    assert rows["203.0.113.5"] == (None, "nxdomain")

    # second run resolves nothing new (all cached and fresh)
    calls = []
    resolve_pending(pool, Settings(), now=now,
                    resolver=lambda ip, t: calls.append(ip) or "x")
    assert calls == []
