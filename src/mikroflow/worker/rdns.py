from datetime import datetime, timezone

import dns.exception
import dns.resolver
import dns.reversename


def resolve_ptr(ip, timeout):
    try:
        name = dns.reversename.from_address(ip)
        answer = dns.resolver.resolve(name, "PTR", lifetime=timeout)
        return str(answer[0]).rstrip(".")
    except dns.resolver.NXDOMAIN:
        return None
    except dns.exception.DNSException:
        raise


def pending_ips(pool, batch_size, now, neg_ttl, pos_ttl):
    with pool.connection() as conn:
        rows = conn.execute(
            """
            SELECT DISTINCT host(f.dst_ip)
            FROM flows_raw f
            LEFT JOIN ip_domain d ON d.ip = f.dst_ip
            WHERE d.ip IS NULL
               OR d.resolved_at < %s - make_interval(secs => d.ttl)
            LIMIT %s
            """,
            (now, batch_size),
        ).fetchall()
    return [r[0] for r in rows]


def upsert_domain(pool, ip, domain, status, now, ttl):
    with pool.connection() as conn:
        conn.execute(
            """
            INSERT INTO ip_domain (ip, domain, status, resolved_at, ttl)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (ip) DO UPDATE
            SET domain = EXCLUDED.domain, status = EXCLUDED.status,
                resolved_at = EXCLUDED.resolved_at, ttl = EXCLUDED.ttl
            """,
            (ip, domain, status, now, ttl),
        )


def resolve_pending(pool, settings, now=None, resolver=resolve_ptr):
    now = now or datetime.now(timezone.utc)
    ips = pending_ips(
        pool,
        settings.rdns_batch_size,
        now,
        settings.rdns_negative_ttl_seconds,
        settings.rdns_positive_ttl_seconds,
    )
    for ip in ips:
        try:
            name = resolver(ip, settings.rdns_timeout_seconds)
        except Exception:
            continue  # transient failure: retry on next cycle
        if name:
            upsert_domain(pool, ip, name, "ok", now, settings.rdns_positive_ttl_seconds)
        else:
            upsert_domain(pool, ip, None, "nxdomain", now, settings.rdns_negative_ttl_seconds)
