from datetime import datetime, timedelta, timezone

# Copies each flow from flows_raw into flows_processed as-is (same columns,
# same one-row-per-flow granularity, original ts untouched) and adds three
# enrichment columns: device_name/mac for whichever side of the flow is the
# LAN endpoint, and remote_domain (reverse-DNS) for the other side. Unlike
# the old hourly aggregator, nothing is grouped or summed, so no time
# information is lost.
_PROCESS_SQL = """
INSERT INTO flows_processed
    (ts, src_ip, dst_ip, src_port, dst_port, protocol, bytes, packets,
     exporter_ip, device_name, mac, remote_domain)
SELECT
    f.ts,
    f.src_ip,
    f.dst_ip,
    f.src_port,
    f.dst_port,
    f.protocol,
    f.bytes,
    f.packets,
    f.exporter_ip,
    coalesce(al.name, l.hostname) AS device_name,
    coalesce(l.mac, a.mac) AS mac,
    d.domain AS remote_domain
FROM flows_raw f
CROSS JOIN LATERAL (
    -- Which side is the LAN device: the inbound dst only when dst is
    -- private and src is public; otherwise src.
    SELECT
        CASE WHEN is_private(f.dst_ip) AND NOT is_private(f.src_ip)
             THEN f.dst_ip ELSE f.src_ip END AS device_ip,
        CASE WHEN is_private(f.dst_ip) AND NOT is_private(f.src_ip)
             THEN f.src_ip ELSE f.dst_ip END AS remote_ip
) o
LEFT JOIN LATERAL (
    -- keep hostname and MAC separately; empty hostname becomes NULL
    SELECT nullif(l.hostname, '') AS hostname, l.mac AS mac
    FROM dhcp_leases l
    WHERE l.ip = o.device_ip
    ORDER BY
        -- prefer the lease that actually covered this exact flow ts...
        (l.valid_from <= f.ts
         AND (l.valid_to IS NULL OR l.valid_to > f.ts)) DESC,
        -- ...otherwise fall back to the lease nearest in time (handles the
        -- bootstrap window where lease history starts after the flow)
        abs(extract(epoch FROM (l.valid_from - f.ts)))
    LIMIT 1
) l ON true
LEFT JOIN arp a ON a.ip = o.device_ip
LEFT JOIN device_alias al ON upper(al.mac) = upper(coalesce(l.mac, a.mac))
LEFT JOIN ip_domain d ON d.ip = o.remote_ip
WHERE f.ts >= %(hour)s AND f.ts < %(hour)s + interval '1 hour'
"""


def process(pool, now=None):
    now = now or datetime.now(timezone.utc)
    boundary = now.replace(minute=0, second=0, microsecond=0)
    with pool.connection() as conn:
        row = conn.execute(
            "SELECT last_hour FROM agg_state WHERE name = 'processed'"
        ).fetchone()
        last = row[0] if row else None
        if last is None:
            first = conn.execute(
                "SELECT date_trunc('hour', min(ts)) FROM flows_raw"
            ).fetchone()[0]
            if first is None:
                return
            last = first
        hour = last
        while hour < boundary:
            conn.execute(_PROCESS_SQL, {"hour": hour})
            hour = hour + timedelta(hours=1)
            conn.execute(
                "INSERT INTO agg_state (name, last_hour) VALUES ('processed', %s) "
                "ON CONFLICT (name) DO UPDATE SET last_hour = EXCLUDED.last_hour",
                (hour,),
            )
