from datetime import datetime, timedelta, timezone

_AGG_SQL = """
INSERT INTO flows_hourly
    (hour, device_ip, device_name, mac, remote_ip, remote_domain, remote_port,
     protocol, bytes, packets, flow_count)
SELECT
    date_trunc('hour', o.ts) AS hour,
    o.device_ip,
    coalesce(al.name, l.hostname) AS device_name,
    coalesce(l.mac, a.mac) AS mac,
    o.remote_ip,
    d.domain,
    o.remote_port,
    o.protocol,
    sum(o.bytes),
    sum(o.packets),
    count(*)
FROM (
    -- Fold both NetFlow directions onto the LAN endpoint. The device is the
    -- inbound dst only when dst is private and src is public; otherwise src.
    SELECT
        f.ts,
        f.protocol,
        f.bytes,
        f.packets,
        CASE WHEN is_private(f.dst_ip) AND NOT is_private(f.src_ip)
             THEN f.dst_ip ELSE f.src_ip END AS device_ip,
        CASE WHEN is_private(f.dst_ip) AND NOT is_private(f.src_ip)
             THEN f.src_ip ELSE f.dst_ip END AS remote_ip,
        CASE WHEN is_private(f.dst_ip) AND NOT is_private(f.src_ip)
             THEN f.src_port ELSE f.dst_port END AS remote_port
    FROM flows_raw f
    WHERE f.ts >= %(hour)s AND f.ts < %(hour)s + interval '1 hour'
) o
LEFT JOIN LATERAL (
    -- keep hostname and MAC separately; empty hostname becomes NULL
    SELECT nullif(l.hostname, '') AS hostname, l.mac AS mac
    FROM dhcp_leases l
    WHERE l.ip = o.device_ip
    ORDER BY
        -- prefer the lease that actually covered this hour...
        (l.valid_from <= %(hour)s
         AND (l.valid_to IS NULL OR l.valid_to > %(hour)s)) DESC,
        -- ...otherwise fall back to the lease nearest in time (handles the
        -- bootstrap window where lease history starts after the flow hour)
        abs(extract(epoch FROM (l.valid_from - %(hour)s)))
    LIMIT 1
) l ON true
LEFT JOIN arp a ON a.ip = o.device_ip
LEFT JOIN device_alias al ON upper(al.mac) = upper(coalesce(l.mac, a.mac))
LEFT JOIN ip_domain d ON d.ip = o.remote_ip
GROUP BY 1, o.device_ip, al.name, l.hostname, l.mac, a.mac, o.remote_ip, d.domain,
         o.remote_port, o.protocol
ON CONFLICT (hour, device_ip, remote_ip, remote_port, protocol) DO UPDATE
SET device_name   = EXCLUDED.device_name,
    mac           = EXCLUDED.mac,
    remote_domain = EXCLUDED.remote_domain,
    bytes         = EXCLUDED.bytes,
    packets       = EXCLUDED.packets,
    flow_count    = EXCLUDED.flow_count
"""


def aggregate(pool, now=None):
    now = now or datetime.now(timezone.utc)
    boundary = now.replace(minute=0, second=0, microsecond=0)
    with pool.connection() as conn:
        row = conn.execute(
            "SELECT last_hour FROM agg_state WHERE name = 'hourly'"
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
            conn.execute(_AGG_SQL, {"hour": hour})
            hour = hour + timedelta(hours=1)
            conn.execute(
                "INSERT INTO agg_state (name, last_hour) VALUES ('hourly', %s) "
                "ON CONFLICT (name) DO UPDATE SET last_hour = EXCLUDED.last_hour",
                (hour,),
            )
