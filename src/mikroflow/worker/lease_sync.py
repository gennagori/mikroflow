from datetime import datetime, timezone

import routeros_api


def fetch_leases(host, user, password, port):
    conn = routeros_api.RouterOsApiPool(
        host, username=user, password=password, port=port, plaintext_login=True
    )
    try:
        api = conn.get_api()
        rows = api.get_resource("/ip/dhcp-server/lease").get()
        return [dict(r) for r in rows]
    finally:
        conn.disconnect()


def normalize_leases(rows):
    out = []
    for r in rows:
        ip = r.get("active-address") or r.get("address")
        if not ip:
            continue
        mac = r.get("active-mac-address") or r.get("mac-address")
        hostname = r.get("host-name") or r.get("comment")
        out.append((ip, mac, hostname))
    return out


def sync_leases(pool, leases, now=None):
    now = now or datetime.now(timezone.utc)
    current_ips = {ip for ip, _, _ in leases}
    with pool.connection() as conn:
        existing = {
            row[0]: (row[1], row[2])
            for row in conn.execute(
                "SELECT host(ip), mac, hostname FROM dhcp_leases WHERE valid_to IS NULL"
            ).fetchall()
        }
        for ip, mac, hostname in leases:
            prev = existing.get(ip)
            if prev is None:
                conn.execute(
                    "INSERT INTO dhcp_leases (ip, mac, hostname, valid_from) "
                    "VALUES (%s, %s, %s, %s)",
                    (ip, mac, hostname, now),
                )
            elif prev != (mac, hostname):
                conn.execute(
                    "UPDATE dhcp_leases SET valid_to=%s WHERE ip=%s AND valid_to IS NULL",
                    (now, ip),
                )
                conn.execute(
                    "INSERT INTO dhcp_leases (ip, mac, hostname, valid_from) "
                    "VALUES (%s, %s, %s, %s)",
                    (ip, mac, hostname, now),
                )
        for ip in set(existing) - current_ips:
            conn.execute(
                "UPDATE dhcp_leases SET valid_to=%s WHERE ip=%s AND valid_to IS NULL",
                (now, ip),
            )
