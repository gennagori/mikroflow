# mikroflow

Collects NetFlow v9 from a MikroTik router into PostgreSQL, enriched with device
names (DHCP) and destination domains (reverse-DNS). Raw flows are kept ~14 days,
processed flows (enriched, per-flow, nothing aggregated) 180 days.

## MikroTik setup

```
/ip traffic-flow set enabled=yes interfaces=all
/ip traffic-flow target add address=<SERVER_IP>:2055 version=9
/ip service enable api
```

Create a read-only API user for lease sync and set its credentials in `.env`.

## Run

```bash
cp .env.example .env   # edit router host/credentials
docker compose up -d --build
```

## Analysis

Query the `v_connections` view:

Each row is one flow exactly as received (nothing folded, grouped, or
summed — every flow keeps its own `ts` down to the second):
`device_name`/`mac`/`device_ip` is the local host, `remote_domain`/
`remote_ip`/`remote_port` is who it talked to.

```sql
SELECT ts, device_name, mac, device_ip, remote_domain, remote_ip,
       remote_port, bytes, packets
FROM v_connections
WHERE ts >= now() - interval '7 days'
ORDER BY bytes DESC
LIMIT 100;
```

If you want hourly (or any other interval) totals, aggregate on the fly:

```sql
SELECT date_trunc('hour', ts) AS hour, device_name, remote_domain,
       sum(bytes) AS bytes, count(*) AS flow_count
FROM v_connections
WHERE ts >= now() - interval '7 days'
GROUP BY 1, 2, 3
ORDER BY bytes DESC
LIMIT 100;
```

## Tests

```bash
pip install -e ".[dev]"
pytest            # integration tests require Docker (testcontainers)
```
