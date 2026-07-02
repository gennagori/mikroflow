# mikroflow

Collects NetFlow v9 from a MikroTik router into PostgreSQL, enriched with device
names (DHCP) and destination domains (reverse-DNS). Raw flows are kept ~14 days,
hourly aggregates 6 months.

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

```sql
SELECT hour, device_name, src_ip, dst_domain, dst_ip, dst_port,
       bytes, flow_count
FROM v_connections
WHERE hour >= now() - interval '7 days'
ORDER BY bytes DESC
LIMIT 100;
```

## Tests

```bash
pip install -e ".[dev]"
pytest            # integration tests require Docker (testcontainers)
```
