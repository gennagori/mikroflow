# MikroTik NetFlow Collector Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Collect NetFlow v9 from a MikroTik router, enrich flows with device names (DHCP) and destination domains (reverse-DNS), and store them in PostgreSQL — raw flows ~14 days, hourly aggregates 6 months.

**Architecture:** Two Python services + Postgres, run via Docker Compose. A `collector` receives NetFlow over UDP into a bounded in-process queue and batch-`COPY`s raw flows to Postgres. A `worker` runs scheduled jobs: sync DHCP leases via RouterOS API, resolve destination IPs to domains via reverse-DNS (cached), roll raw flows up into hourly aggregates (denormalizing device name + domain), and manage partitions/retention.

**Tech Stack:** Python 3.11, psycopg 3 (+ psycopg_pool), pydantic-settings, RouterOS-api, dnspython, APScheduler, PostgreSQL 16 (declarative range partitioning), pytest + testcontainers.

## Global Constraints

- Python 3.11+ only.
- PostgreSQL 16 (declarative partitioning by RANGE).
- All timestamps are timezone-aware UTC (`timestamptz`).
- NetFlow **v9 only** (MikroTik Traffic Flow is configured with `version=9`). IPFIX/v5 are out of scope for v1.
- Config is loaded from environment with prefix `MIKROFLOW_` (and optional `.env`).
- Dependencies (pin majors): `psycopg[binary]>=3.1`, `psycopg-pool>=3.2`, `pydantic-settings>=2.2`, `RouterOS-api>=0.18`, `dnspython>=2.6`, `APScheduler>=3.10`; dev: `pytest>=8`, `testcontainers[postgres]>=4`.
- Package name: `mikroflow`. Source under `src/mikroflow/`. Tests under `tests/`.
- No external message broker; the flow write path goes through a `FlowSink` interface so a broker can be added later without touching the collector.

---

### Task 1: Project scaffolding and configuration

**Files:**
- Create: `pyproject.toml`
- Create: `src/mikroflow/__init__.py` (empty)
- Create: `src/mikroflow/config.py`
- Create: `.env.example`
- Test: `tests/unit/test_config.py`

**Interfaces:**
- Produces: `mikroflow.config.Settings` (pydantic-settings model) and `get_settings() -> Settings`. Fields listed in the implementation below with exact names/types; every later task reads config through this object.

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_config.py
import os
from mikroflow.config import get_settings

def test_settings_read_from_env(monkeypatch):
    monkeypatch.setenv("MIKROFLOW_DB_DSN", "postgresql://u:p@h:5432/db")
    monkeypatch.setenv("MIKROFLOW_ROUTER_HOST", "10.1.1.1")
    monkeypatch.setenv("MIKROFLOW_RAW_RETENTION_DAYS", "7")
    s = get_settings()
    assert s.db_dsn == "postgresql://u:p@h:5432/db"
    assert s.router_host == "10.1.1.1"
    assert s.raw_retention_days == 7
    assert s.netflow_port == 2055  # default
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_config.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'mikroflow'`.

- [ ] **Step 3: Create `pyproject.toml`**

```toml
[project]
name = "mikroflow"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = [
    "psycopg[binary]>=3.1",
    "psycopg-pool>=3.2",
    "pydantic-settings>=2.2",
    "RouterOS-api>=0.18",
    "dnspython>=2.6",
    "APScheduler>=3.10",
]

[project.optional-dependencies]
dev = ["pytest>=8", "testcontainers[postgres]>=4"]

[build-system]
requires = ["setuptools>=68"]
build-backend = "setuptools.build_meta"

[tool.setuptools.packages.find]
where = ["src"]

[tool.pytest.ini_options]
pythonpath = ["src"]
testpaths = ["tests"]
```

- [ ] **Step 4: Create `src/mikroflow/__init__.py`** (empty file)

- [ ] **Step 5: Create `src/mikroflow/config.py`**

```python
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_prefix="MIKROFLOW_", extra="ignore"
    )

    # Database
    db_dsn: str = "postgresql://mikroflow:mikroflow@postgres:5432/mikroflow"

    # NetFlow receiver
    netflow_host: str = "0.0.0.0"
    netflow_port: int = 2055
    recv_buffer_bytes: int = 16 * 1024 * 1024
    queue_maxsize: int = 100_000
    batch_size: int = 1000
    batch_flush_seconds: float = 2.0

    # RouterOS API (DHCP leases)
    router_host: str = "192.168.88.1"
    router_user: str = "api"
    router_password: str = ""
    router_port: int = 8728
    lease_sync_seconds: int = 300

    # reverse-DNS
    rdns_batch_size: int = 200
    rdns_timeout_seconds: float = 2.0
    rdns_positive_ttl_seconds: int = 86_400
    rdns_negative_ttl_seconds: int = 3_600
    rdns_poll_seconds: int = 60

    # aggregation and retention
    aggregate_seconds: int = 300
    raw_retention_days: int = 14
    hourly_retention_days: int = 180
    partition_days_ahead: int = 3
    partition_months_ahead: int = 2


def get_settings() -> Settings:
    return Settings()
```

- [ ] **Step 6: Create `.env.example`**

```dotenv
MIKROFLOW_DB_DSN=postgresql://mikroflow:mikroflow@postgres:5432/mikroflow
MIKROFLOW_ROUTER_HOST=192.168.88.1
MIKROFLOW_ROUTER_USER=api
MIKROFLOW_ROUTER_PASSWORD=change-me
MIKROFLOW_ROUTER_PORT=8728
MIKROFLOW_NETFLOW_PORT=2055
MIKROFLOW_RAW_RETENTION_DAYS=14
MIKROFLOW_HOURLY_RETENTION_DAYS=180
```

- [ ] **Step 7: Run test to verify it passes**

Run: `pip install -e ".[dev]" && pytest tests/unit/test_config.py -v`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add pyproject.toml src/mikroflow/__init__.py src/mikroflow/config.py .env.example tests/unit/test_config.py
git commit -m "feat: project scaffolding and configuration"
```

---

### Task 2: NetFlow v9 parser and Flow model

**Files:**
- Create: `src/mikroflow/models.py`
- Create: `src/mikroflow/collector/__init__.py` (empty)
- Create: `src/mikroflow/collector/parser.py`
- Test: `tests/unit/test_parser.py`

**Interfaces:**
- Produces:
  - `mikroflow.models.Flow` — dataclass with fields `ts: datetime, src_ip: str|None, dst_ip: str|None, src_port: int, dst_port: int, protocol: int, bytes: int, packets: int, exporter_ip: str`.
  - `mikroflow.collector.parser.TemplateStore` — holds v9 templates per `(exporter_ip, source_id, template_id)`.
  - `parse_v9(datagram: bytes, exporter_ip: str, store: TemplateStore, now: datetime|None = None) -> list[Flow]`.

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_parser.py
import struct
from mikroflow.collector.parser import parse_v9, TemplateStore

# NetFlow v9 field types: SRC_IP=8, DST_IP=12, L4_SRC_PORT=7, L4_DST_PORT=11,
# PROTOCOL=4, IN_BYTES=1, IN_PKTS=2
FIELDS = [(8, 4), (12, 4), (7, 2), (11, 2), (4, 1), (1, 4), (2, 4)]


def build_v9_template(template_id, fields, source_id=1, seq=1):
    body = struct.pack("!HH", template_id, len(fields))
    for ftype, flen in fields:
        body += struct.pack("!HH", ftype, flen)
    flowset = struct.pack("!HH", 0, 4 + len(body)) + body
    header = struct.pack("!HHIIII", 9, 1, 0, 0, seq, source_id)
    return header + flowset


def build_v9_data(template_id, records, source_id=1, seq=2):
    body = b"".join(records)
    flowset = struct.pack("!HH", template_id, 4 + len(body)) + body
    header = struct.pack("!HHIIII", 9, len(records), 0, 0, seq, source_id)
    return header + flowset


def make_record():
    return (
        bytes([192, 168, 1, 10]) + bytes([8, 8, 8, 8])
        + struct.pack("!H", 54321) + struct.pack("!H", 443)
        + bytes([6]) + struct.pack("!I", 1500) + struct.pack("!I", 12)
    )


def test_parse_data_after_template():
    store = TemplateStore()
    parse_v9(build_v9_template(256, FIELDS), "10.0.0.1", store)
    flows = parse_v9(build_v9_data(256, [make_record(), make_record()]), "10.0.0.1", store)
    assert len(flows) == 2
    f = flows[0]
    assert f.src_ip == "192.168.1.10"
    assert f.dst_ip == "8.8.8.8"
    assert f.src_port == 54321
    assert f.dst_port == 443
    assert f.protocol == 6
    assert f.bytes == 1500
    assert f.packets == 12
    assert f.exporter_ip == "10.0.0.1"


def test_data_without_template_is_skipped():
    store = TemplateStore()
    flows = parse_v9(build_v9_data(256, [make_record()]), "10.0.0.1", store)
    assert flows == []
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_parser.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'mikroflow.collector'`.

- [ ] **Step 3: Create `src/mikroflow/models.py`**

```python
from dataclasses import dataclass
from datetime import datetime


@dataclass
class Flow:
    ts: datetime
    src_ip: str | None
    dst_ip: str | None
    src_port: int
    dst_port: int
    protocol: int
    bytes: int
    packets: int
    exporter_ip: str
```

- [ ] **Step 4: Create `src/mikroflow/collector/__init__.py`** (empty file)

- [ ] **Step 5: Create `src/mikroflow/collector/parser.py`**

```python
import struct
from datetime import datetime, timezone
from ipaddress import IPv4Address

from mikroflow.models import Flow

FIELD_IN_BYTES = 1
FIELD_IN_PKTS = 2
FIELD_PROTOCOL = 4
FIELD_L4_SRC_PORT = 7
FIELD_IPV4_SRC_ADDR = 8
FIELD_L4_DST_PORT = 11
FIELD_IPV4_DST_ADDR = 12


class TemplateStore:
    def __init__(self) -> None:
        self._templates: dict[tuple[str, int, int], list[tuple[int, int]]] = {}

    def set(self, exporter_ip, source_id, template_id, fields):
        self._templates[(exporter_ip, source_id, template_id)] = fields

    def get(self, exporter_ip, source_id, template_id):
        return self._templates.get((exporter_ip, source_id, template_id))


def _int(data: bytes) -> int:
    return int.from_bytes(data, "big") if data else 0


def parse_v9(datagram, exporter_ip, store, now=None):
    if now is None:
        now = datetime.now(timezone.utc)
    flows: list[Flow] = []
    n = len(datagram)
    if n < 20:
        return flows
    version, count, uptime, secs, seq, source_id = struct.unpack("!HHIIII", datagram[:20])
    if version != 9:
        return flows
    offset = 20
    while offset + 4 <= n:
        flowset_id, length = struct.unpack("!HH", datagram[offset:offset + 4])
        if length < 4 or offset + length > n:
            break
        body = datagram[offset + 4:offset + length]
        if flowset_id == 0:
            _parse_templates(body, exporter_ip, source_id, store)
        elif flowset_id > 255:
            flows.extend(_parse_data(body, exporter_ip, source_id, flowset_id, store, now))
        offset += length
    return flows


def _parse_templates(body, exporter_ip, source_id, store):
    o = 0
    while o + 4 <= len(body):
        template_id, field_count = struct.unpack("!HH", body[o:o + 4])
        o += 4
        fields = []
        for _ in range(field_count):
            if o + 4 > len(body):
                return
            ftype, flen = struct.unpack("!HH", body[o:o + 4])
            fields.append((ftype, flen))
            o += 4
        store.set(exporter_ip, source_id, template_id, fields)


def _parse_data(body, exporter_ip, source_id, template_id, store, now):
    fields = store.get(exporter_ip, source_id, template_id)
    if not fields:
        return []
    rec_len = sum(flen for _, flen in fields)
    if rec_len == 0:
        return []
    flows = []
    o = 0
    while o + rec_len <= len(body):
        values: dict[int, bytes] = {}
        p = o
        for ftype, flen in fields:
            values[ftype] = body[p:p + flen]
            p += flen
        o += rec_len
        flows.append(_to_flow(values, exporter_ip, now))
    return flows


def _to_flow(values, exporter_ip, now):
    def ip(t):
        b = values.get(t)
        return str(IPv4Address(b)) if b and len(b) == 4 else None

    return Flow(
        ts=now,
        src_ip=ip(FIELD_IPV4_SRC_ADDR),
        dst_ip=ip(FIELD_IPV4_DST_ADDR),
        src_port=_int(values.get(FIELD_L4_SRC_PORT, b"")),
        dst_port=_int(values.get(FIELD_L4_DST_PORT, b"")),
        protocol=_int(values.get(FIELD_PROTOCOL, b"")),
        bytes=_int(values.get(FIELD_IN_BYTES, b"")),
        packets=_int(values.get(FIELD_IN_PKTS, b"")),
        exporter_ip=exporter_ip,
    )
```

- [ ] **Step 6: Run test to verify it passes**

Run: `pytest tests/unit/test_parser.py -v`
Expected: PASS (2 passed).

- [ ] **Step 7: Commit**

```bash
git add src/mikroflow/models.py src/mikroflow/collector/__init__.py src/mikroflow/collector/parser.py tests/unit/test_parser.py
git commit -m "feat: NetFlow v9 parser and Flow model"
```

---

### Task 3: Database schema and partition management

**Files:**
- Create: `db/schema.sql`
- Create: `src/mikroflow/db.py`
- Test: `tests/integration/conftest.py`
- Test: `tests/integration/test_schema.py`

**Interfaces:**
- Consumes: `mikroflow.config` (not directly; DSN passed in).
- Produces:
  - `mikroflow.db.make_pool(dsn: str) -> ConnectionPool`
  - `mikroflow.db.apply_schema(pool, schema_path: Path = SCHEMA_PATH) -> None`
  - `mikroflow.db.ensure_partitions(pool, days_ahead: int, months_ahead: int) -> None`
  - SQL objects: tables `flows_raw`, `dhcp_leases`, `ip_domain`, `flows_hourly`, `agg_state`; view `v_connections`; functions `ensure_partition_window(int,int)`, `drop_old_partitions(int,int)`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/integration/conftest.py
import pytest
from testcontainers.postgres import PostgresContainer
from mikroflow.db import make_pool, apply_schema, ensure_partitions


@pytest.fixture(scope="session")
def pg_container():
    with PostgresContainer("postgres:16-alpine") as container:
        yield container


@pytest.fixture
def pool(pg_container):
    dsn = pg_container.get_connection_url().replace("+psycopg2", "")
    p = make_pool(dsn)
    apply_schema(p)
    ensure_partitions(p, 3, 2)
    with p.connection() as conn:
        conn.execute(
            "TRUNCATE flows_raw, dhcp_leases, ip_domain, flows_hourly, agg_state"
        )
    yield p
    p.close()
```

```python
# tests/integration/test_schema.py
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/integration/test_schema.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'mikroflow.db'` (requires Docker running for testcontainers).

- [ ] **Step 3: Create `db/schema.sql`**

```sql
-- Raw flows: partitioned by day, short retention.
CREATE TABLE IF NOT EXISTS flows_raw (
    ts          timestamptz NOT NULL,
    src_ip      inet        NOT NULL,
    dst_ip      inet        NOT NULL,
    src_port    integer     NOT NULL,
    dst_port    integer     NOT NULL,
    protocol    smallint    NOT NULL,
    bytes       bigint      NOT NULL,
    packets     bigint      NOT NULL,
    exporter_ip inet        NOT NULL
) PARTITION BY RANGE (ts);

CREATE INDEX IF NOT EXISTS flows_raw_ts_idx ON flows_raw (ts);
CREATE INDEX IF NOT EXISTS flows_raw_dst_idx ON flows_raw (dst_ip);

-- DHCP leases with history (valid_to IS NULL == currently active).
CREATE TABLE IF NOT EXISTS dhcp_leases (
    ip         inet        NOT NULL,
    mac        text,
    hostname   text,
    valid_from timestamptz NOT NULL,
    valid_to   timestamptz
);
CREATE INDEX IF NOT EXISTS dhcp_leases_ip_idx ON dhcp_leases (ip, valid_from);
CREATE UNIQUE INDEX IF NOT EXISTS dhcp_leases_open_uidx
    ON dhcp_leases (ip) WHERE valid_to IS NULL;

-- Reverse-DNS cache.
CREATE TABLE IF NOT EXISTS ip_domain (
    ip          inet PRIMARY KEY,
    domain      text,
    status      text        NOT NULL,   -- 'ok' | 'nxdomain'
    resolved_at timestamptz NOT NULL,
    ttl         integer     NOT NULL
);

-- Hourly aggregates: partitioned by month, 6-month retention. Main analysis table.
CREATE TABLE IF NOT EXISTS flows_hourly (
    hour        timestamptz NOT NULL,
    src_ip      inet        NOT NULL,
    device_name text,
    dst_ip      inet        NOT NULL,
    dst_domain  text,
    dst_port    integer     NOT NULL,
    protocol    smallint    NOT NULL,
    bytes       bigint      NOT NULL,
    packets     bigint      NOT NULL,
    flow_count  bigint      NOT NULL,
    PRIMARY KEY (hour, src_ip, dst_ip, dst_port, protocol)
) PARTITION BY RANGE (hour);

-- Aggregation watermark.
CREATE TABLE IF NOT EXISTS agg_state (
    name      text PRIMARY KEY,
    last_hour timestamptz
);

CREATE OR REPLACE VIEW v_connections AS
SELECT hour, src_ip, device_name, dst_ip, dst_domain, dst_port, protocol,
       bytes, packets, flow_count
FROM flows_hourly;

CREATE OR REPLACE FUNCTION ensure_daily_partition(day date) RETURNS void AS $$
DECLARE part text := 'flows_raw_' || to_char(day, 'YYYYMMDD');
BEGIN
    IF to_regclass(part) IS NULL THEN
        EXECUTE format(
            'CREATE TABLE %I PARTITION OF flows_raw FOR VALUES FROM (%L) TO (%L)',
            part, day::timestamptz, (day + 1)::timestamptz);
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION ensure_monthly_partition(mon date) RETURNS void AS $$
DECLARE
    first date := date_trunc('month', mon)::date;
    part  text := 'flows_hourly_' || to_char(first, 'YYYYMM');
BEGIN
    IF to_regclass(part) IS NULL THEN
        EXECUTE format(
            'CREATE TABLE %I PARTITION OF flows_hourly FOR VALUES FROM (%L) TO (%L)',
            part, first::timestamptz, (first + interval '1 month')::timestamptz);
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION ensure_partition_window(days_ahead int, months_ahead int)
RETURNS void AS $$
DECLARE d int; m int;
BEGIN
    FOR d IN -1 .. days_ahead LOOP
        PERFORM ensure_daily_partition((current_date + d));
    END LOOP;
    FOR m IN -1 .. months_ahead LOOP
        PERFORM ensure_monthly_partition(
            (date_trunc('month', current_date) + (m || ' month')::interval)::date);
    END LOOP;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION drop_old_partitions(raw_days int, hourly_days int)
RETURNS void AS $$
DECLARE
    r record;
    cutoff_raw    date := current_date - raw_days;
    cutoff_hourly date := date_trunc('month', current_date - hourly_days)::date;
BEGIN
    FOR r IN SELECT relname FROM pg_class WHERE relname LIKE 'flows_raw_2%' LOOP
        IF to_date(right(r.relname, 8), 'YYYYMMDD') < cutoff_raw THEN
            EXECUTE format('DROP TABLE IF EXISTS %I', r.relname);
        END IF;
    END LOOP;
    FOR r IN SELECT relname FROM pg_class WHERE relname LIKE 'flows_hourly_2%' LOOP
        IF to_date(right(r.relname, 6) || '01', 'YYYYMMDD') < cutoff_hourly THEN
            EXECUTE format('DROP TABLE IF EXISTS %I', r.relname);
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;
```

- [ ] **Step 4: Create `src/mikroflow/db.py`**

```python
from pathlib import Path

from psycopg_pool import ConnectionPool

SCHEMA_PATH = Path(__file__).resolve().parent.parent.parent / "db" / "schema.sql"


def make_pool(dsn: str) -> ConnectionPool:
    return ConnectionPool(dsn, min_size=1, max_size=10, open=True)


def apply_schema(pool: ConnectionPool, schema_path: Path = SCHEMA_PATH) -> None:
    # No parameters -> psycopg uses the simple query protocol and runs the
    # whole multi-statement script in one call.
    sql = schema_path.read_text()
    with pool.connection() as conn:
        conn.execute(sql)


def ensure_partitions(pool: ConnectionPool, days_ahead: int, months_ahead: int) -> None:
    with pool.connection() as conn:
        conn.execute("SELECT ensure_partition_window(%s, %s)", (days_ahead, months_ahead))
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `pytest tests/integration/test_schema.py -v`
Expected: PASS (2 passed). (Docker must be available.)

- [ ] **Step 6: Commit**

```bash
git add db/schema.sql src/mikroflow/db.py tests/integration/conftest.py tests/integration/test_schema.py
git commit -m "feat: database schema and partition management"
```

---

### Task 4: FlowSink interface and Postgres sink

**Files:**
- Create: `src/mikroflow/sinks.py`
- Test: `tests/integration/test_sink.py`

**Interfaces:**
- Consumes: `mikroflow.models.Flow`, a `ConnectionPool` from `mikroflow.db`.
- Produces:
  - `mikroflow.sinks.FlowSink` — ABC with `write_batch(self, flows: Sequence[Flow]) -> None`.
  - `mikroflow.sinks.PostgresFlowSink(pool)` — implements `FlowSink` via `COPY` into `flows_raw`.

- [ ] **Step 1: Write the failing test**

```python
# tests/integration/test_sink.py
from datetime import datetime, timezone
from mikroflow.models import Flow
from mikroflow.sinks import PostgresFlowSink


def test_write_batch_copies_rows(pool):
    now = datetime(2026, 7, 1, 12, 30, tzinfo=timezone.utc)
    flows = [
        Flow(now, "192.168.1.10", "8.8.8.8", 5000, 443, 6, 1500, 12, "10.0.0.1"),
        Flow(now, "192.168.1.11", "1.1.1.1", 5001, 53, 17, 90, 1, "10.0.0.1"),
    ]
    PostgresFlowSink(pool).write_batch(flows)
    with pool.connection() as conn:
        count = conn.execute("SELECT count(*) FROM flows_raw").fetchone()[0]
        one = conn.execute(
            "SELECT host(src_ip), host(dst_ip), dst_port, bytes FROM flows_raw "
            "WHERE dst_port = 443"
        ).fetchone()
    assert count == 2
    assert one == ("192.168.1.10", "8.8.8.8", 443, 1500)


def test_write_batch_empty_is_noop(pool):
    PostgresFlowSink(pool).write_batch([])
    with pool.connection() as conn:
        assert conn.execute("SELECT count(*) FROM flows_raw").fetchone()[0] == 0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/integration/test_sink.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'mikroflow.sinks'`.

- [ ] **Step 3: Create `src/mikroflow/sinks.py`**

```python
from abc import ABC, abstractmethod
from collections.abc import Sequence

from psycopg_pool import ConnectionPool

from mikroflow.models import Flow

_COPY_SQL = (
    "COPY flows_raw (ts, src_ip, dst_ip, src_port, dst_port, protocol, "
    "bytes, packets, exporter_ip) FROM STDIN"
)


class FlowSink(ABC):
    @abstractmethod
    def write_batch(self, flows: Sequence[Flow]) -> None: ...


class PostgresFlowSink(FlowSink):
    def __init__(self, pool: ConnectionPool) -> None:
        self._pool = pool

    def write_batch(self, flows: Sequence[Flow]) -> None:
        if not flows:
            return
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                with cur.copy(_COPY_SQL) as copy:
                    for f in flows:
                        copy.write_row(
                            (
                                f.ts, f.src_ip, f.dst_ip, f.src_port, f.dst_port,
                                f.protocol, f.bytes, f.packets, f.exporter_ip,
                            )
                        )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/integration/test_sink.py -v`
Expected: PASS (2 passed).

- [ ] **Step 5: Commit**

```bash
git add src/mikroflow/sinks.py tests/integration/test_sink.py
git commit -m "feat: FlowSink interface and Postgres COPY sink"
```

---

### Task 5: Collector receiver, batch writer, and entrypoint

**Files:**
- Create: `src/mikroflow/collector/receiver.py`
- Create: `src/mikroflow/collector/writer.py`
- Create: `src/mikroflow/collector/main.py`
- Test: `tests/unit/test_writer.py`
- Test: `tests/integration/test_collector_e2e.py`

**Interfaces:**
- Consumes: `parse_v9`, `TemplateStore` (Task 2); `FlowSink` (Task 4); `Settings`, `make_pool`, `apply_schema`, `ensure_partitions` (Tasks 1/3).
- Produces:
  - `UdpReceiver(host, port, recv_buffer_bytes, out_queue, store=None)` — `threading.Thread`; `.start()`, `.stop()`.
  - `BatchWriter(in_queue, sink, batch_size, flush_seconds)` — `threading.Thread`; `.start()`, `.stop()`.
  - `mikroflow.collector.main.main()` — process entrypoint.

- [ ] **Step 1: Write the failing unit test for BatchWriter**

```python
# tests/unit/test_writer.py
import queue
from datetime import datetime, timezone
from mikroflow.models import Flow
from mikroflow.sinks import FlowSink
from mikroflow.collector.writer import BatchWriter


class MemorySink(FlowSink):
    def __init__(self):
        self.batches = []

    def write_batch(self, flows):
        if flows:
            self.batches.append(list(flows))


def _flow():
    return Flow(datetime.now(timezone.utc), "10.0.0.2", "8.8.8.8", 1, 2, 6, 10, 1, "10.0.0.1")


def test_writer_flushes_by_batch_size():
    q = queue.Queue()
    sink = MemorySink()
    for _ in range(5):
        q.put(_flow())
    w = BatchWriter(q, sink, batch_size=5, flush_seconds=30.0)
    w.start()
    w.stop()
    w.join(timeout=5)
    assert sum(len(b) for b in sink.batches) == 5
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_writer.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'mikroflow.collector.writer'`.

- [ ] **Step 3: Create `src/mikroflow/collector/writer.py`**

```python
import queue
import threading
import time

from mikroflow.sinks import FlowSink


class BatchWriter(threading.Thread):
    def __init__(self, in_queue, sink: FlowSink, batch_size: int, flush_seconds: float):
        super().__init__(daemon=True)
        self._queue = in_queue
        self._sink = sink
        self._batch_size = batch_size
        self._flush_seconds = flush_seconds
        self._stop = threading.Event()

    def run(self) -> None:
        buf = []
        last = time.monotonic()
        while not self._stop.is_set() or not self._queue.empty():
            timeout = max(0.0, self._flush_seconds - (time.monotonic() - last))
            try:
                buf.append(self._queue.get(timeout=timeout))
            except queue.Empty:
                pass
            due = len(buf) >= self._batch_size or (time.monotonic() - last) >= self._flush_seconds
            if buf and due:
                self._sink.write_batch(buf)
                buf = []
                last = time.monotonic()
        if buf:
            self._sink.write_batch(buf)

    def stop(self) -> None:
        self._stop.set()
```

- [ ] **Step 4: Run unit test to verify it passes**

Run: `pytest tests/unit/test_writer.py -v`
Expected: PASS.

- [ ] **Step 5: Create `src/mikroflow/collector/receiver.py`**

```python
import queue
import socket
import threading

from mikroflow.collector.parser import TemplateStore, parse_v9


class UdpReceiver(threading.Thread):
    def __init__(self, host, port, recv_buffer_bytes, out_queue, store=None):
        super().__init__(daemon=True)
        self._host = host
        self._port = port
        self._recv_buffer_bytes = recv_buffer_bytes
        self._queue = out_queue
        self._store = store or TemplateStore()
        self._stop = threading.Event()

    def _make_socket(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, self._recv_buffer_bytes)
        except OSError:
            pass
        s.bind((self._host, self._port))
        s.settimeout(1.0)
        return s

    def run(self) -> None:
        sock = self._make_socket()
        try:
            while not self._stop.is_set():
                try:
                    data, addr = sock.recvfrom(65535)
                except socket.timeout:
                    continue
                for flow in parse_v9(data, addr[0], self._store):
                    try:
                        self._queue.put_nowait(flow)
                    except queue.Full:
                        pass  # bounded queue: drop under sustained overload
        finally:
            sock.close()

    def stop(self) -> None:
        self._stop.set()
```

- [ ] **Step 6: Create `src/mikroflow/collector/main.py`**

```python
import queue
import signal

from mikroflow.config import get_settings
from mikroflow.db import apply_schema, ensure_partitions, make_pool
from mikroflow.sinks import PostgresFlowSink
from mikroflow.collector.receiver import UdpReceiver
from mikroflow.collector.writer import BatchWriter


def main() -> None:
    s = get_settings()
    pool = make_pool(s.db_dsn)
    apply_schema(pool)
    ensure_partitions(pool, s.partition_days_ahead, s.partition_months_ahead)

    q: queue.Queue = queue.Queue(maxsize=s.queue_maxsize)
    sink = PostgresFlowSink(pool)
    writer = BatchWriter(q, sink, s.batch_size, s.batch_flush_seconds)
    receiver = UdpReceiver(s.netflow_host, s.netflow_port, s.recv_buffer_bytes, q)

    writer.start()
    receiver.start()
    signal.sigwait([signal.SIGINT, signal.SIGTERM])

    receiver.stop()
    writer.stop()
    receiver.join(timeout=10)
    writer.join(timeout=10)
    pool.close()


if __name__ == "__main__":
    main()
```

- [ ] **Step 7: Write the integration end-to-end test**

```python
# tests/integration/test_collector_e2e.py
import queue
import socket
import struct
import time
from mikroflow.sinks import PostgresFlowSink
from mikroflow.collector.receiver import UdpReceiver
from mikroflow.collector.writer import BatchWriter

FIELDS = [(8, 4), (12, 4), (7, 2), (11, 2), (4, 1), (1, 4), (2, 4)]


def _template(tid=256, source_id=1):
    body = struct.pack("!HH", tid, len(FIELDS))
    for ft, fl in FIELDS:
        body += struct.pack("!HH", ft, fl)
    fs = struct.pack("!HH", 0, 4 + len(body)) + body
    return struct.pack("!HHIIII", 9, 1, 0, 0, 1, source_id) + fs


def _data(tid=256, source_id=1):
    rec = (bytes([192, 168, 1, 10]) + bytes([8, 8, 8, 8])
           + struct.pack("!H", 5000) + struct.pack("!H", 443)
           + bytes([6]) + struct.pack("!I", 1500) + struct.pack("!I", 12))
    fs = struct.pack("!HH", tid, 4 + len(rec)) + rec
    return struct.pack("!HHIIII", 9, 1, 0, 0, 2, source_id) + fs


def test_udp_to_postgres(pool):
    q = queue.Queue(maxsize=1000)
    writer = BatchWriter(q, PostgresFlowSink(pool), batch_size=1, flush_seconds=0.2)
    receiver = UdpReceiver("127.0.0.1", 0, 1 << 20, q)
    # bind to an ephemeral port by pre-creating the socket
    sock = receiver._make_socket()
    port = sock.getsockname()[1]
    sock.close()
    receiver = UdpReceiver("127.0.0.1", port, 1 << 20, q)
    writer.start()
    receiver.start()
    try:
        tx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        tx.sendto(_template(), ("127.0.0.1", port))
        time.sleep(0.2)
        tx.sendto(_data(), ("127.0.0.1", port))
        deadline = time.time() + 5
        rows = 0
        while time.time() < deadline:
            with pool.connection() as conn:
                rows = conn.execute("SELECT count(*) FROM flows_raw").fetchone()[0]
            if rows:
                break
            time.sleep(0.1)
        assert rows == 1
    finally:
        receiver.stop()
        writer.stop()
        receiver.join(timeout=5)
        writer.join(timeout=5)
```

- [ ] **Step 8: Run all collector tests to verify they pass**

Run: `pytest tests/unit/test_writer.py tests/integration/test_collector_e2e.py -v`
Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add src/mikroflow/collector/receiver.py src/mikroflow/collector/writer.py src/mikroflow/collector/main.py tests/unit/test_writer.py tests/integration/test_collector_e2e.py
git commit -m "feat: collector receiver, batch writer, and entrypoint"
```

---

### Task 6: DHCP lease sync

**Files:**
- Create: `src/mikroflow/worker/__init__.py` (empty)
- Create: `src/mikroflow/worker/lease_sync.py`
- Test: `tests/unit/test_lease_normalize.py`
- Test: `tests/integration/test_lease_sync.py`

**Interfaces:**
- Consumes: a `ConnectionPool`; `Settings` fields `router_host/user/password/port`.
- Produces:
  - `normalize_leases(rows: list[dict]) -> list[tuple[str, str|None, str|None]]` — `(ip, mac, hostname)`.
  - `fetch_leases(host, user, password, port) -> list[dict]` (RouterOS API).
  - `sync_leases(pool, leases, now=None) -> None` — maintains history: opens new leases, closes changed/removed ones.

- [ ] **Step 1: Write the failing unit test for normalize_leases**

```python
# tests/unit/test_lease_normalize.py
from mikroflow.worker.lease_sync import normalize_leases


def test_normalize_prefers_active_fields_and_hostname():
    rows = [
        {"active-address": "192.168.1.10", "active-mac-address": "AA:BB",
         "host-name": "laptop", "address": "192.168.1.10"},
        {"address": "192.168.1.11", "mac-address": "CC:DD", "comment": "printer"},
        {"mac-address": "EE:FF"},  # no ip -> skipped
    ]
    assert normalize_leases(rows) == [
        ("192.168.1.10", "AA:BB", "laptop"),
        ("192.168.1.11", "CC:DD", "printer"),
    ]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_lease_normalize.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'mikroflow.worker'`.

- [ ] **Step 3: Create `src/mikroflow/worker/__init__.py`** (empty file)

- [ ] **Step 4: Create `src/mikroflow/worker/lease_sync.py`**

```python
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
```

- [ ] **Step 5: Run unit test to verify it passes**

Run: `pytest tests/unit/test_lease_normalize.py -v`
Expected: PASS.

- [ ] **Step 6: Write the integration test for sync_leases**

```python
# tests/integration/test_lease_sync.py
from datetime import datetime, timezone, timedelta
from mikroflow.worker.lease_sync import sync_leases


def _open_leases(pool):
    with pool.connection() as conn:
        return {
            r[0]: r[1]
            for r in conn.execute(
                "SELECT host(ip), hostname FROM dhcp_leases WHERE valid_to IS NULL"
            ).fetchall()
        }


def test_sync_opens_updates_and_closes(pool):
    t0 = datetime(2026, 7, 1, 10, 0, tzinfo=timezone.utc)
    sync_leases(pool, [("192.168.1.10", "AA", "laptop")], now=t0)
    assert _open_leases(pool) == {"192.168.1.10": "laptop"}

    # hostname change -> old row closed, new open row
    t1 = t0 + timedelta(hours=1)
    sync_leases(pool, [("192.168.1.10", "AA", "laptop-renamed")], now=t1)
    assert _open_leases(pool) == {"192.168.1.10": "laptop-renamed"}
    with pool.connection() as conn:
        total = conn.execute("SELECT count(*) FROM dhcp_leases").fetchone()[0]
    assert total == 2

    # lease disappears -> closed, no open rows
    t2 = t1 + timedelta(hours=1)
    sync_leases(pool, [], now=t2)
    assert _open_leases(pool) == {}
```

- [ ] **Step 7: Run integration test to verify it passes**

Run: `pytest tests/integration/test_lease_sync.py -v`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add src/mikroflow/worker/__init__.py src/mikroflow/worker/lease_sync.py tests/unit/test_lease_normalize.py tests/integration/test_lease_sync.py
git commit -m "feat: DHCP lease sync with history"
```

---

### Task 7: Reverse-DNS resolver worker

**Files:**
- Create: `src/mikroflow/worker/rdns.py`
- Test: `tests/integration/test_rdns.py`

**Interfaces:**
- Consumes: a `ConnectionPool`; `Settings` (rdns_* fields).
- Produces:
  - `resolve_ptr(ip: str, timeout: float) -> str | None` (real DNS; raises on transient errors, returns `None` on NXDOMAIN).
  - `pending_ips(pool, batch_size, now, neg_ttl, pos_ttl) -> list[str]`.
  - `upsert_domain(pool, ip, domain, status, now, ttl) -> None`.
  - `resolve_pending(pool, settings, now=None, resolver=resolve_ptr) -> None` (`resolver` is injectable for tests).

- [ ] **Step 1: Write the failing integration test (with injected resolver)**

```python
# tests/integration/test_rdns.py
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/integration/test_rdns.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'mikroflow.worker.rdns'`.

- [ ] **Step 3: Create `src/mikroflow/worker/rdns.py`**

```python
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/integration/test_rdns.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/mikroflow/worker/rdns.py tests/integration/test_rdns.py
git commit -m "feat: reverse-DNS resolver with cache and TTL"
```

---

### Task 8: Hourly aggregator

**Files:**
- Create: `src/mikroflow/worker/aggregator.py`
- Test: `tests/integration/test_aggregator.py`

**Interfaces:**
- Consumes: a `ConnectionPool`; tables `flows_raw`, `dhcp_leases`, `ip_domain`, `flows_hourly`, `agg_state`.
- Produces:
  - `aggregate(pool, now=None) -> None` — rolls up all complete hours since the watermark into `flows_hourly` (idempotent via `ON CONFLICT`), advancing `agg_state('hourly')`.

- [ ] **Step 1: Write the failing integration test**

```python
# tests/integration/test_aggregator.py
from datetime import datetime, timezone, timedelta
from mikroflow.worker.aggregator import aggregate


def _seed(pool):
    hour = datetime(2026, 7, 1, 10, 0, tzinfo=timezone.utc)
    with pool.connection() as conn:
        conn.execute(
            "INSERT INTO dhcp_leases (ip, mac, hostname, valid_from) "
            "VALUES (%s,%s,%s,%s)",
            ("192.168.1.10", "AA", "laptop", hour - timedelta(days=1)),
        )
        conn.execute(
            "INSERT INTO ip_domain (ip, domain, status, resolved_at, ttl) "
            "VALUES (%s,%s,%s,%s,%s)",
            ("8.8.8.8", "dns.google", "ok", hour, 86400),
        )
        for i in range(3):  # 3 flows in same hour, same 5-tuple
            conn.execute(
                "INSERT INTO flows_raw (ts, src_ip, dst_ip, src_port, dst_port, "
                "protocol, bytes, packets, exporter_ip) VALUES "
                "(%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (hour + timedelta(minutes=i * 10), "192.168.1.10", "8.8.8.8",
                 5000, 443, 6, 100, 2, "10.0.0.1"),
            )
    return hour


def test_aggregate_rolls_up_hour_with_enrichment(pool):
    hour = _seed(pool)
    now = hour + timedelta(hours=2)  # hour 10:00 is complete
    aggregate(pool, now=now)
    with pool.connection() as conn:
        row = conn.execute(
            "SELECT device_name, dst_domain, bytes, packets, flow_count "
            "FROM flows_hourly WHERE hour = %s", (hour,)
        ).fetchone()
    assert row == ("laptop", "dns.google", 300, 6, 3)


def test_aggregate_is_idempotent(pool):
    hour = _seed(pool)
    now = hour + timedelta(hours=2)
    aggregate(pool, now=now)
    aggregate(pool, now=now)  # second run must not double count
    with pool.connection() as conn:
        row = conn.execute(
            "SELECT bytes, flow_count FROM flows_hourly WHERE hour = %s", (hour,)
        ).fetchone()
    assert row == (300, 3)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/integration/test_aggregator.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'mikroflow.worker.aggregator'`.

- [ ] **Step 3: Create `src/mikroflow/worker/aggregator.py`**

```python
from datetime import datetime, timedelta, timezone

_AGG_SQL = """
INSERT INTO flows_hourly
    (hour, src_ip, device_name, dst_ip, dst_domain, dst_port, protocol,
     bytes, packets, flow_count)
SELECT
    date_trunc('hour', f.ts) AS hour,
    f.src_ip,
    l.hostname,
    f.dst_ip,
    d.domain,
    f.dst_port,
    f.protocol,
    sum(f.bytes),
    sum(f.packets),
    count(*)
FROM flows_raw f
LEFT JOIN LATERAL (
    SELECT hostname FROM dhcp_leases l
    WHERE l.ip = f.src_ip
      AND l.valid_from <= %(hour)s
      AND (l.valid_to IS NULL OR l.valid_to > %(hour)s)
    ORDER BY l.valid_from DESC
    LIMIT 1
) l ON true
LEFT JOIN ip_domain d ON d.ip = f.dst_ip
WHERE f.ts >= %(hour)s AND f.ts < %(hour)s + interval '1 hour'
GROUP BY 1, f.src_ip, l.hostname, f.dst_ip, d.domain, f.dst_port, f.protocol
ON CONFLICT (hour, src_ip, dst_ip, dst_port, protocol) DO UPDATE
SET device_name = EXCLUDED.device_name,
    dst_domain  = EXCLUDED.dst_domain,
    bytes       = EXCLUDED.bytes,
    packets     = EXCLUDED.packets,
    flow_count  = EXCLUDED.flow_count
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/integration/test_aggregator.py -v`
Expected: PASS (2 passed).

- [ ] **Step 5: Commit**

```bash
git add src/mikroflow/worker/aggregator.py tests/integration/test_aggregator.py
git commit -m "feat: hourly aggregator with denormalized name and domain"
```

---

### Task 9: Retention and partition maintenance

**Files:**
- Create: `src/mikroflow/worker/retention.py`
- Test: `tests/integration/test_retention.py`

**Interfaces:**
- Consumes: a `ConnectionPool`; `Settings` (retention/partition fields).
- Produces:
  - `run_maintenance(pool, settings) -> None` — ensures future partitions exist and drops partitions older than the configured windows.

- [ ] **Step 1: Write the failing integration test**

```python
# tests/integration/test_retention.py
from mikroflow.config import Settings
from mikroflow.worker.retention import run_maintenance


def _partition_names(pool, prefix):
    with pool.connection() as conn:
        return {
            r[0]
            for r in conn.execute(
                "SELECT tablename FROM pg_tables WHERE tablename LIKE %s",
                (prefix + "%",),
            ).fetchall()
        }


def test_maintenance_drops_old_and_keeps_recent(pool):
    # create an old daily partition well outside the retention window
    with pool.connection() as conn:
        conn.execute("SELECT ensure_daily_partition('2000-01-01')")
    assert "flows_raw_20000101" in _partition_names(pool, "flows_raw_")

    run_maintenance(pool, Settings())

    names = _partition_names(pool, "flows_raw_")
    assert "flows_raw_20000101" not in names   # dropped
    assert len(names) >= 1                       # recent window still present
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/integration/test_retention.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'mikroflow.worker.retention'`.

- [ ] **Step 3: Create `src/mikroflow/worker/retention.py`**

```python
def run_maintenance(pool, settings):
    with pool.connection() as conn:
        conn.execute(
            "SELECT ensure_partition_window(%s, %s)",
            (settings.partition_days_ahead, settings.partition_months_ahead),
        )
        conn.execute(
            "SELECT drop_old_partitions(%s, %s)",
            (settings.raw_retention_days, settings.hourly_retention_days),
        )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/integration/test_retention.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/mikroflow/worker/retention.py tests/integration/test_retention.py
git commit -m "feat: retention and partition maintenance"
```

---

### Task 10: Worker entrypoint, Docker packaging, and docs

**Files:**
- Create: `src/mikroflow/worker/main.py`
- Create: `Dockerfile`
- Create: `docker-compose.yml`
- Create: `README.md`
- Test: `tests/unit/test_worker_wiring.py`

**Interfaces:**
- Consumes: everything from Tasks 1–9.
- Produces:
  - `mikroflow.worker.main.build_scheduler(pool, settings) -> BlockingScheduler` (jobs registered, not started — testable).
  - `mikroflow.worker.main.main()` — process entrypoint.

- [ ] **Step 1: Write the failing unit test**

```python
# tests/unit/test_worker_wiring.py
from unittest.mock import MagicMock
from mikroflow.config import Settings
from mikroflow.worker.main import build_scheduler


def test_scheduler_registers_all_jobs():
    pool = MagicMock()
    sched = build_scheduler(pool, Settings())
    names = {job.name for job in sched.get_jobs()}
    assert names == {"lease_sync", "rdns", "aggregate", "maintenance"}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_worker_wiring.py -v`
Expected: FAIL with `ImportError: cannot import name 'build_scheduler'`.

- [ ] **Step 3: Create `src/mikroflow/worker/main.py`**

```python
from apscheduler.schedulers.blocking import BlockingScheduler

from mikroflow.config import get_settings
from mikroflow.db import apply_schema, make_pool
from mikroflow.worker.aggregator import aggregate
from mikroflow.worker.lease_sync import fetch_leases, normalize_leases, sync_leases
from mikroflow.worker.rdns import resolve_pending
from mikroflow.worker.retention import run_maintenance


def _do_lease_sync(pool, s):
    rows = fetch_leases(s.router_host, s.router_user, s.router_password, s.router_port)
    sync_leases(pool, normalize_leases(rows))


def build_scheduler(pool, s):
    sched = BlockingScheduler(timezone="UTC")
    sched.add_job(lambda: _do_lease_sync(pool, s), "interval",
                  seconds=s.lease_sync_seconds, id="lease_sync", name="lease_sync")
    sched.add_job(lambda: resolve_pending(pool, s), "interval",
                  seconds=s.rdns_poll_seconds, id="rdns", name="rdns")
    sched.add_job(lambda: aggregate(pool), "interval",
                  seconds=s.aggregate_seconds, id="aggregate", name="aggregate")
    sched.add_job(lambda: run_maintenance(pool, s), "interval",
                  hours=1, id="maintenance", name="maintenance")
    return sched


def main():
    s = get_settings()
    pool = make_pool(s.db_dsn)
    apply_schema(pool)
    run_maintenance(pool, s)
    build_scheduler(pool, s).start()


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run unit test to verify it passes**

Run: `pytest tests/unit/test_worker_wiring.py -v`
Expected: PASS.

- [ ] **Step 5: Create `Dockerfile`**

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY pyproject.toml ./
COPY src ./src
COPY db ./db
RUN pip install --no-cache-dir .
# entrypoint chosen per-service in docker-compose
CMD ["python", "-m", "mikroflow.collector.main"]
```

- [ ] **Step 6: Create `docker-compose.yml`**

```yaml
services:
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: mikroflow
      POSTGRES_PASSWORD: mikroflow
      POSTGRES_DB: mikroflow
    volumes:
      - pgdata:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U mikroflow"]
      interval: 5s
      timeout: 3s
      retries: 10

  collector:
    build: .
    command: python -m mikroflow.collector.main
    env_file: .env
    environment:
      MIKROFLOW_DB_DSN: postgresql://mikroflow:mikroflow@postgres:5432/mikroflow
    ports:
      - "2055:2055/udp"
    depends_on:
      postgres:
        condition: service_healthy
    restart: unless-stopped

  worker:
    build: .
    command: python -m mikroflow.worker.main
    env_file: .env
    environment:
      MIKROFLOW_DB_DSN: postgresql://mikroflow:mikroflow@postgres:5432/mikroflow
    depends_on:
      postgres:
        condition: service_healthy
    restart: unless-stopped

volumes:
  pgdata:
```

- [ ] **Step 7: Create `README.md`**

````markdown
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
````

- [ ] **Step 8: Run the full test suite**

Run: `pytest -v`
Expected: all tests PASS.

- [ ] **Step 9: Commit**

```bash
git add src/mikroflow/worker/main.py Dockerfile docker-compose.yml README.md tests/unit/test_worker_wiring.py
git commit -m "feat: worker entrypoint, Docker packaging, and docs"
```

---

## Self-Review Notes

- **Spec coverage:** NetFlow ingestion (Tasks 2, 5); DHCP name enrichment (Task 6); reverse-DNS domain (Task 7); hybrid storage: raw + hourly (Tasks 3, 8); partitioning for 100+ devices (Task 3); 6-month retention (Task 9); bounded queue + FlowSink instead of a broker (Tasks 4, 5); Docker Compose on Linux (Task 10); `v_connections` analysis view (Task 3).
- **Type consistency:** `Flow` fields are consumed identically by `PostgresFlowSink` (Task 4) and produced by `parse_v9` (Task 2). `Settings` field names used in Tasks 5–10 match Task 1 exactly. Partition helper names (`ensure_partition_window`, `drop_old_partitions`, `ensure_daily_partition`, `ensure_monthly_partition`) are consistent between `db/schema.sql` (Task 3) and callers (Tasks 3, 9).
- **Out of scope (per spec):** IPFIX/v5 parsing, passive DNS via mirroring, user-login names, web UI.
```
