import pytest
from testcontainers.postgres import PostgresContainer

from mikroflow.db import apply_schema, ensure_partitions, make_pool


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
            "TRUNCATE flows_raw, dhcp_leases, arp, device_alias, ip_domain, "
            "flows_hourly, agg_state"
        )
    yield p
    p.close()
