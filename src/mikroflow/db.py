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
