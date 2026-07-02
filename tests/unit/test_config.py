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
