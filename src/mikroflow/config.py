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
