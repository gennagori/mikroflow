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
