from dagster import RunConfig, RunRequest, SensorResult, sensor


def create_new_revenue_data_sensor(job):
    @sensor(job=job)
    def new_revenue_data_sensor(context):
        with context.resources.database.get_connection() as conn:
            result = conn.execute(
                """
                SELECT COUNT(*) as new_count
                FROM main.revenues_per_day r
                LEFT JOIN raw_ingestion_log l ON l.source_table = 'revenues_per_day'
                WHERE r.date > COALESCE(l.last_ingested_date, '1900-01-01')
            """
            ).fetchone()

        new_count = result[0] if result else 0

        if new_count > 0:
            return RunRequest(
                run_key=f"new_revenue_data_{new_count}",
                run_config=RunConfig(
                    ops={"raw_revenues_per_day": {"config": {"limit": min(new_count, 10000)}}}
                ),
            )

        return SensorResult(skip_reason="No new revenue data available")

    return new_revenue_data_sensor
