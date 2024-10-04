from dagster import RunConfig, ScheduleDefinition


def create_daily_etl_schedule(job):
    return ScheduleDefinition(
        job=job,
        cron_schedule="0 1 * * *",
        execution_timezone="UTC",
        run_config=RunConfig(
            ops={
                "raw_revenues_per_day": {},
            }
        ),
        should_execute=lambda _: True,
    )
