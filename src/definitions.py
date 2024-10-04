import os

from dagster import (
    AssetSelection,
    Definitions,
    EnvVar,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_modules,
)
from dagster_dbt import DbtCliResource
from dagster_duckdb_pandas import DuckDBPandasIOManager

from src.assets import marts, raw, staging
from src.resources.database import MotherDuckResource
from src.resources.external import OMDbAPIResource
from src.sensors.data import create_new_revenue_data_sensor

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DBT_ROOT = os.path.join(PROJECT_ROOT, "dbt")

all_assets = [
    *load_assets_from_modules([raw]),
    *load_assets_from_modules([staging]),
    *load_assets_from_modules([marts]),
]

jobs = {
    "raw_job": define_asset_job(
        "raw_job",
        selection=AssetSelection.assets(
            "raw_revenues_per_day",
        ),
    ),
    "staging_job": define_asset_job(
        "staging_job",
        selection=AssetSelection.assets(
            "stg_revenue_per_day",
            "stg_movies_to_fetch",
            "stg_omdb_raw_data",
        ),
    ),
    "marts_job": define_asset_job(
        "marts_job",
        selection=AssetSelection.assets(
            "marts/dim_dates",
            "marts/dim_distributors",
            "marts/dim_movies",
            "marts/fct_daily_revenues",
            "marts/fct_weekly_revenues",
        ),
    ),
    "full_refresh_job": define_asset_job(
        "full_refresh_job",
        selection=AssetSelection.all(),
    ),
}

defs = Definitions(
    assets=all_assets,
    resources={
        "motherduck_io_manager": DuckDBPandasIOManager(
            database="my_db",
            schema="main",
        ),
        "database": MotherDuckResource(
            connection_string=EnvVar("MOTHERDUCK_CONNECTION_STRING"),
            token=EnvVar("MOTHERDUCK_TOKEN"),
        ),
        "omdb_api": OMDbAPIResource(api_key=EnvVar("OMDB_API_KEY")),
        "dbt": DbtCliResource(
            project_dir=DBT_ROOT,
            profiles_dir=DBT_ROOT,
        ),
    },
    schedules=[
        ScheduleDefinition(
            job=jobs["full_refresh_job"],
            cron_schedule="0 1 * * *",
            execution_timezone="UTC",
        )
    ],
    sensors=[create_new_revenue_data_sensor(jobs["full_refresh_job"])],
    jobs=list(jobs.values()),
)
