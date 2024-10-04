from datetime import datetime, timedelta

from dagster import AssetExecutionContext, Output, asset
from dagster_dbt import DbtCliResource, dbt_assets

from ..resources.external import APILimitReachedException
from ..utils.staging.helpers import (
    get_titles_to_fetch,
    initialize_tables,
    process_movies,
    update_api_usage_log,
)


@dbt_assets(manifest="dbt/target/manifest.json", select="stg_revenue_per_day")
def stg_revenue_per_day(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["run", "--select", "stg_revenue_per_day"], context=context).stream()


@dbt_assets(manifest="dbt/target/manifest.json", select="stg_movies_to_fetch")
def stg_movies_to_fetch(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["run", "--select", "stg_movies_to_fetch"], context=context).stream()


@asset(
    deps=["stg_movies_to_fetch"],
    compute_kind="python",
    group_name="api_extraction",
    description="Fetch movie data from OMDB API",
    required_resource_keys={"omdb_api", "database"},
)
async def stg_omdb_raw_data(context: AssetExecutionContext) -> Output[None]:
    omdb_api = context.resources.omdb_api
    database = context.resources.database
    current_date = datetime.now().date()
    seven_days_ago = current_date - timedelta(days=7)

    try:
        await database.run_async(initialize_tables, database)
        titles_to_fetch = await database.run_async(get_titles_to_fetch, database, seven_days_ago)

        async with omdb_api.get_session() as session:
            processed_count, total_requests = await process_movies(
                context, omdb_api, database, session, titles_to_fetch, current_date
            )

        await database.run_async(update_api_usage_log, database, current_date, total_requests)
        context.log.info(f"Fetched and updated data for {processed_count} movies")

        return Output(None, metadata={"movies_fetched": processed_count})
    except APILimitReachedException as e:
        context.log.warning(f"API Limit reached: {str(e)}")
        context.log.info("API limit reached. Stopping further processing.")
        return Output(None, metadata={"api_limit_reached": True})
    except Exception as e:
        context.log.error(f"An error occurred in stg_omdb_raw_data: {str(e)}")
        raise
