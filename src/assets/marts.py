from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets


@dbt_assets(manifest="dbt/target/manifest.json", select="dim_dates")
def dim_dates(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["run", "--select", "dim_dates"], context=context).stream()


@dbt_assets(manifest="dbt/target/manifest.json", select="dim_distributors")
def dim_distributors(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["run", "--select", "dim_distributors"], context=context).stream()


@dbt_assets(manifest="dbt/target/manifest.json", select="dim_movies")
def dim_movies(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["run", "--select", "dim_movies"], context=context).stream()


@dbt_assets(manifest="dbt/target/manifest.json", select="fct_daily_revenues")
def fct_daily_revenues(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["run", "--select", "fct_daily_revenues"], context=context).stream()


@dbt_assets(manifest="dbt/target/manifest.json", select="int_weekly_revenues")
def int_weekly_revenues(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["run", "--select", "int_weekly_revenues"], context=context).stream()


@dbt_assets(manifest="dbt/target/manifest.json", select="fct_weekly_revenues")
def fct_weekly_revenues(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["run", "--select", "fct_weekly_revenues"], context=context).stream()
