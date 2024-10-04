from datetime import datetime

from dagster import Config, Output, asset

from ..resources.database import MotherDuckResource
from ..utils.raw.helpers import (
    create_raw_tables,
    get_last_ingested_date,
    get_table_structure,
    insert_new_data,
    log_error,
    update_ingestion_log,
)


class ExtractRevenueDataConfig(Config):
    source_table: str = "revenues_per_day"
    target_table: str = "raw_revenues_per_day"


@asset(
    group_name="raw_data",
    compute_kind="python",
    required_resource_keys={"database"},
)
def raw_revenues_per_day(context, config: ExtractRevenueDataConfig) -> Output[None]:
    database: MotherDuckResource = context.resources.database
    try:
        with database.connection() as conn:
            create_raw_tables(conn)
            last_processed_date = get_last_ingested_date(conn, config.source_table)
            context.log.info(f"Last processed date: {last_processed_date}")
            table_structure = get_table_structure(conn, f"main.{config.source_table}")

            processed_count = insert_new_data(conn, config, table_structure, last_processed_date)

            if processed_count == 0:
                context.log.info("No new revenue data to process.")
                return Output(None, metadata={"row_count": 0})

            current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            update_ingestion_log(conn, config.source_table, current_date, processed_count)

        context.log.info(f"Processed {processed_count} new revenue records.")
        return Output(
            None,
            metadata={
                "row_count": processed_count,
                "last_processed_date": current_date,
            },
        )
    except Exception as e:
        log_error(context.log, "Error in raw_revenues_per_day", e)
        raise
