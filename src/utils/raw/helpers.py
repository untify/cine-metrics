from typing import List

import duckdb
from dagster import DagsterLogManager


def get_last_ingested_date(conn: duckdb.DuckDBPyConnection, source_table: str) -> str:
    result = conn.execute(
        """
        SELECT MAX(last_ingested_date) FROM raw_ingestion_log
        WHERE source_table = ?
        """,
        [source_table],
    ).fetchone()
    return result[0] if result and result[0] else "1900-01-01"


def update_ingestion_log(
    conn: duckdb.DuckDBPyConnection, source_table: str, last_ingested_date: str, record_count: int
):
    conn.execute(
        """
        INSERT INTO raw_ingestion_log (source_table, last_ingested_date, record_count)
        VALUES (?, ?, ?)
        ON CONFLICT (source_table) DO UPDATE SET
        last_ingested_date = excluded.last_ingested_date,
        record_count = excluded.record_count
        """,
        [source_table, last_ingested_date, record_count],
    )


def get_table_structure(conn: duckdb.DuckDBPyConnection, table_name: str) -> List[str]:
    result = conn.execute(f"DESCRIBE {table_name}").fetchall()
    return [row[0] for row in result]


def create_raw_tables(conn: duckdb.DuckDBPyConnection):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_revenues_per_day (
            id VARCHAR,
            date DATE,
            title VARCHAR,
            revenue VARCHAR,
            theaters VARCHAR,
            distributor VARCHAR,
            ingestion_timestamp TIMESTAMP DEFAULT NOW()
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_ingestion_log (
            source_table VARCHAR PRIMARY KEY,
            last_ingested_date TIMESTAMP,
            record_count INTEGER
        )
        """
    )

    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_raw_revenues_date ON raw_revenues_per_day(date)
        """
    )


def log_error(logger: DagsterLogManager, message: str, exception: Exception = None):
    logger.error(message)
    if exception:
        logger.error(f"Exception details: {str(exception)}")


def insert_new_data(
    conn: duckdb.DuckDBPyConnection,
    config: "ExtractRevenueDataConfig",
    table_structure: List[str],
    last_processed_date: str,
) -> int:
    columns = ", ".join([f"src.{col}" for col in table_structure])
    query = f"""
    WITH new_data AS (
        SELECT {columns}, NOW() as ingestion_timestamp
        FROM main.{config.source_table} src
        WHERE TRY_CAST(src.date AS DATE) > TRY_CAST(? AS DATE)
    )
    INSERT INTO {config.target_table}
    SELECT * FROM new_data
    WHERE NOT EXISTS (
        SELECT 1 FROM {config.target_table} tgt
        WHERE tgt.id = new_data.id AND TRY_CAST(tgt.date AS DATE) = TRY_CAST(new_data.date AS DATE)
    )
    """
    result = conn.execute(query, [last_processed_date])
    return result.fetchone()[0]
