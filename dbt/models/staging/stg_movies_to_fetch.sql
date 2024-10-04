{{ config(
    materialized='table',
    alias='stg_movies_to_fetch',
    unique_key='clean_title',
    on_schema_change='sync_all_columns'
) }}

WITH latest_revenue AS (
    SELECT
        clean_title,
        MAX(date) AS max_date
    FROM {{ ref('stg_revenue_per_day') }}
    GROUP BY clean_title
)
SELECT
    lr.clean_title,
    lr.max_date,
    CAST(NULL AS DATE) AS omdb_last_fetched_date,
    CAST(NULL AS DATE) AS omdb_last_error_date
FROM latest_revenue lr