{{ config(
    materialized='incremental',
    alias='stg_revenue_per_day',
    unique_key='id || date',
    on_schema_change='sync_all_columns',
    index=['date', 'clean_title']
) }}

{% set year_pattern = '(19\d{2}|20\d{2})' %}
{% set rerelease_pattern = '\s*(Re-release|Remaster|IMAX|3D|4K|HD)' %}
{% set anniversary_pattern = '\s*\d+(\s*th|\s*st|\s*nd|\s*rd)?\s*(Year\s*)?Anniversary' %}

WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_revenues_per_day') }}
    {% if is_incremental() %}
    WHERE date > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
),
cleaned_titles AS (
    SELECT
        *,
        title::VARCHAR AS original_title,
        REGEXP_REPLACE(title::VARCHAR, '{{ rerelease_pattern }}.*$', '') AS step1,
        REGEXP_REPLACE(step1, '{{ anniversary_pattern }}.*$', '') AS step2,
        CASE
            WHEN REGEXP_MATCHES(step2, '^\(.*\)$') THEN step2
            ELSE REGEXP_REPLACE(step2, '\s*\([^)]*\)$', '')
        END AS step3,
        REGEXP_REPLACE(step3, '[^\w\s\-:'''']', ' ') AS step4,
        REGEXP_REPLACE(step4, '\s+', ' ') AS step5,
        CASE
            WHEN REGEXP_MATCHES(step5, '^{{ year_pattern }}$') THEN step5
            ELSE REGEXP_REPLACE(step5, '\s+{{ year_pattern }}$', '')
        END AS step6
    FROM source
)

SELECT
    id::VARCHAR AS id,
    TRY_CAST(date AS DATE) AS date,
    original_title,
    LOWER(TRIM(step6)) AS clean_title,
    TRY_CAST(revenue AS DECIMAL(18,2)) AS revenue,
    TRY_CAST(theaters AS INTEGER) AS theaters,
    distributor::VARCHAR AS distributor,
    ingestion_timestamp,
    current_timestamp AS etl_updated_at
FROM cleaned_titles