{{ config(
    materialized='table',
    alias='dim_dates',
    indexes=[
        {'columns': ['date_key']},
        {'columns': ['date']},
        {'columns': ['year', 'month', 'day']}
    ]
) }}

{%- set start_date = "cast('2000-01-01' as date)" -%}
{%- set end_date = "cast('2030-12-31' as date)" -%}

WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date=start_date,
        end_date=end_date
    ) }}
),

base_dates AS (
    SELECT
        date_day,
        EXTRACT(YEAR FROM date_day) AS year,
        EXTRACT(MONTH FROM date_day) AS month,
        EXTRACT(DAY FROM date_day) AS day,
        EXTRACT(DOW FROM date_day) AS day_of_week,
        EXTRACT(QUARTER FROM date_day) AS quarter,
        EXTRACT(DOY FROM date_day) AS day_of_year,
        EXTRACT(WEEK FROM date_day) AS week_of_year
    FROM date_spine
),

enhanced_dates AS (
    SELECT
        *,
        CASE
            WHEN day_of_week = 0 THEN 7
            ELSE day_of_week
        END AS iso_day_of_week,
        CASE
            WHEN day_of_week IN (6, 0) THEN TRUE
            ELSE FALSE
        END AS is_weekend,
        DATE_TRUNC('week', date_day) + INTERVAL 4 DAY AS iso_week_start,
        CONCAT(CAST(EXTRACT(YEAR FROM date_day) AS VARCHAR), '-W',
               LPAD(CAST(EXTRACT(WEEK FROM date_day) AS VARCHAR), 2, '0')) AS iso_week_key
    FROM base_dates
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['date_day']) }} AS date_key,
    date_day AS date,
    year,
    month,
    day,
    iso_day_of_week,
    quarter,
    day_of_year,
    week_of_year,
    CAST(SUBSTRING(iso_week_key, 7) AS INTEGER) AS week_of_year_iso,
    is_weekend,
    CASE
        WHEN month = 1 THEN 'January'
        WHEN month = 2 THEN 'February'
        WHEN month = 3 THEN 'March'
        WHEN month = 4 THEN 'April'
        WHEN month = 5 THEN 'May'
        WHEN month = 6 THEN 'June'
        WHEN month = 7 THEN 'July'
        WHEN month = 8 THEN 'August'
        WHEN month = 9 THEN 'September'
        WHEN month = 10 THEN 'October'
        WHEN month = 11 THEN 'November'
        WHEN month = 12 THEN 'December'
    END AS month_name,
    CASE
        WHEN day_of_week = 0 THEN 'Sunday'
        WHEN day_of_week = 1 THEN 'Monday'
        WHEN day_of_week = 2 THEN 'Tuesday'
        WHEN day_of_week = 3 THEN 'Wednesday'
        WHEN day_of_week = 4 THEN 'Thursday'
        WHEN day_of_week = 5 THEN 'Friday'
        WHEN day_of_week = 6 THEN 'Saturday'
    END AS day_name,
    CASE
        WHEN month IN (12, 1, 2) THEN year
        ELSE year + 1
    END AS fiscal_year,
    CASE
        WHEN month IN (12, 1, 2) THEN 4
        WHEN month IN (3, 4, 5) THEN 1
        WHEN month IN (6, 7, 8) THEN 2
        ELSE 3
    END AS fiscal_quarter,
    current_timestamp AS dbt_updated_at
FROM enhanced_dates