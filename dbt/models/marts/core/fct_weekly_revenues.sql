{{ config(
    materialized='table',
    alias='fct_weekly_revenues',
    indexes=[
        {'columns': ['weekly_revenue_key']},
        {'columns': ['movie_key']},
        {'columns': ['year', 'week_of_year_iso']}
    ],
    partition_by={
        'field': 'year',
        'data_type': 'int'
    }
) }}

WITH source_data AS (
    SELECT * FROM {{ ref('int_weekly_revenues') }}
),

weekly_metrics AS (
    SELECT
        *,
        LAG(weekly_revenue) OVER (PARTITION BY movie_key ORDER BY year, week_of_year_iso) AS previous_week_revenue,
        LAG(avg_weekly_theaters) OVER (PARTITION BY movie_key ORDER BY year, week_of_year_iso) AS previous_week_theaters,
        LEAD(weekly_revenue) OVER (PARTITION BY movie_key ORDER BY year, week_of_year_iso) AS next_week_revenue,
        SUM(weekly_revenue) OVER (PARTITION BY movie_key ORDER BY year, week_of_year_iso ROWS UNBOUNDED PRECEDING) AS cumulative_revenue
    FROM source_data
),

calculated_metrics AS (
    SELECT
        *,
        weekly_revenue - COALESCE(previous_week_revenue, 0) AS revenue_change,
        CASE
            WHEN COALESCE(previous_week_revenue, 0) = 0 THEN NULL
            ELSE (weekly_revenue - previous_week_revenue) / previous_week_revenue
        END AS revenue_change_percentage,
        CASE
            WHEN COALESCE(previous_week_theaters, 0) = 0 THEN NULL
            ELSE (avg_weekly_theaters - previous_week_theaters) / previous_week_theaters
        END AS theater_change_percentage,
        CASE
            WHEN next_week_revenue IS NOT NULL AND weekly_revenue > 0
            THEN 1 - (next_week_revenue / weekly_revenue)
            ELSE NULL
        END AS drop_percentage
    FROM weekly_metrics
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['movie_key', 'year', 'week_of_year_iso']) }} AS weekly_revenue_key,
    movie_key,
    year,
    week_of_year_iso,
    week_number_since_release,
    weekly_revenue,
    avg_weekly_theaters AS weekly_theaters,
    previous_week_revenue,
    revenue_change,
    revenue_change_percentage,
    theater_change_percentage,
    avg_revenue_per_theater AS revenue_per_theater,
    drop_percentage,
    days_in_week,
    days_in_theaters,
    week_start_date,
    week_end_date,
    first_revenue_date,
    last_revenue_date,
    released_date,
    run_stage,
    cumulative_revenue,
    CASE
        WHEN week_number_since_release = 1 THEN 'Opening'
        WHEN drop_percentage > 0.5 THEN 'Significant Drop'
        WHEN drop_percentage BETWEEN 0.2 AND 0.5 THEN 'Moderate Drop'
        WHEN drop_percentage < 0.2 THEN 'Stable'
        WHEN drop_percentage < 0 THEN 'Growth'
        ELSE 'Unknown'
    END AS performance_category,
    current_timestamp AS dbt_updated_at
FROM calculated_metrics