{{ config(
    materialized='table',
    alias='fct_daily_revenues',
    indexes=[
        {'columns': ['revenue_key']},
        {'columns': ['date_key']},
        {'columns': ['movie_key']},
        {'columns': ['distributor_key']}
    ],
    partition_by={
        'field': 'revenue_date',
        'data_type': 'date',
        'granularity': 'month'
    }
) }}

WITH source_data AS (
    SELECT
        s.id,
        s.date,
        s.revenue,
        s.theaters,
        s.clean_title,
        s.distributor,
        d.date_key,
        m.movie_key,
        dist.distributor_key,
        m.released_date
    FROM {{ ref('stg_revenue_per_day') }} s
    LEFT JOIN {{ ref('dim_dates') }} d ON s.date = d.date
    LEFT JOIN {{ ref('dim_movies') }} m ON s.clean_title = m.clean_title
    LEFT JOIN {{ ref('dim_distributors') }} dist ON s.distributor = dist.distributor
),

daily_metrics AS (
    SELECT
        *,
        CASE
            WHEN theaters > 0 THEN revenue / theaters
            ELSE 0
        END AS revenue_per_theater,
        LAG(revenue) OVER (PARTITION BY movie_key ORDER BY date) AS prev_day_revenue,
        LEAD(revenue) OVER (PARTITION BY movie_key ORDER BY date) AS next_day_revenue,
        ROW_NUMBER() OVER (PARTITION BY movie_key ORDER BY date) AS day_number
    FROM source_data
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['id', 'date']) }} AS revenue_key,
    date_key,
    movie_key,
    distributor_key,
    revenue,
    theaters,
    date AS revenue_date,
    revenue_per_theater,
    CASE
        WHEN prev_day_revenue IS NOT NULL THEN (revenue - prev_day_revenue) / prev_day_revenue
        ELSE NULL
    END AS day_over_day_change,
    CASE
        WHEN revenue > 0 AND next_day_revenue IS NOT NULL THEN 1 - (next_day_revenue / revenue)
        ELSE NULL
    END AS drop_percentage,
    day_number,
    DATEDIFF('day', released_date, date) AS days_since_release,
    current_timestamp AS dbt_updated_at
FROM daily_metrics