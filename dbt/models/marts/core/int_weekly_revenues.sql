{{ config(
    materialized='ephemeral',
    alias='int_weekly_revenues'
) }}

WITH daily_data AS (
    SELECT
        f.movie_key,
        d.year,
        d.week_of_year_iso,
        f.revenue,
        f.theaters,
        d.date,
        m.released_date
    FROM {{ ref('fct_daily_revenues') }} f
    JOIN {{ ref('dim_dates') }} d ON f.date_key = d.date_key
    JOIN {{ ref('dim_movies') }} m ON f.movie_key = m.movie_key
),

weekly_aggregates AS (
    SELECT
        movie_key,
        year,
        week_of_year_iso,
        SUM(revenue) AS weekly_revenue,
        AVG(theaters) AS avg_weekly_theaters,
        MIN(date) AS week_start_date,
        MAX(date) AS week_end_date,
        COUNT(DISTINCT date) AS days_in_week,
        MIN(CASE WHEN revenue > 0 THEN date END) AS first_revenue_date,
        MAX(CASE WHEN revenue > 0 THEN date END) AS last_revenue_date,
        SUM(CASE WHEN theaters > 0 THEN 1 ELSE 0 END) AS days_in_theaters
    FROM daily_data
    GROUP BY movie_key, year, week_of_year_iso
),

weekly_metrics AS (
    SELECT
        wa.*,
        ROUND(weekly_revenue / NULLIF(avg_weekly_theaters, 0), 2) AS avg_revenue_per_theater,
        dd.released_date,
        CASE
            WHEN wa.week_start_date = dd.released_date THEN 1
            ELSE DATEDIFF('week', dd.released_date, wa.week_start_date) + 1
        END AS week_number_since_release
    FROM weekly_aggregates wa
    JOIN daily_data dd ON wa.movie_key = dd.movie_key AND wa.week_start_date = dd.date
)

SELECT
    movie_key,
    year,
    week_of_year_iso,
    weekly_revenue,
    avg_weekly_theaters,
    week_start_date,
    week_end_date,
    days_in_week,
    first_revenue_date,
    last_revenue_date,
    days_in_theaters,
    avg_revenue_per_theater,
    released_date,
    week_number_since_release,
    CASE
        WHEN week_number_since_release = 1 THEN 'Opening Week'
        WHEN week_number_since_release <= 4 THEN 'Early Weeks'
        WHEN week_number_since_release <= 8 THEN 'Mid Run'
        ELSE 'Late Run'
    END AS run_stage
FROM weekly_metrics