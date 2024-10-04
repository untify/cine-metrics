{{ config(
    materialized='table',
    alias='dim_distributors',
    indexes=[
        {'columns': ['distributor_key']},
        {'columns': ['distributor']}
    ]
) }}

WITH source_data AS (
    SELECT DISTINCT distributor
    FROM {{ ref('stg_revenue_per_day') }}
    WHERE distributor IS NOT NULL
),

distributor_stats AS (
    SELECT
        distributor,
        COUNT(DISTINCT id) AS total_movies,
        MIN(date) AS first_appearance_date,
        MAX(date) AS last_appearance_date,
        SUM(revenue) AS total_revenue,
        AVG(revenue) AS avg_revenue_per_movie
    FROM {{ ref('stg_revenue_per_day') }}
    WHERE distributor IS NOT NULL
    GROUP BY distributor
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['sd.distributor']) }} AS distributor_key,
    sd.distributor,
    COALESCE(ds.total_movies, 0) AS total_movies,
    ds.first_appearance_date,
    ds.last_appearance_date,
    COALESCE(ds.total_revenue, 0) AS total_revenue,
    COALESCE(ds.avg_revenue_per_movie, 0) AS avg_revenue_per_movie,
    CASE
        WHEN ds.total_revenue > 1000000000 THEN 'Major'
        WHEN ds.total_revenue > 100000000 THEN 'Medium'
        ELSE 'Minor'
    END AS distributor_category,
    current_timestamp AS dbt_updated_at
FROM source_data sd
LEFT JOIN distributor_stats ds ON sd.distributor = ds.distributor