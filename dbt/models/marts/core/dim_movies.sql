{{ config(
    materialized='table',
    alias='dim_movies',
    indexes=[
        {'columns': ['movie_key']},
        {'columns': ['imdb_id']},
        {'columns': ['clean_title']}
    ]
) }}

{%- set json_columns = [
    'imdbID', 'Title', 'Year', 'Rated', 'Released', 'Runtime',
    'Director', 'Actors', 'BoxOffice', 'imdbRating', 'imdbVotes'
] -%}

WITH source_data AS (
    SELECT * FROM {{ source('staging', 'stg_omdb_raw_data') }}
),

extracted_data AS (
    SELECT
        {%- for column in json_columns %}
        data->>'{{ column }}' AS {{ column | lower }},
        {%- endfor %}
        last_updated
    FROM source_data
),

transformed_data AS (
    SELECT
        *,
        TRIM(REGEXP_REPLACE(LOWER(title), '[^a-zA-Z0-9 ]', '')) AS clean_title,
        TRY_CAST(year AS INTEGER) AS year_int,
        TRY_CAST(released AS DATE) AS released_date,
        TRY_CAST(REGEXP_EXTRACT(runtime, '(\d+)') AS INTEGER) AS runtime_minutes,
        TRY_CAST(REPLACE(REPLACE(boxoffice, '$', ''), ',', '') AS DECIMAL(18,2)) AS box_office_total,
        TRY_CAST(imdbrating AS DECIMAL(3,1)) AS imdb_rating_num,
        TRY_CAST(REPLACE(imdbvotes, ',', '') AS INTEGER) AS imdb_votes_int
    FROM extracted_data
    WHERE REGEXP_MATCHES(year, '^\d{4}$')
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['imdbid', 'last_updated']) }} AS movie_key,
    imdbid AS imdb_id,
    title,
    clean_title,
    year_int AS year,
    rated,
    released_date,
    runtime_minutes,
    director,
    actors,
    box_office_total,
    imdb_rating_num AS imdb_rating,
    imdb_votes_int AS imdb_votes,
    last_updated,
    current_timestamp AS dbt_updated_at
FROM transformed_data