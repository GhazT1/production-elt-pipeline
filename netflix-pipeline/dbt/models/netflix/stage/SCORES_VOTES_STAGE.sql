{{
    config(
        materialized='table',
        tags=['STAGE'],
        pre_hook="ALTER SESSION SET QUOTED_IDENTIFIERS_IGNORE_CASE = TRUE"
    )
}}

/*
  SCORES_VOTES_STAGE
  ------------------
  Extracts and coalesces rating and popularity scores.
  NULL scores are kept as NULL (not zeroed) so downstream aggregations
  can use COUNT vs AVG correctly without distorting averages.
*/

SELECT
    ID
    ,NULLIF(IMDB_ID, '')            AS IMDB_ID
    ,IMDB_SCORE::FLOAT              AS IMDB_SCORE
    ,IMDB_VOTES::INTEGER            AS IMDB_VOTES
    ,TMDB_POPULARITY::FLOAT         AS TMDB_POPULARITY
    ,TMDB_SCORE::FLOAT              AS TMDB_SCORE

FROM {{ source('netflix_raw', 'TITLES_RAW') }}

WHERE ID IS NOT NULL
