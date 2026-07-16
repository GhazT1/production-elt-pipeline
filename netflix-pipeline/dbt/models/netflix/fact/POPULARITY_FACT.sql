{{
    config(
        materialized='incremental',
        unique_key='ID',
        incremental_strategy='merge',
        tags=['FACT'],
        pre_hook="ALTER SESSION SET QUOTED_IDENTIFIERS_IGNORE_CASE = TRUE"
    )
}}

/*
  POPULARITY_FACT
  ---------------
  Joins show details with rating/popularity scores.
  Incremental: on re-run, only rows whose ID appears in the latest
  stage load are merged — full rebuild is avoided.
*/

WITH scores AS (
    SELECT
        ID
        ,IMDB_ID
        ,IMDB_SCORE
        ,IMDB_VOTES
        ,TMDB_POPULARITY
        ,TMDB_SCORE
    FROM {{ ref('scores_votes_stage') }}
)

SELECT
    d.ID
    ,d.TITLE
    ,d.TYPE
    ,d.DESCRIPTION
    ,d.RELEASE_YEAR
    ,d.GENRES
    ,d.RUNTIME_MINUTES
    ,d.AGE_CERTIFICATION
    ,s.IMDB_ID
    ,s.IMDB_SCORE
    ,s.IMDB_VOTES
    ,s.TMDB_POPULARITY
    ,s.TMDB_SCORE
    ,CURRENT_TIMESTAMP()    AS _UPDATED_AT

FROM {{ ref('show_details_stage') }} AS d
LEFT JOIN scores AS s ON d.ID = s.ID

{% if is_incremental() %}
WHERE d.ID IN (
    SELECT ID FROM {{ ref('show_details_stage') }}
    WHERE _LOADED_AT > (SELECT MAX(_UPDATED_AT) FROM {{ this }})
)
{% endif %}
