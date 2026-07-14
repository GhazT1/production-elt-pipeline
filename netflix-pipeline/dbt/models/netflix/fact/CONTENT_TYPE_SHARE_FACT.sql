{{
    config(
        materialized='incremental',
        unique_key='GENRE',
        incremental_strategy='merge',
        tags=['FACT'],
        pre_hook="ALTER SESSION SET QUOTED_IDENTIFIERS_IGNORE_CASE = TRUE"
    )
}}

/*
  CONTENT_TYPE_SHARE_FACT
  -----------------------
  For each genre, calculates what percentage of content is movies vs shows.
  Each genre row shows: total_movies, total_shows, total_content,
  and the percentage split.
*/

WITH exploded AS (
    SELECT
        TRIM(g.VALUE::STRING)  AS GENRE
        ,TYPE
    FROM {{ ref('SHOW_DETAILS_STAGE') }}
    ,LATERAL SPLIT_TO_TABLE(GENRES, ',') AS g
    WHERE GENRES IS NOT NULL
      AND TYPE IS NOT NULL
),

aggregated AS (
    SELECT
        GENRE
        ,SUM(CASE WHEN TYPE = 'MOVIE' THEN 1 ELSE 0 END)   AS TOTAL_MOVIES
        ,SUM(CASE WHEN TYPE = 'SHOW'  THEN 1 ELSE 0 END)   AS TOTAL_SHOWS
        ,COUNT(*)                                            AS TOTAL_CONTENT
    FROM exploded
    GROUP BY GENRE
)

SELECT
    GENRE
    ,TOTAL_MOVIES
    ,TOTAL_SHOWS
    ,TOTAL_CONTENT
    ,ROUND(TOTAL_MOVIES / NULLIF(TOTAL_CONTENT, 0) * 100, 1)  AS MOVIE_PCT
    ,ROUND(TOTAL_SHOWS  / NULLIF(TOTAL_CONTENT, 0) * 100, 1)  AS SHOW_PCT
    ,CURRENT_TIMESTAMP()                                        AS _UPDATED_AT

FROM aggregated

{% if is_incremental() %}
WHERE GENRE IN (
    SELECT DISTINCT TRIM(g.VALUE::STRING)
    FROM {{ ref('SHOW_DETAILS_STAGE') }}
    ,LATERAL SPLIT_TO_TABLE(GENRES, ',') AS g
    WHERE _LOADED_AT > (SELECT MAX(_UPDATED_AT) FROM {{ this }})
)
{% endif %}
