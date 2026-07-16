{{
    config(
        materialized='incremental',
        unique_key="GENRE || '|' || ACTOR_NAME",
        incremental_strategy='merge',
        tags=['FACT'],
        pre_hook="ALTER SESSION SET QUOTED_IDENTIFIERS_IGNORE_CASE = TRUE"
    )
}}

/*
  ACTORS_DOMINATING_FACT
  ----------------------
  For each genre, ranks actors by total number of appearances.

  GENRES arrives from SHOW_DETAILS_STAGE as a comma-separated string,
  e.g. 'ACTION, DRAMA'. We split it with LATERAL SPLIT_TO_TABLE so
  each actor-genre pair gets its own row — no string manipulation in
  the fact layer.
*/

WITH actors AS (
    SELECT ID, NAME AS ACTOR_NAME
    FROM {{ ref('credits_stage') }}
    WHERE ROLE = 'ACTOR'
),

show_genres AS (
    SELECT
        sd.ID
        ,TRIM(g.VALUE::STRING) AS GENRE
    FROM {{ ref('show_details_stage') }} AS sd
    -- Split comma-separated genres into individual rows
    ,LATERAL SPLIT_TO_TABLE(sd.GENRES, ',') AS g
    WHERE sd.GENRES IS NOT NULL
)

SELECT
    sg.GENRE
    ,a.ACTOR_NAME
    ,COUNT(*)                   AS TOTAL_PERFORMANCES
    ,CURRENT_TIMESTAMP()        AS _UPDATED_AT

FROM show_genres AS sg
JOIN actors AS a ON sg.ID = a.ID

GROUP BY sg.GENRE, a.ACTOR_NAME

{% if is_incremental() %}
HAVING sg.GENRE || '|' || a.ACTOR_NAME IN (
    SELECT GENRE || '|' || ACTOR_NAME
    FROM {{ this }}
    WHERE _UPDATED_AT > DATEADD(DAY, -1, CURRENT_TIMESTAMP())
)
{% endif %}
