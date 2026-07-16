{{
    config(
        materialized='table',
        tags=['STAGE'],
        pre_hook="ALTER SESSION SET QUOTED_IDENTIFIERS_IGNORE_CASE = TRUE"
    )
}}

/*
  SHOW_DETAILS_STAGE
  ------------------
  Cleans and selects show/movie metadata from the raw titles table.
  Normalises the GENRES array column into a plain comma-separated string
  so downstream models don't need repeated string manipulation.
*/

SELECT
    ID
    ,TITLE
    ,TYPE
    ,DESCRIPTION
    ,RELEASE_YEAR::INTEGER                                      AS RELEASE_YEAR
    ,NULLIF(TRIM(AGE_CERTIFICATION), '')                        AS AGE_CERTIFICATION
    ,RUNTIME::INTEGER                                           AS RUNTIME_MINUTES
    -- Strip Python list brackets: ['Action', 'Drama'] → 'ACTION, DRAMA'
    ,UPPER(
        TRIM(
            REGEXP_REPLACE(GENRES, '[\\[\\]''"]', '')
        )
    )                                                           AS GENRES
    ,UPPER(
        TRIM(
            REGEXP_REPLACE(PRODUCTION_COUNTRIES, '[\\[\\]''"]', '')
        )
    )                                                           AS PRODUCTION_COUNTRIES
    ,SEASONS::INTEGER                                           AS SEASONS
    ,CURRENT_TIMESTAMP()                                        AS _LOADED_AT

FROM {{ source('netflix_raw', 'TITLES_RAW') }}

-- Exclude rows with no usable ID or title
WHERE ID IS NOT NULL
  AND TITLE IS NOT NULL
