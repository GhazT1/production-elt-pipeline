{{
    config(
        materialized='table',
        tags=['STAGE'],
        pre_hook="ALTER SESSION SET QUOTED_IDENTIFIERS_IGNORE_CASE = TRUE"
    )
}}

/*
  CREDITS_STAGE
  -------------
  Filters credits to only ACTOR and DIRECTOR roles and deduplcates
  person-title combinations that appear more than once in the raw load.
*/

SELECT DISTINCT
    ID
    ,UPPER(TRIM(NAME))  AS NAME
    ,UPPER(TRIM(ROLE))  AS ROLE
    ,CHARACTER

FROM {{ source('netflix_raw', 'CREDITS_RAW') }}

WHERE ROLE IN ('ACTOR', 'DIRECTOR')
  AND ID IS NOT NULL
  AND NAME IS NOT NULL
