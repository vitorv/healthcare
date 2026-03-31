{{ config(materialized='view') }}

WITH source_data AS (
    SELECT * FROM {{ source('healthcare', 'NH_HLTHINSPECCUTPOINTSSTATE_OCT2024') }}
),

renamed AS (
    SELECT
        "State"                                            AS state,
        "5 Stars"                                          AS _5_stars,
        "4 Stars"                                          AS _4_stars,
        "3 Stars"                                          AS _3_stars,
        "2 Stars"                                          AS _2_stars,
        "1 Star"                                           AS _1_star
    FROM source_data
)

SELECT * FROM renamed