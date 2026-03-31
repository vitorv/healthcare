{{ config(materialized='view') }}

WITH source_data AS (
    SELECT * FROM {{ source('healthcare', 'NH_CITATIONDESCRIPTIONS_OCT2024') }}
),

renamed AS (
    SELECT
        "Deficiency Prefix"                                AS deficiency_prefix,
        "Deficiency Tag Number"                            AS deficiency_tag_number,
        "Deficiency Prefix and Number"                     AS deficiency_prefix_and_number,
        "Deficiency Description"                           AS deficiency_description,
        "Deficiency Category"                              AS deficiency_category
    FROM source_data
)

SELECT * FROM renamed