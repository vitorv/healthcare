{{ config(materialized='view') }}

WITH source_data AS (
    SELECT * FROM {{ source('healthcare', 'NH_SURVEYDATES_OCT2024') }}
),

renamed AS (
    SELECT
        "CMS Certification Number (CCN)"                   AS cms_certification_number_ccn,
        "Survey Date"                                      AS survey_date,
        "Type of Survey"                                   AS type_of_survey,
        "Survey Cycle"                                     AS survey_cycle,
        "Processing Date"                                  AS processing_date
    FROM source_data
)

SELECT * FROM renamed