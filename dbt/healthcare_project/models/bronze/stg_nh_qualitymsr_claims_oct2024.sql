{{ config(materialized='view') }}

WITH source_data AS (
    SELECT * FROM {{ source('healthcare', 'NH_QUALITYMSR_CLAIMS_OCT2024') }}
),

renamed AS (
    SELECT
        "CMS Certification Number (CCN)"                   AS cms_certification_number_ccn,
        "Provider Name"                                    AS provider_name,
        "Provider Address"                                 AS provider_address,
        "City/Town"                                        AS city_town,
        "State"                                            AS state,
        "ZIP Code"                                         AS zip_code,
        "Measure Code"                                     AS measure_code,
        "Measure Description"                              AS measure_description,
        "Resident type"                                    AS resident_type,
        "Adjusted Score"                                   AS adjusted_score,
        "Observed Score"                                   AS observed_score,
        "Expected Score"                                   AS expected_score,
        "Footnote for Score"                               AS footnote_for_score,
        "Used in Quality Measure Five Star Rating"         AS used_in_quality_measure_five_star_rating,
        "Measure Period"                                   AS measure_period,
        "Location"                                         AS location,
        "Processing Date"                                  AS processing_date
    FROM source_data
)

SELECT * FROM renamed