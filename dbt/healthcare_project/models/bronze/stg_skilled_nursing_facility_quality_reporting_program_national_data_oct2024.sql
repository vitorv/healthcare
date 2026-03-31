{{ config(materialized='view') }}

WITH source_data AS (
    SELECT * FROM {{ source('healthcare', 'SKILLED_NURSING_FACILITY_QUALITY_REPORTING_PROGRAM_NATIONAL_DATA_OCT2024') }}
),

renamed AS (
    SELECT
        "CMS Certification Number (CCN)"                   AS cms_certification_number_ccn,
        "Measure Code"                                     AS measure_code,
        "Score"                                            AS score,
        "Footnote"                                         AS footnote,
        "Start Date"                                       AS start_date,
        "End Date"                                         AS end_date,
        "Measure Date Range"                               AS measure_date_range
    FROM source_data
)

SELECT * FROM renamed