{{ config(materialized='view') }}

WITH source_data AS (
    SELECT * FROM {{ source('healthcare', 'SKILLED_NURSING_FACILITY_QUALITY_REPORTING_PROGRAM_PROVIDER_DATA_OCT2024') }}
),

renamed AS (
    SELECT
        "CMS Certification Number (CCN)"                   AS cms_certification_number_ccn,
        "Provider Name"                                    AS provider_name,
        "Address Line 1"                                   AS address_line_1,
        "City/Town"                                        AS city_town,
        "State"                                            AS state,
        "ZIP Code"                                         AS zip_code,
        "County/Parish"                                    AS county_parish,
        "Telephone Number"                                 AS telephone_number,
        "CMS Region"                                       AS cms_region,
        "Measure Code"                                     AS measure_code,
        "Score"                                            AS score,
        "Footnote"                                         AS footnote,
        "Start Date"                                       AS start_date,
        "End Date"                                         AS end_date,
        "Measure Date Range"                               AS measure_date_range,
        "LOCATION1"
    FROM source_data
)

SELECT * FROM renamed