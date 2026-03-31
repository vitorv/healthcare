{{ config(materialized='view') }}

WITH source_data AS (
    SELECT * FROM {{ source('healthcare', 'NH_PENALTIES_OCT2024') }}
),

renamed AS (
    SELECT
        "CMS Certification Number (CCN)"                   AS cms_certification_number_ccn,
        "Provider Name"                                    AS provider_name,
        "Provider Address"                                 AS provider_address,
        "City/Town"                                        AS city_town,
        "State"                                            AS state,
        "ZIP Code"                                         AS zip_code,
        "Penalty Date"                                     AS penalty_date,
        "Penalty Type"                                     AS penalty_type,
        "Fine Amount"                                      AS fine_amount,
        "Payment Denial Start Date"                        AS payment_denial_start_date,
        "Payment Denial Length in Days"                    AS payment_denial_length_in_days,
        "Location"                                         AS location,
        "Processing Date"                                  AS processing_date
    FROM source_data
)

SELECT * FROM renamed