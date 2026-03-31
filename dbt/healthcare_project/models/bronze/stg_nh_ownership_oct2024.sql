{{ config(materialized='view') }}

WITH source_data AS (
    SELECT * FROM {{ source('healthcare', 'NH_OWNERSHIP_OCT2024') }}
),

renamed AS (
    SELECT
        "CMS Certification Number (CCN)"                   AS cms_certification_number_ccn,
        "Provider Name"                                    AS provider_name,
        "Provider Address"                                 AS provider_address,
        "City/Town"                                        AS city_town,
        "State"                                            AS state,
        "ZIP Code"                                         AS zip_code,
        "Role played by Owner or Manager in Facility"      AS role_played_by_owner_or_manager_in_facility,
        "Owner Type"                                       AS owner_type,
        "Owner Name"                                       AS owner_name,
        "Ownership Percentage"                             AS ownership_percentage,
        "Association Date"                                 AS association_date,
        "Location"                                         AS location,
        "Processing Date"                                  AS processing_date
    FROM source_data
)

SELECT * FROM renamed