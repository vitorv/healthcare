{{ config(materialized='view') }}

WITH source_data AS (
    SELECT * FROM {{ source('healthcare', 'NH_HEALTHCITATIONS_OCT2024') }}
),

renamed AS (
    SELECT
        "CMS Certification Number (CCN)"                   AS cms_certification_number_ccn,
        "Provider Name"                                    AS provider_name,
        "Provider Address"                                 AS provider_address,
        "City/Town"                                        AS city_town,
        "State"                                            AS state,
        "ZIP Code"                                         AS zip_code,
        "Survey Date"                                      AS survey_date,
        "Survey Type"                                      AS survey_type,
        "Deficiency Prefix"                                AS deficiency_prefix,
        "Deficiency Category"                              AS deficiency_category,
        "Deficiency Tag Number"                            AS deficiency_tag_number,
        "Deficiency Description"                           AS deficiency_description,
        "Scope Severity Code"                              AS scope_severity_code,
        "Deficiency Corrected"                             AS deficiency_corrected,
        "Correction Date"                                  AS correction_date,
        "Inspection Cycle"                                 AS inspection_cycle,
        "Standard Deficiency"                              AS standard_deficiency,
        "Complaint Deficiency"                             AS complaint_deficiency,
        "Infection Control Inspection Deficiency"          AS infection_control_inspection_deficiency,
        "Citation under IDR"                               AS citation_under_idr,
        "Citation under IIDR"                              AS citation_under_iidr,
        "Location"                                         AS location,
        "Processing Date"                                  AS processing_date
    FROM source_data
)

SELECT * FROM renamed