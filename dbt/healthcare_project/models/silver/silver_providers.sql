{{ config(materialized='table') }}

{#
    Silver Layer: Cleaned, typed, and deduplicated nursing home provider data.
    One row per CMS Certification Number (CCN).

    Source: stg_nh_providerinfo_oct2024

    6 Core Responsibilities:
      1. Data type casting — TRY_TO_* safe casts (Bronze is all VARCHAR)
      2. Null / default handling — NULLIF for empty strings
      3. Deduplication — ROW_NUMBER on CCN
      4. Business-key integrity — WHERE ccn IS NOT NULL
      5. Column pruning — 103 cols → ~40 (all footnotes removed)
      6. Light standardization — TRIM, UPPER state, LPAD zip, phone cleanup

    Supports metrics: 1.1, 2.3, 2.5, 3.5, v1-Q2
#}

WITH source AS (
    SELECT * FROM {{ ref('stg_nh_providerinfo_oct2024') }}
),

deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY cms_certification_number_ccn
            ORDER BY processing_date DESC
        ) AS _row_num
    FROM source
),

cleaned AS (
    SELECT
        -- ══════════════════════════════════════════════
        -- Primary Key
        -- ══════════════════════════════════════════════
        TRIM(cms_certification_number_ccn)                          AS ccn,

        -- ══════════════════════════════════════════════
        -- Provider Demographics
        -- ══════════════════════════════════════════════
        NULLIF(TRIM(provider_name), '')                             AS provider_name,
        NULLIF(TRIM(provider_address), '')                          AS provider_address,
        NULLIF(TRIM(city_town), '')                                 AS city,
        UPPER(TRIM(state))                                          AS state_code,
        LPAD(TRIM(zip_code), 5, '0')                                AS zip_code,
        REGEXP_REPLACE(telephone_number, '[^0-9]', '')              AS phone_number,
        NULLIF(TRIM(county_parish), '')                             AS county,

        -- ══════════════════════════════════════════════
        -- Facility Classification
        -- ══════════════════════════════════════════════
        NULLIF(TRIM(ownership_type), '')                            AS ownership_type,
        NULLIF(TRIM(provider_type), '')                             AS provider_type,

        CASE
            WHEN UPPER(TRIM(provider_resides_in_hospital)) = 'Y' THEN TRUE
            WHEN UPPER(TRIM(provider_resides_in_hospital)) = 'N' THEN FALSE
            ELSE NULL
        END                                                         AS is_hospital_based,

        CASE
            WHEN UPPER(TRIM(continuing_care_retirement_community)) = 'Y' THEN TRUE
            WHEN UPPER(TRIM(continuing_care_retirement_community)) = 'N' THEN FALSE
            ELSE NULL
        END                                                         AS is_ccrc,

        -- ══════════════════════════════════════════════
        -- Capacity & Occupancy (Metrics 2.3, 2.5)
        -- ══════════════════════════════════════════════
        TRY_TO_NUMBER(number_of_certified_beds)                     AS certified_bed_count,
        TRY_TO_DECIMAL(average_number_of_residents_per_day, 10, 1)  AS avg_daily_residents,

        -- ══════════════════════════════════════════════
        -- CMS Star Ratings
        -- ══════════════════════════════════════════════
        TRY_TO_NUMBER(overall_rating)                               AS overall_rating,
        TRY_TO_NUMBER(health_inspection_rating)                     AS health_inspection_rating,
        TRY_TO_NUMBER(qm_rating)                                    AS quality_measure_rating,
        TRY_TO_NUMBER(long_stay_qm_rating)                          AS long_stay_qm_rating,
        TRY_TO_NUMBER(short_stay_qm_rating)                         AS short_stay_qm_rating,
        TRY_TO_NUMBER(staffing_rating)                              AS staffing_rating,

        -- ══════════════════════════════════════════════
        -- Reported Staffing Hours per Resident per Day (Metric 1.1)
        -- ══════════════════════════════════════════════
        TRY_TO_DECIMAL(reported_nurse_aide_staffing_hours_per_resident_per_day, 10, 2)
            AS reported_cna_hours_prd,
        TRY_TO_DECIMAL(reported_lpn_staffing_hours_per_resident_per_day, 10, 2)
            AS reported_lpn_hours_prd,
        TRY_TO_DECIMAL(reported_rn_staffing_hours_per_resident_per_day, 10, 2)
            AS reported_rn_hours_prd,
        TRY_TO_DECIMAL(reported_licensed_staffing_hours_per_resident_per_day, 10, 2)
            AS reported_licensed_hours_prd,
        TRY_TO_DECIMAL(reported_total_nurse_staffing_hours_per_resident_per_day, 10, 2)
            AS reported_total_nurse_hours_prd,
        TRY_TO_DECIMAL(total_number_of_nurse_staff_hours_per_resident_per_day_on_the_weekend, 10, 2)
            AS weekend_total_nurse_hours_prd,
        TRY_TO_DECIMAL(registered_nurse_hours_per_resident_per_day_on_the_weekend, 10, 2)
            AS weekend_rn_hours_prd,
        TRY_TO_DECIMAL(reported_physical_therapist_staffing_hours_per_resident_per_day, 10, 2)
            AS reported_pt_hours_prd,

        -- ══════════════════════════════════════════════
        -- Case-Mix Adjusted Staffing Hours
        -- ══════════════════════════════════════════════
        TRY_TO_DECIMAL(nursing_case_mix_index, 10, 4)               AS nursing_case_mix_index,
        TRY_TO_DECIMAL(case_mix_nurse_aide_staffing_hours_per_resident_per_day, 10, 2)
            AS casemix_cna_hours_prd,
        TRY_TO_DECIMAL(case_mix_lpn_staffing_hours_per_resident_per_day, 10, 2)
            AS casemix_lpn_hours_prd,
        TRY_TO_DECIMAL(case_mix_rn_staffing_hours_per_resident_per_day, 10, 2)
            AS casemix_rn_hours_prd,
        TRY_TO_DECIMAL(case_mix_total_nurse_staffing_hours_per_resident_per_day, 10, 2)
            AS casemix_total_nurse_hours_prd,

        -- ══════════════════════════════════════════════
        -- Turnover (Metric v1-Q2)
        -- ══════════════════════════════════════════════
        TRY_TO_DECIMAL(total_nursing_staff_turnover, 10, 2)         AS total_nurse_turnover_pct,
        TRY_TO_DECIMAL(registered_nurse_turnover, 10, 2)            AS rn_turnover_pct,
        TRY_TO_NUMBER(number_of_administrators_who_have_left_the_nursing_home)
            AS administrator_departures,

        -- ══════════════════════════════════════════════
        -- Complaints & Penalties (for context / benchmarking)
        -- ══════════════════════════════════════════════
        TRY_TO_NUMBER(number_of_substantiated_complaints)           AS substantiated_complaints,
        TRY_TO_NUMBER(number_of_facility_reported_incidents)        AS facility_reported_incidents,
        TRY_TO_NUMBER(number_of_fines)                              AS fine_count,
        TRY_TO_DECIMAL(total_amount_of_fines_in_dollars, 12, 2)    AS total_fine_amount,
        TRY_TO_NUMBER(total_number_of_penalties)                    AS total_penalties,

        -- ══════════════════════════════════════════════
        -- Geography
        -- ══════════════════════════════════════════════
        TRY_TO_DOUBLE(latitude)                                     AS latitude,
        TRY_TO_DOUBLE(longitude)                                    AS longitude,

        -- ══════════════════════════════════════════════
        -- Metadata
        -- ══════════════════════════════════════════════
        TRY_TO_DATE(processing_date)                                AS processing_date

    FROM deduplicated
    WHERE _row_num = 1
)

SELECT *
FROM cleaned
WHERE ccn IS NOT NULL
