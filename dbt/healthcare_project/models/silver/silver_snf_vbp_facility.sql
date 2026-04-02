{{ config(materialized='table') }}

{#
    Silver Layer: Cleaned and typed SNF Value-Based Purchasing facility performance data.
    One row per CMS Certification Number (CCN).

    Source: stg_fy_2024_snf_vbp_facility_performance

    6 Core Responsibilities:
      1. Data type casting — TRY_TO_* safe casts (Bronze is all VARCHAR)
      2. Null / default handling — NULLIF for strings, TRY_TO_* returns NULL for '---'
      3. Deduplication — ROW_NUMBER on CCN
      4. Business-key integrity — WHERE ccn IS NOT NULL
      5. Column pruning — 20 cols → 13 (all footnote columns removed)
      6. Light standardization — TRIM, UPPER state, LPAD zip

    Supports metrics: 3.3 (readmission rates), 3.5 (staffing-readmission correlation)
#}

WITH source AS (
    SELECT * FROM {{ ref('stg_fy_2024_snf_vbp_facility_performance') }}
),

deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY cms_certification_number_ccn
            ORDER BY snf_vbp_program_ranking ASC NULLS LAST
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
        -- Provider Info
        -- ══════════════════════════════════════════════
        NULLIF(TRIM(provider_name), '')                             AS provider_name,
        NULLIF(TRIM(provider_address), '')                          AS provider_address,
        NULLIF(TRIM(city_town), '')                                 AS city,
        UPPER(TRIM(state))                                          AS state_code,
        LPAD(TRIM(zip_code), 5, '0')                                AS zip_code,

        -- ══════════════════════════════════════════════
        -- Readmission Rates (Metric 3.3, 3.5)
        -- Values like '---' are safely converted to NULL by TRY_TO_DECIMAL
        -- ══════════════════════════════════════════════
        TRY_TO_DECIMAL(baseline_period_fy_2019_risk_standardized_readmission_rate, 10, 4)
            AS baseline_readmission_rate,
        TRY_TO_DECIMAL(performance_period_fy_2022_risk_standardized_readmission_rate, 10, 4)
            AS performance_readmission_rate,

        -- ══════════════════════════════════════════════
        -- VBP Scores
        -- ══════════════════════════════════════════════
        TRY_TO_DECIMAL(achievement_score, 10, 2)                    AS achievement_score,
        TRY_TO_DECIMAL(improvement_score, 10, 2)                    AS improvement_score,
        TRY_TO_DECIMAL(performance_score, 10, 2)                    AS performance_score,
        TRY_TO_DECIMAL(incentive_payment_multiplier, 10, 6)         AS incentive_payment_multiplier,

        -- ══════════════════════════════════════════════
        -- Ranking
        -- ══════════════════════════════════════════════
        TRY_TO_NUMBER(snf_vbp_program_ranking)                      AS vbp_ranking

    FROM deduplicated
    WHERE _row_num = 1
)

SELECT *
FROM cleaned
WHERE ccn IS NOT NULL
