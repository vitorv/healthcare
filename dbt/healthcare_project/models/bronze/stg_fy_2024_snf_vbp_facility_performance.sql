{{ config(materialized='view') }}

WITH source_data AS (
    SELECT * FROM {{ source('healthcare', 'FY_2024_SNF_VBP_FACILITY_PERFORMANCE') }}
),

renamed AS (
    SELECT
        "SNF VBP Program Ranking"                          AS snf_vbp_program_ranking,
        "Footnote -- SNF VBP Program Ranking"              AS footnote_snf_vbp_program_ranking,
        "CMS Certification Number (CCN)"                   AS cms_certification_number_ccn,
        "Provider Name"                                    AS provider_name,
        "Provider Address"                                 AS provider_address,
        "City/Town"                                        AS city_town,
        "State"                                            AS state,
        "ZIP Code"                                         AS zip_code,
        "Baseline Period: FY 2019 Risk-Standardized Readmission Rate" AS baseline_period_fy_2019_risk_standardized_readmission_rate,
        "Footnote -- Baseline Period: FY 2019 Risk-Standardized Readmission Rate" AS footnote_baseline_period_fy_2019_risk_standardized_readmission_rate,
        "Performance Period: FY 2022 Risk-Standardized Readmission Rate" AS performance_period_fy_2022_risk_standardized_readmission_rate,
        "Footnote -- Performance Period: FY 2022 Risk-Standardized Readmission Rate" AS footnote_performance_period_fy_2022_risk_standardized_readmission_rate,
        "Achievement Score"                                AS achievement_score,
        "Footnote -- Achievement Score"                    AS footnote_achievement_score,
        "Improvement Score"                                AS improvement_score,
        "Footnote -- Improvement Score"                    AS footnote_improvement_score,
        "Performance Score"                                AS performance_score,
        "Footnote -- Performance Score"                    AS footnote_performance_score,
        "Incentive Payment Multiplier"                     AS incentive_payment_multiplier,
        "Footnote -- Incentive Payment Multiplier"         AS footnote_incentive_payment_multiplier
    FROM source_data
)

SELECT * FROM renamed