{{ config(materialized='view') }}

{#
    Gold Fact: Quality and Readmission Analysis.
    Answers Metrics:
    - 3.3: Risk-standardized readmission rates (Facility vs State/National)
    - 3.5: Correlation between nurse staffing levels and readmission rates

    Updates:
    - Switched to INNER JOIN to dim_providers to fix 170 orphan records
      that exist in the VBP dataset but not in the main Provider dataset.
#}

WITH vbp AS (
    SELECT * FROM {{ ref('silver_snf_vbp_facility') }}
),

providers AS (
    SELECT 
        ccn, 
        reported_total_nurse_hours_prd, 
        state_code 
    FROM {{ ref('silver_providers') }}
),

state_benchmarks AS (
    SELECT * FROM {{ ref('silver_state_averages') }}
    WHERE state_or_nation != 'NATION'
),

national_benchmarks AS (
    SELECT * FROM {{ ref('silver_state_averages') }}
    WHERE state_or_nation = 'NATION'
),

final AS (
    SELECT
        -- Primary Key
        v.ccn,

        -- Facility Readmission Metrics (Metric 3.3)
        v.performance_readmission_rate,
        v.baseline_readmission_rate,
        v.performance_score,
        v.vbp_ranking,

        -- State Benchmarks
        s.pct_short_stay_rehospitalized                          AS state_avg_short_stay_rehospitalized,
        s.pct_short_stay_ed_visit                                 AS state_avg_short_stay_ed_visit,

        -- National Benchmarks
        n.pct_short_stay_rehospitalized                          AS national_avg_short_stay_rehospitalized,

        -- Joined Staffing for Correlation (Metric 3.5)
        p.reported_total_nurse_hours_prd,

        -- Comparison Metrics (Metric 3.3 vs Benchmarks)
        CASE
            WHEN s.pct_short_stay_rehospitalized > 0 
            THEN ROUND(v.performance_readmission_rate / s.pct_short_stay_rehospitalized, 4)
            ELSE NULL
        END                                                         AS readmission_to_state_avg_ratio,

        CASE
            WHEN n.pct_short_stay_rehospitalized > 0 
            THEN ROUND(v.performance_readmission_rate / n.pct_short_stay_rehospitalized, 4)
            ELSE NULL
        END                                                         AS readmission_to_national_avg_ratio

    FROM vbp v
    INNER JOIN {{ ref('dim_providers') }} d ON v.ccn = d.ccn
    LEFT JOIN providers p ON v.ccn = p.ccn
    LEFT JOIN state_benchmarks s ON p.state_code = s.state_or_nation
    CROSS JOIN national_benchmarks n
)

SELECT * FROM final
