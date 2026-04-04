{{ config(materialized='view') }}

{#
    Gold Fact: Turnover Analysis.
    Answers Metric:
    - v1-Q2: Nurse staff turnover rates and correlation between turnover and staffing levels.

    Updates:
    - Switched to INNER JOIN for Star Schema consistency and referential integrity.
#}

WITH providers AS (
    SELECT 
        ccn, 
        state_code, 
        total_nurse_turnover_pct, 
        rn_turnover_pct, 
        administrator_departures, 
        reported_total_nurse_hours_prd 
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
        p.ccn,

        -- Facility Turnover Metrics (Metric v1-Q2)
        p.total_nurse_turnover_pct,
        p.rn_turnover_pct,
        p.administrator_departures,

        -- Facility Staffing (for correlation analysis)
        p.reported_total_nurse_hours_prd,

        -- State Benchmarks
        s.avg_total_nurse_turnover_pct                          AS state_avg_total_turnover_pct,
        s.avg_rn_turnover_pct                                    AS state_avg_rn_turnover_pct,
        s.avg_administrator_departures                          AS state_avg_administrator_departures,

        -- National Benchmarks
        n.avg_total_nurse_turnover_pct                          AS national_avg_total_nurse_turnover_pct,
        n.avg_rn_turnover_pct                                    AS national_avg_rn_turnover_pct,

        -- Comparison Metrics (Facility vs Average)
        CASE
            WHEN s.avg_total_nurse_turnover_pct > 0 
            THEN ROUND(p.total_nurse_turnover_pct / s.avg_total_nurse_turnover_pct, 4)
            ELSE NULL
        END                                                         AS turnover_to_state_avg_ratio,

        CASE
            WHEN n.avg_total_nurse_turnover_pct > 0 
            THEN ROUND(p.total_nurse_turnover_pct / n.avg_total_nurse_turnover_pct, 4)
            ELSE NULL
        END                                                         AS turnover_to_national_avg_ratio

    FROM providers p
    INNER JOIN {{ ref('dim_providers') }} d ON p.ccn = d.ccn
    LEFT JOIN state_benchmarks s ON p.state_code = s.state_or_nation
    CROSS JOIN national_benchmarks n
)

SELECT * FROM final
