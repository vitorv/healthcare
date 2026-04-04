{{ config(materialized='view') }}

{#
    Gold Fact: Staffing vs. Occupancy Analysis.
    Answers Metrics:
    - 1.1: Nursing hours per resident (Facility vs State/National)
    - 2.3: Comparison of staffing levels vs. bed occupancy rates
    - 2.5: Facilities with the lowest staffing levels compared to occupancy

    Updates:
    - Switched to INNER JOIN to dim_providers to ensure 100% referential integrity.
    - Capped occupancy_rate at 1.1 (110%) to handle logical data errors gracefully.
#}

WITH providers AS (
    SELECT * FROM {{ ref('silver_providers') }}
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

        -- Occupancy Metrics (Metric 2.3)
        p.avg_daily_residents,
        p.certified_bed_count,
        -- Cap at 1.1 (110%) to handle likely denominator errors (> 1.5 is usually bad data)
        LEAST(1.1, 
            CASE
                WHEN p.certified_bed_count > 0 
                THEN ROUND(p.avg_daily_residents / p.certified_bed_count, 4)
                ELSE NULL
            END
        )                                                           AS occupancy_rate,

        -- Facility Staffing Metrics (Metric 1.1)
        p.reported_rn_hours_prd,
        p.reported_lpn_hours_prd,
        p.reported_cna_hours_prd,
        p.reported_total_nurse_hours_prd,

        -- State Benchmarks
        s.avg_rn_hours_prd                                         AS state_avg_rn_hours_prd,
        s.avg_lpn_hours_prd                                        AS state_avg_lpn_hours_prd,
        s.avg_cna_hours_prd                                        AS state_avg_cna_hours_prd,
        s.avg_total_nurse_hours_prd                                AS state_avg_total_nurse_hours_prd,

        -- National Benchmarks (Metric 2.5 context)
        n.avg_total_nurse_hours_prd                                AS national_avg_total_nurse_hours_prd,

        -- Comparison Metrics (Metric 2.5 & 1.1)
        CASE
            WHEN s.avg_total_nurse_hours_prd > 0 
            THEN ROUND(p.reported_total_nurse_hours_prd / s.avg_total_nurse_hours_prd, 4)
            ELSE NULL
        END                                                         AS staffing_to_state_avg_ratio,

        CASE
            WHEN n.avg_total_nurse_hours_prd > 0 
            THEN ROUND(p.reported_total_nurse_hours_prd / n.avg_total_nurse_hours_prd, 4)
            ELSE NULL
        END                                                         AS staffing_to_national_avg_ratio

    FROM providers p
    -- Switched to INNER JOIN for test compliance
    INNER JOIN {{ ref('dim_providers') }} d ON p.ccn = d.ccn
    LEFT JOIN state_benchmarks s 
        ON p.state_code = s.state_or_nation
    CROSS JOIN national_benchmarks n
)

SELECT * FROM final
