{{ config(materialized='table') }}

{#
    Silver Layer: Cleaned and typed state/national average benchmarks.
    One row per state (plus one "Nation" row).

    Source: stg_nh_stateusaverages_oct2024

    6 Core Responsibilities:
      1. Data type casting — TRY_TO_* safe casts (Bronze is all VARCHAR)
      2. Null / default handling — implicit via TRY_TO_*
      3. Deduplication — ROW_NUMBER on state_or_nation
      4. Business-key integrity — WHERE state_or_nation IS NOT NULL
      5. Column pruning — 48 cols → ~30 (fire safety deficiency cols removed)
      6. Light standardization — UPPER TRIM on state

    Supports metrics: 1.1 (benchmarks), 2.5, 3.3, v1-Q2 (benchmarks)
#}

WITH source AS (
    SELECT * FROM {{ ref('stg_nh_stateusaverages_oct2024') }}
),

deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY state_or_nation
            ORDER BY processing_date DESC
        ) AS _row_num
    FROM source
),

cleaned AS (
    SELECT
        -- ══════════════════════════════════════════════
        -- Primary Key
        -- ══════════════════════════════════════════════
        UPPER(TRIM(state_or_nation))                                AS state_or_nation,

        -- ══════════════════════════════════════════════
        -- Average Facility Size
        -- ══════════════════════════════════════════════
        TRY_TO_DECIMAL(average_number_of_residents_per_day, 10, 1)  AS avg_daily_residents,

        -- ══════════════════════════════════════════════
        -- Staffing Hours per Resident per Day (benchmarks for Metric 1.1)
        -- ══════════════════════════════════════════════
        TRY_TO_DECIMAL(reported_nurse_aide_staffing_hours_per_resident_per_day, 10, 2)
            AS avg_cna_hours_prd,
        TRY_TO_DECIMAL(reported_lpn_staffing_hours_per_resident_per_day, 10, 2)
            AS avg_lpn_hours_prd,
        TRY_TO_DECIMAL(reported_rn_staffing_hours_per_resident_per_day, 10, 2)
            AS avg_rn_hours_prd,
        TRY_TO_DECIMAL(reported_licensed_staffing_hours_per_resident_per_day, 10, 2)
            AS avg_licensed_hours_prd,
        TRY_TO_DECIMAL(reported_total_nurse_staffing_hours_per_resident_per_day, 10, 2)
            AS avg_total_nurse_hours_prd,
        TRY_TO_DECIMAL(total_number_of_nurse_staff_hours_per_resident_per_day_on_the_weekend, 10, 2)
            AS avg_weekend_total_nurse_hours_prd,
        TRY_TO_DECIMAL(registered_nurse_hours_per_resident_per_day_on_the_weekend, 10, 2)
            AS avg_weekend_rn_hours_prd,
        TRY_TO_DECIMAL(reported_physical_therapist_staffing_hours_per_resident_per_day, 10, 2)
            AS avg_pt_hours_prd,

        -- ══════════════════════════════════════════════
        -- Turnover Benchmarks (Metric v1-Q2)
        -- ══════════════════════════════════════════════
        TRY_TO_DECIMAL(total_nursing_staff_turnover, 10, 2)         AS avg_total_nurse_turnover_pct,
        TRY_TO_DECIMAL(registered_nurse_turnover, 10, 2)            AS avg_rn_turnover_pct,
        TRY_TO_DECIMAL(number_of_administrators_who_have_left_the_nursing_home, 10, 2)
            AS avg_administrator_departures,

        -- ══════════════════════════════════════════════
        -- Case-Mix Adjusted Staffing
        -- ══════════════════════════════════════════════
        TRY_TO_DECIMAL(nursing_case_mix_index, 10, 4)               AS avg_nursing_case_mix_index,
        TRY_TO_DECIMAL(case_mix_rn_staffing_hours_per_resident_per_day, 10, 2)
            AS avg_casemix_rn_hours_prd,
        TRY_TO_DECIMAL(case_mix_total_nurse_staffing_hours_per_resident_per_day, 10, 2)
            AS avg_casemix_total_nurse_hours_prd,

        -- ══════════════════════════════════════════════
        -- Rehospitalization / Readmission (Metric 3.3)
        -- ══════════════════════════════════════════════
        TRY_TO_DECIMAL(percentage_of_short_stay_residents_who_were_rehospitalized_after_a_nursing_home_admission, 10, 2)
            AS pct_short_stay_rehospitalized,
        TRY_TO_DECIMAL(percentage_of_short_stay_residents_who_had_an_outpatient_emergency_department_visit, 10, 2)
            AS pct_short_stay_ed_visit,
        TRY_TO_DECIMAL(number_of_hospitalizations_per_1000_long_stay_resident_days, 10, 2)
            AS hospitalizations_per_1000_long_stay_days,
        TRY_TO_DECIMAL(number_of_outpatient_emergency_department_visits_per_1000_long_stay_resident_days, 10, 2)
            AS ed_visits_per_1000_long_stay_days,

        -- ══════════════════════════════════════════════
        -- Key Quality Indicators (for enrichment)
        -- ══════════════════════════════════════════════
        TRY_TO_DECIMAL(percentage_of_long_stay_residents_who_received_an_antipsychotic_medication, 10, 2)
            AS pct_long_stay_antipsychotic,
        TRY_TO_DECIMAL(percentage_of_long_stay_residents_experiencing_one_or_more_falls_with_major_injury, 10, 2)
            AS pct_long_stay_falls_major_injury,
        TRY_TO_DECIMAL(percentage_of_high_risk_long_stay_residents_with_pressure_ulcers, 10, 2)
            AS pct_high_risk_pressure_ulcers,
        TRY_TO_DECIMAL(percentage_of_short_stay_residents_who_made_improvements_in_function, 10, 2)
            AS pct_short_stay_functional_improvement,

        -- ══════════════════════════════════════════════
        -- Penalties Summary
        -- ══════════════════════════════════════════════
        TRY_TO_DECIMAL(number_of_fines, 10, 1)                     AS avg_fine_count,
        TRY_TO_NUMBER(fine_amount_in_dollars)                       AS avg_fine_amount,

        -- ══════════════════════════════════════════════
        -- Health Deficiencies (kept for context, fire safety pruned)
        -- ══════════════════════════════════════════════
        TRY_TO_DECIMAL(cycle_1_total_number_of_health_deficiencies, 10, 1)
            AS avg_cycle_1_health_deficiencies,
        TRY_TO_DECIMAL(cycle_2_total_number_of_health_deficiencies, 10, 1)
            AS avg_cycle_2_health_deficiencies,
        TRY_TO_DECIMAL(cycle_3_total_number_of_health_deficiencies, 10, 1)
            AS avg_cycle_3_health_deficiencies,

        -- ══════════════════════════════════════════════
        -- Metadata
        -- ══════════════════════════════════════════════
        TRY_TO_DATE(processing_date)                                AS processing_date

    FROM deduplicated
    WHERE _row_num = 1
)

SELECT *
FROM cleaned
WHERE state_or_nation IS NOT NULL
