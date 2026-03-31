{{ config(materialized='view') }}

WITH source_data AS (
    SELECT * FROM {{ source('healthcare', 'FY_2024_SNF_VBP_AGGREGATE_PERFORMANCE') }}
),

renamed AS (
    SELECT
        "Baseline Period: FY 2019 National Average Readmission Rate" AS baseline_period_fy_2019_national_average_readmission_rate,
        "Performance Period: FY 2022 National Average Readmission Rate" AS performance_period_fy_2022_national_average_readmission_rate,
        "FY 2024 Achievement Threshold"                    AS fy_2024_achievement_threshold,
        "FY 2024 Benchmark"                                AS fy_2024_benchmark,
        "Range of Performance Scores"                      AS range_of_performance_scores,
        "Total Number of SNFs Receiving Value-Based Incentive Payments" AS total_number_of_sn_fs_receiving_value_based_incentive_payments,
        "Range of Incentive Payment Multipliers"           AS range_of_incentive_payment_multipliers,
        "Range of Value-Based Incentive Payments ($)"      AS range_of_value_based_incentive_payments,
        "Total Amount of Value-Based Incentive Payments ($)" AS total_amount_of_value_based_incentive_payments
    FROM source_data
)

SELECT * FROM renamed