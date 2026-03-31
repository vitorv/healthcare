{{ config(materialized='view') }}

WITH source_data AS (
    SELECT * FROM {{ source('healthcare', 'NH_DATACOLLECTIONINTERVALS_OCT2024') }}
),

renamed AS (
    SELECT
        "Measure Code"                                     AS measure_code,
        "Measure Description"                              AS measure_description,
        "Data Collection Period From Date"                 AS data_collection_period_from_date,
        "Data Collection Period Through Date"              AS data_collection_period_through_date,
        "Measure Date Range"                               AS measure_date_range,
        "Processing Date"                                  AS processing_date
    FROM source_data
)

SELECT * FROM renamed