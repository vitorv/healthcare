{{ config(materialized='view') }}

WITH source_data AS (
    SELECT * FROM {{ source('healthcare', 'NH_COVIDVAXAVERAGES_20241027') }}
),

renamed AS (
    SELECT
        "State"                                            AS state,
        "Percent of residents who are up-to-date on their vaccines" AS percent_of_residents_who_are_up_to_date_on_their_vaccines,
        "Percent of staff who are up-to-date on their vaccines" AS percent_of_staff_who_are_up_to_date_on_their_vaccines,
        "Date vaccination data last updated"               AS date_vaccination_data_last_updated
    FROM source_data
)

SELECT * FROM renamed