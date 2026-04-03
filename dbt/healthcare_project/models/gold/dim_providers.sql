{{ config(materialized='view') }}

{#
    Gold Layer: 
    │   • Facility attributes for grouping/filtering
    │   • state_code, ownership_type, provider_type, is_hospital_based
    │   • certified_bed_count, overall_rating

    Source: silver_providers
#}

SELECT
    ccn,
    provider_name,
    provider_address,
    city,
    state_code,
    ownership_type,
    provider_type,
    is_hospital_based,
    certified_bed_count,
    overall_rating,
FROM source
