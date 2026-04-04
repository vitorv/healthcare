{{ config(materialized='view') }}

{#
    Gold Dimension: Central Provider (SNF) Master Table.
    
    BEST PRACTICES APPLIED:
    1. Logical Grouping: Organizational structure for BI readability.
    2. Derived Columns: Pre-calculated strings like full_address.
    3. Standardization: Consistent naming and value formatting.
    4. Metadata: Preservation of processing dates for lineage.
#}

WITH providers AS (
    SELECT * FROM {{ ref('silver_providers') }}
),

final AS (
    SELECT
        -- ══════════════════════════════════════════════
        -- Primary Key
        -- ══════════════════════════════════════════════
        ccn,

        -- ══════════════════════════════════════════════
        -- Provider Demographics (Grouping for Ease of Use)
        -- ══════════════════════════════════════════════
        provider_name,
        provider_address,
        city,
        state_code,
        zip_code,
        county,
        -- Quality of Life: Pre-concatenated address for display in reports
        CONCAT(provider_address, ', ', city, ', ', state_code, ' ', zip_code) AS provider_full_address,
        phone_number,

        -- ══════════════════════════════════════════════
        -- Facility Classification & Ownership
        -- ══════════════════════════════════════════════
        ownership_type,
        provider_type,
        is_hospital_based,
        is_ccrc,
        certified_bed_count,

        CASE 
            WHEN certified_bed_count <= 50 THEN 'Small (0-50 beds)'
            WHEN certified_bed_count <= 120 THEN 'Medium (51-120 beds)'
            ELSE 'Large (121+ beds)'
        END                                                         AS facility_size_category,

        -- ══════════════════════════════════════════════
        -- CMS Quality Benchmarks
        -- ══════════════════════════════════════════════
        overall_rating,
        -- Quality of Life: Text-based label for better charting
        CASE 
            WHEN overall_rating IS NULL THEN 'Not Rated'
            ELSE CONCAT(CAST(overall_rating AS VARCHAR), ' Stars')
        END                                                         AS overall_rating_label,
        
        health_inspection_rating,
        quality_measure_rating,
        staffing_rating,

        -- ══════════════════════════════════════════════
        -- Geospatial (For Mapping Tools)
        -- ══════════════════════════════════════════════
        latitude,
        longitude,

        -- ══════════════════════════════════════════════
        -- Record Metadata
        -- ══════════════════════════════════════════════
        processing_date                                             AS data_snapshot_date

    FROM providers
)

SELECT * FROM final
