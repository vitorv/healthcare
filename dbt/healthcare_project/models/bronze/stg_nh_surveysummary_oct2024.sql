{{ config(materialized='view') }}

WITH source_data AS (
    SELECT * FROM {{ source('healthcare', 'NH_SURVEYSUMMARY_OCT2024') }}
),

renamed AS (
    SELECT
        "CMS Certification Number (CCN)"                   AS cms_certification_number_ccn,
        "Provider Name"                                    AS provider_name,
        "Provider Address"                                 AS provider_address,
        "City/Town"                                        AS city_town,
        "State"                                            AS state,
        "ZIP Code"                                         AS zip_code,
        "Inspection Cycle"                                 AS inspection_cycle,
        "Health Survey Date"                               AS health_survey_date,
        "Fire Safety Survey Date"                          AS fire_safety_survey_date,
        "Total Number of Health Deficiencies"              AS total_number_of_health_deficiencies,
        "Total Number of Fire Safety Deficiencies"         AS total_number_of_fire_safety_deficiencies,
        "Count of Freedom from Abuse and Neglect and Exploitation Deficiencies" AS count_of_freedom_from_abuse_and_neglect_and_exploitation_deficiencies,
        "Count of Quality of Life and Care Deficiencies"   AS count_of_quality_of_life_and_care_deficiencies,
        "Count of Resident Assessment and Care Planning Deficiencies" AS count_of_resident_assessment_and_care_planning_deficiencies,
        "Count of Nursing and Physician Services Deficiencies" AS count_of_nursing_and_physician_services_deficiencies,
        "Count of Resident Rights Deficiencies"            AS count_of_resident_rights_deficiencies,
        "Count of Nutrition and Dietary Deficiencies"      AS count_of_nutrition_and_dietary_deficiencies,
        "Count of Pharmacy Service Deficiencies"           AS count_of_pharmacy_service_deficiencies,
        "Count of Environmental Deficiencies"              AS count_of_environmental_deficiencies,
        "Count of Administration Deficiencies"             AS count_of_administration_deficiencies,
        "Count of Infection Control Deficiencies"          AS count_of_infection_control_deficiencies,
        "Count of Emergency Preparedness Deficiencies"     AS count_of_emergency_preparedness_deficiencies,
        "Count of Automatic Sprinkler Systems Deficiencies" AS count_of_automatic_sprinkler_systems_deficiencies,
        "Count of Construction Deficiencies"               AS count_of_construction_deficiencies,
        "Count of Services Deficiencies"                   AS count_of_services_deficiencies,
        "Count of Corridor Walls and Doors Deficiencies"   AS count_of_corridor_walls_and_doors_deficiencies,
        "Count of Egress Deficiencies"                     AS count_of_egress_deficiencies,
        "Count of Electrical Deficiencies"                 AS count_of_electrical_deficiencies,
        "Count of Emergency Plans and Fire Drills Deficiencies" AS count_of_emergency_plans_and_fire_drills_deficiencies,
        "Count of Fire Alarm Systems Deficiencies"         AS count_of_fire_alarm_systems_deficiencies,
        "Count of Smoke Deficiencies"                      AS count_of_smoke_deficiencies,
        "Count of Interior Deficiencies"                   AS count_of_interior_deficiencies,
        "Count of Gas and Vacuum and Electrical Systems Deficiencies" AS count_of_gas_and_vacuum_and_electrical_systems_deficiencies,
        "Count of Hazardous Area Deficiencies"             AS count_of_hazardous_area_deficiencies,
        "Count of Illumination and Emergency Power Deficiencies" AS count_of_illumination_and_emergency_power_deficiencies,
        "Count of Laboratories Deficiencies"               AS count_of_laboratories_deficiencies,
        "Count of Medical Gases and Anaesthetizing Areas Deficiencies" AS count_of_medical_gases_and_anaesthetizing_areas_deficiencies,
        "Count of Smoking Regulations Deficiencies"        AS count_of_smoking_regulations_deficiencies,
        "Count of Miscellaneous Deficiencies"              AS count_of_miscellaneous_deficiencies,
        "Location"                                         AS location,
        "Processing Date"                                  AS processing_date
    FROM source_data
)

SELECT * FROM renamed