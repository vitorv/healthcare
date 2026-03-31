{{ config(materialized='view') }}

WITH source_data AS (
    SELECT * FROM {{ source('healthcare', 'NH_STATEUSAVERAGES_OCT2024') }}
),

renamed AS (
    SELECT
        "State or Nation"                                  AS state_or_nation,
        "Cycle 1 Total Number of Health Deficiencies"      AS cycle_1_total_number_of_health_deficiencies,
        "Cycle 1 Total Number of Fire Safety Deficiencies" AS cycle_1_total_number_of_fire_safety_deficiencies,
        "Cycle 2 Total Number of Health Deficiencies"      AS cycle_2_total_number_of_health_deficiencies,
        "Cycle 2 Total Number of Fire Safety Deficiencies" AS cycle_2_total_number_of_fire_safety_deficiencies,
        "Cycle 3 Total Number of Health Deficiencies"      AS cycle_3_total_number_of_health_deficiencies,
        "Cycle 3 Total Number of Fire Safety Deficiencies" AS cycle_3_total_number_of_fire_safety_deficiencies,
        "Average Number of Residents per Day"              AS average_number_of_residents_per_day,
        "Reported Nurse Aide Staffing Hours per Resident per Day" AS reported_nurse_aide_staffing_hours_per_resident_per_day,
        "Reported LPN Staffing Hours per Resident per Day" AS reported_lpn_staffing_hours_per_resident_per_day,
        "Reported RN Staffing Hours per Resident per Day"  AS reported_rn_staffing_hours_per_resident_per_day,
        "Reported Licensed Staffing Hours per Resident per Day" AS reported_licensed_staffing_hours_per_resident_per_day,
        "Reported Total Nurse Staffing Hours per Resident per Day" AS reported_total_nurse_staffing_hours_per_resident_per_day,
        "Total number of nurse staff hours per resident per day on the weekend" AS total_number_of_nurse_staff_hours_per_resident_per_day_on_the_weekend,
        "Registered Nurse hours per resident per day on the weekend" AS registered_nurse_hours_per_resident_per_day_on_the_weekend,
        "Reported Physical Therapist Staffing Hours per Resident Per Day" AS reported_physical_therapist_staffing_hours_per_resident_per_day,
        "Total nursing staff turnover"                     AS total_nursing_staff_turnover,
        "Registered Nurse turnover"                        AS registered_nurse_turnover,
        "Number of administrators who have left the nursing home" AS number_of_administrators_who_have_left_the_nursing_home,
        "Nursing Case-Mix Index"                           AS nursing_case_mix_index,
        "Case-Mix RN Staffing Hours per Resident per Day"  AS case_mix_rn_staffing_hours_per_resident_per_day,
        "Case-Mix Total Nurse Staffing Hours per Resident per Day" AS case_mix_total_nurse_staffing_hours_per_resident_per_day,
        "Case-Mix Weekend Total Nurse Staffing Hours per Resident per Day" AS case_mix_weekend_total_nurse_staffing_hours_per_resident_per_day,
        "Number of Fines"                                  AS number_of_fines,
        "Fine Amount in Dollars"                           AS fine_amount_in_dollars,
        "Percentage of long stay residents whose need for help with daily activities has increased" AS percentage_of_long_stay_residents_whose_need_for_help_with_daily_activities_has_increased,
        "Percentage of long stay residents who lose too much weight" AS percentage_of_long_stay_residents_who_lose_too_much_weight,
        "Percentage of low risk long stay residents who lose control of their bowels or bladder" AS percentage_of_low_risk_long_stay_residents_who_lose_control_of_their_bowels_or_bladder,
        "Percentage of long stay residents with a catheter inserted and left in their bladder" AS percentage_of_long_stay_residents_with_a_catheter_inserted_and_left_in_their_bladder,
        "Percentage of long stay residents with a urinary tract infection" AS percentage_of_long_stay_residents_with_a_urinary_tract_infection,
        "Percentage of long stay residents who have depressive symptoms" AS percentage_of_long_stay_residents_who_have_depressive_symptoms,
        "Percentage of long stay residents who were physically restrained" AS percentage_of_long_stay_residents_who_were_physically_restrained,
        "Percentage of long stay residents experiencing one or more falls with major injury" AS percentage_of_long_stay_residents_experiencing_one_or_more_falls_with_major_injury,
        "Percentage of long stay residents assessed and appropriately given the pneumococcal vaccine" AS percentage_of_long_stay_residents_assessed_and_appropriately_given_the_pneumococcal_vaccine,
        "Percentage of long stay residents who received an antipsychotic medication" AS percentage_of_long_stay_residents_who_received_an_antipsychotic_medication,
        "Percentage of short stay residents assessed and appropriately given the pneumococcal vaccine" AS percentage_of_short_stay_residents_assessed_and_appropriately_given_the_pneumococcal_vaccine,
        "Percentage of short stay residents who newly received an antipsychotic medication" AS percentage_of_short_stay_residents_who_newly_received_an_antipsychotic_medication,
        "Percentage of long stay residents whose ability to move independently worsened" AS percentage_of_long_stay_residents_whose_ability_to_move_independently_worsened,
        "Percentage of long stay residents who received an antianxiety or hypnotic medication" AS percentage_of_long_stay_residents_who_received_an_antianxiety_or_hypnotic_medication,
        "Percentage of high risk long stay residents with pressure ulcers" AS percentage_of_high_risk_long_stay_residents_with_pressure_ulcers,
        "Percentage of long stay residents assessed and appropriately given the seasonal influenza vaccine" AS percentage_of_long_stay_residents_assessed_and_appropriately_given_the_seasonal_influenza_vaccine,
        "Percentage of short stay residents who made improvements in function" AS percentage_of_short_stay_residents_who_made_improvements_in_function,
        "Percentage of short stay residents who were assessed and appropriately given the seasonal influenza vaccine" AS percentage_of_short_stay_residents_who_were_assessed_and_appropriately_given_the_seasonal_influenza_vaccine,
        "Percentage of short stay residents who were rehospitalized after a nursing home admission" AS percentage_of_short_stay_residents_who_were_rehospitalized_after_a_nursing_home_admission,
        "Percentage of short stay residents who had an outpatient emergency department visit" AS percentage_of_short_stay_residents_who_had_an_outpatient_emergency_department_visit,
        "Number of hospitalizations per 1000 long-stay resident days" AS number_of_hospitalizations_per_1000_long_stay_resident_days,
        "Number of outpatient emergency department visits per 1000 long-stay resident days" AS number_of_outpatient_emergency_department_visits_per_1000_long_stay_resident_days,
        "Processing Date"                                  AS processing_date
    FROM source_data
)

SELECT * FROM renamed