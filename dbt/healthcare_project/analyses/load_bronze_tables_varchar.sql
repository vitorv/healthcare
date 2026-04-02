-- ============================================================
-- Bronze Layer: VARCHAR-Only Table Creation & Data Loading
-- ============================================================
-- This script recreates all 20 raw tables with ALL columns as VARCHAR.
-- This follows medallion architecture best practice: Bronze stores raw data
-- exactly as it arrives, with zero type inference. Type casting is the
-- responsibility of the Silver layer.
--
-- IMPORTANT: This will DROP and RECREATE all raw tables.
-- Data will need to be reloaded from the S3 stage.
-- ============================================================

USE SCHEMA HEALTHCARE.BRONZE;

-- Table 1: FY_2024_SNF_VBP_AGGREGATE_PERFORMANCE
-- Source CSV: FY_2024_SNF_VBP_Aggregate_Performance.csv
-- Columns: 9
CREATE OR REPLACE TABLE FY_2024_SNF_VBP_AGGREGATE_PERFORMANCE (
    "Baseline Period: FY 2019 National Average Readmission Rate" VARCHAR,
    "Performance Period: FY 2022 National Average Readmission Rate" VARCHAR,
    "FY 2024 Achievement Threshold" VARCHAR,
    "FY 2024 Benchmark" VARCHAR,
    "Range of Performance Scores" VARCHAR,
    "Total Number of SNFs Receiving Value-Based Incentive Payments" VARCHAR,
    "Range of Incentive Payment Multipliers" VARCHAR,
    "Range of Value-Based Incentive Payments ($)" VARCHAR,
    "Total Amount of Value-Based Incentive Payments ($)" VARCHAR
);

COPY INTO FY_2024_SNF_VBP_AGGREGATE_PERFORMANCE
FROM @HEALTHCARE_S3_STAGE/FY_2024_SNF_VBP_Aggregate_Performance.csv
FILE_FORMAT = (FORMAT_NAME = 'MY_CSV_FORMAT')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- Table 2: FY_2024_SNF_VBP_FACILITY_PERFORMANCE
-- Source CSV: FY_2024_SNF_VBP_Facility_Performance.csv
-- Columns: 20
CREATE OR REPLACE TABLE FY_2024_SNF_VBP_FACILITY_PERFORMANCE (
    "SNF VBP Program Ranking" VARCHAR,
    "Footnote -- SNF VBP Program Ranking" VARCHAR,
    "CMS Certification Number (CCN)" VARCHAR,
    "Provider Name" VARCHAR,
    "Provider Address" VARCHAR,
    "City/Town" VARCHAR,
    "State" VARCHAR,
    "ZIP Code" VARCHAR,
    "Baseline Period: FY 2019 Risk-Standardized Readmission Rate" VARCHAR,
    "Footnote -- Baseline Period: FY 2019 Risk-Standardized Readmission Rate" VARCHAR,
    "Performance Period: FY 2022 Risk-Standardized Readmission Rate" VARCHAR,
    "Footnote -- Performance Period: FY 2022 Risk-Standardized Readmission Rate" VARCHAR,
    "Achievement Score" VARCHAR,
    "Footnote -- Achievement Score" VARCHAR,
    "Improvement Score" VARCHAR,
    "Footnote -- Improvement Score" VARCHAR,
    "Performance Score" VARCHAR,
    "Footnote -- Performance Score" VARCHAR,
    "Incentive Payment Multiplier" VARCHAR,
    "Footnote -- Incentive Payment Multiplier" VARCHAR
);

COPY INTO FY_2024_SNF_VBP_FACILITY_PERFORMANCE
FROM @HEALTHCARE_S3_STAGE/FY_2024_SNF_VBP_Facility_Performance.csv
FILE_FORMAT = (FORMAT_NAME = 'MY_CSV_FORMAT')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- Table 3: NH_CITATIONDESCRIPTIONS_OCT2024
-- Source CSV: NH_CitationDescriptions_Oct2024.csv
-- Columns: 5
CREATE OR REPLACE TABLE NH_CITATIONDESCRIPTIONS_OCT2024 (
    "Deficiency Prefix" VARCHAR,
    "Deficiency Tag Number" VARCHAR,
    "Deficiency Prefix and Number" VARCHAR,
    "Deficiency Description" VARCHAR,
    "Deficiency Category" VARCHAR
);

COPY INTO NH_CITATIONDESCRIPTIONS_OCT2024
FROM @HEALTHCARE_S3_STAGE/NH_CitationDescriptions_Oct2024.csv
FILE_FORMAT = (FORMAT_NAME = 'MY_CSV_FORMAT')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- Table 4: NH_COVIDVAXAVERAGES_20241027
-- Source CSV: NH_CovidVaxAverages_20241027.csv
-- Columns: 4
CREATE OR REPLACE TABLE NH_COVIDVAXAVERAGES_20241027 (
    "State" VARCHAR,
    "Percent of residents who are up-to-date on their vaccines" VARCHAR,
    "Percent of staff who are up-to-date on their vaccines" VARCHAR,
    "Date vaccination data last updated" VARCHAR
);

COPY INTO NH_COVIDVAXAVERAGES_20241027
FROM @HEALTHCARE_S3_STAGE/NH_CovidVaxAverages_20241027.csv
FILE_FORMAT = (FORMAT_NAME = 'MY_CSV_FORMAT')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- Table 5: NH_COVIDVAXPROVIDER_20241027
-- Source CSV: NH_CovidVaxProvider_20241027.csv
-- Columns: 5
CREATE OR REPLACE TABLE NH_COVIDVAXPROVIDER_20241027 (
    "CMS Certification Number (CCN)" VARCHAR,
    "State" VARCHAR,
    "Percent of residents who are up-to-date on their vaccines" VARCHAR,
    "Percent of staff who are up-to-date on their vaccines" VARCHAR,
    "Date vaccination data last updated" VARCHAR
);

COPY INTO NH_COVIDVAXPROVIDER_20241027
FROM @HEALTHCARE_S3_STAGE/NH_CovidVaxProvider_20241027.csv
FILE_FORMAT = (FORMAT_NAME = 'MY_CSV_FORMAT')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- Table 6: NH_DATACOLLECTIONINTERVALS_OCT2024
-- Source CSV: NH_DataCollectionIntervals_Oct2024.csv
-- Columns: 6
CREATE OR REPLACE TABLE NH_DATACOLLECTIONINTERVALS_OCT2024 (
    "Measure Code" VARCHAR,
    "Measure Description" VARCHAR,
    "Data Collection Period From Date" VARCHAR,
    "Data Collection Period Through Date" VARCHAR,
    "Measure Date Range" VARCHAR,
    "Processing Date" VARCHAR
);

COPY INTO NH_DATACOLLECTIONINTERVALS_OCT2024
FROM @HEALTHCARE_S3_STAGE/NH_DataCollectionIntervals_Oct2024.csv
FILE_FORMAT = (FORMAT_NAME = 'MY_CSV_FORMAT')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- Table 7: NH_FIRESAFETYCITATIONS_OCT2024
-- Source CSV: NH_FireSafetyCitations_Oct2024.csv
-- Columns: 24
CREATE OR REPLACE TABLE NH_FIRESAFETYCITATIONS_OCT2024 (
    "CMS Certification Number (CCN)" VARCHAR,
    "Provider Name" VARCHAR,
    "Provider Address" VARCHAR,
    "City/Town" VARCHAR,
    "State" VARCHAR,
    "ZIP Code" VARCHAR,
    "Survey Date" VARCHAR,
    "Survey Type" VARCHAR,
    "Deficiency Prefix" VARCHAR,
    "Deficiency Category" VARCHAR,
    "Deficiency Tag Number" VARCHAR,
    "Tag Version" VARCHAR,
    "Deficiency Description" VARCHAR,
    "Scope Severity Code" VARCHAR,
    "Deficiency Corrected" VARCHAR,
    "Correction Date" VARCHAR,
    "Inspection Cycle" VARCHAR,
    "Standard Deficiency" VARCHAR,
    "Complaint Deficiency" VARCHAR,
    "Infection Control Inspection Deficiency" VARCHAR,
    "Citation under IDR" VARCHAR,
    "Citation under IIDR" VARCHAR,
    "Location" VARCHAR,
    "Processing Date" VARCHAR
);

COPY INTO NH_FIRESAFETYCITATIONS_OCT2024
FROM @HEALTHCARE_S3_STAGE/NH_FireSafetyCitations_Oct2024.csv
FILE_FORMAT = (FORMAT_NAME = 'MY_CSV_FORMAT')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- Table 8: NH_HEALTHCITATIONS_OCT2024
-- Source CSV: NH_HealthCitations_Oct2024.csv
-- Columns: 23
CREATE OR REPLACE TABLE NH_HEALTHCITATIONS_OCT2024 (
    "CMS Certification Number (CCN)" VARCHAR,
    "Provider Name" VARCHAR,
    "Provider Address" VARCHAR,
    "City/Town" VARCHAR,
    "State" VARCHAR,
    "ZIP Code" VARCHAR,
    "Survey Date" VARCHAR,
    "Survey Type" VARCHAR,
    "Deficiency Prefix" VARCHAR,
    "Deficiency Category" VARCHAR,
    "Deficiency Tag Number" VARCHAR,
    "Deficiency Description" VARCHAR,
    "Scope Severity Code" VARCHAR,
    "Deficiency Corrected" VARCHAR,
    "Correction Date" VARCHAR,
    "Inspection Cycle" VARCHAR,
    "Standard Deficiency" VARCHAR,
    "Complaint Deficiency" VARCHAR,
    "Infection Control Inspection Deficiency" VARCHAR,
    "Citation under IDR" VARCHAR,
    "Citation under IIDR" VARCHAR,
    "Location" VARCHAR,
    "Processing Date" VARCHAR
);

COPY INTO NH_HEALTHCITATIONS_OCT2024
FROM @HEALTHCARE_S3_STAGE/NH_HealthCitations_Oct2024.csv
FILE_FORMAT = (FORMAT_NAME = 'MY_CSV_FORMAT')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- Table 9: NH_HLTHINSPECCUTPOINTSSTATE_OCT2024
-- Source CSV: NH_HlthInspecCutpointsState_Oct2024.csv
-- Columns: 6
CREATE OR REPLACE TABLE NH_HLTHINSPECCUTPOINTSSTATE_OCT2024 (
    "State" VARCHAR,
    "5 Stars" VARCHAR,
    "4 Stars" VARCHAR,
    "3 Stars" VARCHAR,
    "2 Stars" VARCHAR,
    "1 Star" VARCHAR
);

COPY INTO NH_HLTHINSPECCUTPOINTSSTATE_OCT2024
FROM @HEALTHCARE_S3_STAGE/NH_HlthInspecCutpointsState_Oct2024.csv
FILE_FORMAT = (FORMAT_NAME = 'MY_CSV_FORMAT')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- Table 10: NH_OWNERSHIP_OCT2024
-- Source CSV: NH_Ownership_Oct2024.csv
-- Columns: 13
CREATE OR REPLACE TABLE NH_OWNERSHIP_OCT2024 (
    "CMS Certification Number (CCN)" VARCHAR,
    "Provider Name" VARCHAR,
    "Provider Address" VARCHAR,
    "City/Town" VARCHAR,
    "State" VARCHAR,
    "ZIP Code" VARCHAR,
    "Role played by Owner or Manager in Facility" VARCHAR,
    "Owner Type" VARCHAR,
    "Owner Name" VARCHAR,
    "Ownership Percentage" VARCHAR,
    "Association Date" VARCHAR,
    "Location" VARCHAR,
    "Processing Date" VARCHAR
);

COPY INTO NH_OWNERSHIP_OCT2024
FROM @HEALTHCARE_S3_STAGE/NH_Ownership_Oct2024.csv
FILE_FORMAT = (FORMAT_NAME = 'MY_CSV_FORMAT')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- Table 11: NH_PENALTIES_OCT2024
-- Source CSV: NH_Penalties_Oct2024.csv
-- Columns: 13
CREATE OR REPLACE TABLE NH_PENALTIES_OCT2024 (
    "CMS Certification Number (CCN)" VARCHAR,
    "Provider Name" VARCHAR,
    "Provider Address" VARCHAR,
    "City/Town" VARCHAR,
    "State" VARCHAR,
    "ZIP Code" VARCHAR,
    "Penalty Date" VARCHAR,
    "Penalty Type" VARCHAR,
    "Fine Amount" VARCHAR,
    "Payment Denial Start Date" VARCHAR,
    "Payment Denial Length in Days" VARCHAR,
    "Location" VARCHAR,
    "Processing Date" VARCHAR
);

COPY INTO NH_PENALTIES_OCT2024
FROM @HEALTHCARE_S3_STAGE/NH_Penalties_Oct2024.csv
FILE_FORMAT = (FORMAT_NAME = 'MY_CSV_FORMAT')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- Table 12: NH_PROVIDERINFO_OCT2024
-- Source CSV: NH_ProviderInfo_Oct2024.csv
-- Columns: 103
CREATE OR REPLACE TABLE NH_PROVIDERINFO_OCT2024 (
    "CMS Certification Number (CCN)" VARCHAR,
    "Provider Name" VARCHAR,
    "Provider Address" VARCHAR,
    "City/Town" VARCHAR,
    "State" VARCHAR,
    "ZIP Code" VARCHAR,
    "Telephone Number" VARCHAR,
    "Provider SSA County Code" VARCHAR,
    "County/Parish" VARCHAR,
    "Ownership Type" VARCHAR,
    "Number of Certified Beds" VARCHAR,
    "Average Number of Residents per Day" VARCHAR,
    "Average Number of Residents per Day Footnote" VARCHAR,
    "Provider Type" VARCHAR,
    "Provider Resides in Hospital" VARCHAR,
    "Legal Business Name" VARCHAR,
    "Date First Approved to Provide Medicare and Medicaid Services" VARCHAR,
    "Affiliated Entity Name" VARCHAR,
    "Affiliated Entity ID" VARCHAR,
    "Continuing Care Retirement Community" VARCHAR,
    "Special Focus Status" VARCHAR,
    "Abuse Icon" VARCHAR,
    "Most Recent Health Inspection More Than 2 Years Ago" VARCHAR,
    "Provider Changed Ownership in Last 12 Months" VARCHAR,
    "With a Resident and Family Council" VARCHAR,
    "Automatic Sprinkler Systems in All Required Areas" VARCHAR,
    "Overall Rating" VARCHAR,
    "Overall Rating Footnote" VARCHAR,
    "Health Inspection Rating" VARCHAR,
    "Health Inspection Rating Footnote" VARCHAR,
    "QM Rating" VARCHAR,
    "QM Rating Footnote" VARCHAR,
    "Long-Stay QM Rating" VARCHAR,
    "Long-Stay QM Rating Footnote" VARCHAR,
    "Short-Stay QM Rating" VARCHAR,
    "Short-Stay QM Rating Footnote" VARCHAR,
    "Staffing Rating" VARCHAR,
    "Staffing Rating Footnote" VARCHAR,
    "Reported Staffing Footnote" VARCHAR,
    "Physical Therapist Staffing Footnote" VARCHAR,
    "Reported Nurse Aide Staffing Hours per Resident per Day" VARCHAR,
    "Reported LPN Staffing Hours per Resident per Day" VARCHAR,
    "Reported RN Staffing Hours per Resident per Day" VARCHAR,
    "Reported Licensed Staffing Hours per Resident per Day" VARCHAR,
    "Reported Total Nurse Staffing Hours per Resident per Day" VARCHAR,
    "Total number of nurse staff hours per resident per day on the weekend" VARCHAR,
    "Registered Nurse hours per resident per day on the weekend" VARCHAR,
    "Reported Physical Therapist Staffing Hours per Resident Per Day" VARCHAR,
    "Total nursing staff turnover" VARCHAR,
    "Total nursing staff turnover footnote" VARCHAR,
    "Registered Nurse turnover" VARCHAR,
    "Registered Nurse turnover footnote" VARCHAR,
    "Number of administrators who have left the nursing home" VARCHAR,
    "Administrator turnover footnote" VARCHAR,
    "Nursing Case-Mix Index" VARCHAR,
    "Nursing Case-Mix Index Ratio" VARCHAR,
    "Case-Mix Nurse Aide Staffing Hours per Resident per Day" VARCHAR,
    "Case-Mix LPN Staffing Hours per Resident per Day" VARCHAR,
    "Case-Mix RN Staffing Hours per Resident per Day" VARCHAR,
    "Case-Mix Total Nurse Staffing Hours per Resident per Day" VARCHAR,
    "Case-Mix Weekend Total Nurse Staffing Hours per Resident per Day" VARCHAR,
    "Adjusted Nurse Aide Staffing Hours per Resident per Day" VARCHAR,
    "Adjusted LPN Staffing Hours per Resident per Day" VARCHAR,
    "Adjusted RN Staffing Hours per Resident per Day" VARCHAR,
    "Adjusted Total Nurse Staffing Hours per Resident per Day" VARCHAR,
    "Adjusted Weekend Total Nurse Staffing Hours per Resident per Day" VARCHAR,
    "Rating Cycle 1 Standard Survey Health Date" VARCHAR,
    "Rating Cycle 1 Total Number of Health Deficiencies" VARCHAR,
    "Rating Cycle 1 Number of Standard Health Deficiencies" VARCHAR,
    "Rating Cycle 1 Number of Complaint Health Deficiencies" VARCHAR,
    "Rating Cycle 1 Health Deficiency Score" VARCHAR,
    "Rating Cycle 1 Number of Health Revisits" VARCHAR,
    "Rating Cycle 1 Health Revisit Score" VARCHAR,
    "Rating Cycle 1 Total Health Score" VARCHAR,
    "Rating Cycle 2 Standard Health Survey Date" VARCHAR,
    "Rating Cycle 2 Total Number of Health Deficiencies" VARCHAR,
    "Rating Cycle 2 Number of Standard Health Deficiencies" VARCHAR,
    "Rating Cycle 2 Number of Complaint Health Deficiencies" VARCHAR,
    "Rating Cycle 2 Health Deficiency Score" VARCHAR,
    "Rating Cycle 2 Number of Health Revisits" VARCHAR,
    "Rating Cycle 2 Health Revisit Score" VARCHAR,
    "Rating Cycle 2 Total Health Score" VARCHAR,
    "Rating Cycle 3 Standard Health Survey Date" VARCHAR,
    "Rating Cycle 3 Total Number of Health Deficiencies" VARCHAR,
    "Rating Cycle 3 Number of Standard Health Deficiencies" VARCHAR,
    "Rating Cycle 3 Number of Complaint Health Deficiencies" VARCHAR,
    "Rating Cycle 3 Health Deficiency Score" VARCHAR,
    "Rating Cycle 3 Number of Health Revisits" VARCHAR,
    "Rating Cycle 3 Health Revisit Score" VARCHAR,
    "Rating Cycle 3 Total Health Score" VARCHAR,
    "Total Weighted Health Survey Score" VARCHAR,
    "Number of Facility Reported Incidents" VARCHAR,
    "Number of Substantiated Complaints" VARCHAR,
    "Number of Citations from Infection Control Inspections" VARCHAR,
    "Number of Fines" VARCHAR,
    "Total Amount of Fines in Dollars" VARCHAR,
    "Number of Payment Denials" VARCHAR,
    "Total Number of Penalties" VARCHAR,
    "Location" VARCHAR,
    "Latitude" VARCHAR,
    "Longitude" VARCHAR,
    "Geocoding Footnote" VARCHAR,
    "Processing Date" VARCHAR
);

COPY INTO NH_PROVIDERINFO_OCT2024
FROM @HEALTHCARE_S3_STAGE/NH_ProviderInfo_Oct2024.csv
FILE_FORMAT = (FORMAT_NAME = 'MY_CSV_FORMAT')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- Table 13: NH_QUALITYMSR_CLAIMS_OCT2024
-- Source CSV: NH_QualityMsr_Claims_Oct2024.csv
-- Columns: 17
CREATE OR REPLACE TABLE NH_QUALITYMSR_CLAIMS_OCT2024 (
    "CMS Certification Number (CCN)" VARCHAR,
    "Provider Name" VARCHAR,
    "Provider Address" VARCHAR,
    "City/Town" VARCHAR,
    "State" VARCHAR,
    "ZIP Code" VARCHAR,
    "Measure Code" VARCHAR,
    "Measure Description" VARCHAR,
    "Resident type" VARCHAR,
    "Adjusted Score" VARCHAR,
    "Observed Score" VARCHAR,
    "Expected Score" VARCHAR,
    "Footnote for Score" VARCHAR,
    "Used in Quality Measure Five Star Rating" VARCHAR,
    "Measure Period" VARCHAR,
    "Location" VARCHAR,
    "Processing Date" VARCHAR
);

COPY INTO NH_QUALITYMSR_CLAIMS_OCT2024
FROM @HEALTHCARE_S3_STAGE/NH_QualityMsr_Claims_Oct2024.csv
FILE_FORMAT = (FORMAT_NAME = 'MY_CSV_FORMAT')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- Table 14: NH_QUALITYMSR_MDS_OCT2024
-- Source CSV: NH_QualityMsr_MDS_Oct2024.csv
-- Columns: 23
CREATE OR REPLACE TABLE NH_QUALITYMSR_MDS_OCT2024 (
    "CMS Certification Number (CCN)" VARCHAR,
    "Provider Name" VARCHAR,
    "Provider Address" VARCHAR,
    "City/Town" VARCHAR,
    "State" VARCHAR,
    "ZIP Code" VARCHAR,
    "Measure Code" VARCHAR,
    "Measure Description" VARCHAR,
    "Resident type" VARCHAR,
    "Q1 Measure Score" VARCHAR,
    "Footnote for Q1 Measure Score" VARCHAR,
    "Q2 Measure Score" VARCHAR,
    "Footnote for Q2 Measure Score" VARCHAR,
    "Q3 Measure Score" VARCHAR,
    "Footnote for Q3 Measure Score" VARCHAR,
    "Q4 Measure Score" VARCHAR,
    "Footnote for Q4 Measure Score" VARCHAR,
    "Four Quarter Average Score" VARCHAR,
    "Footnote for Four Quarter Average Score" VARCHAR,
    "Used in Quality Measure Five Star Rating" VARCHAR,
    "Measure Period" VARCHAR,
    "Location" VARCHAR,
    "Processing Date" VARCHAR
);

COPY INTO NH_QUALITYMSR_MDS_OCT2024
FROM @HEALTHCARE_S3_STAGE/NH_QualityMsr_MDS_Oct2024.csv
FILE_FORMAT = (FORMAT_NAME = 'MY_CSV_FORMAT')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- Table 15: NH_STATEUSAVERAGES_OCT2024
-- Source CSV: NH_StateUSAverages_Oct2024.csv
-- Columns: 48
CREATE OR REPLACE TABLE NH_STATEUSAVERAGES_OCT2024 (
    "State or Nation" VARCHAR,
    "Cycle 1 Total Number of Health Deficiencies" VARCHAR,
    "Cycle 1 Total Number of Fire Safety Deficiencies" VARCHAR,
    "Cycle 2 Total Number of Health Deficiencies" VARCHAR,
    "Cycle 2 Total Number of Fire Safety Deficiencies" VARCHAR,
    "Cycle 3 Total Number of Health Deficiencies" VARCHAR,
    "Cycle 3 Total Number of Fire Safety Deficiencies" VARCHAR,
    "Average Number of Residents per Day" VARCHAR,
    "Reported Nurse Aide Staffing Hours per Resident per Day" VARCHAR,
    "Reported LPN Staffing Hours per Resident per Day" VARCHAR,
    "Reported RN Staffing Hours per Resident per Day" VARCHAR,
    "Reported Licensed Staffing Hours per Resident per Day" VARCHAR,
    "Reported Total Nurse Staffing Hours per Resident per Day" VARCHAR,
    "Total number of nurse staff hours per resident per day on the weekend" VARCHAR,
    "Registered Nurse hours per resident per day on the weekend" VARCHAR,
    "Reported Physical Therapist Staffing Hours per Resident Per Day" VARCHAR,
    "Total nursing staff turnover" VARCHAR,
    "Registered Nurse turnover" VARCHAR,
    "Number of administrators who have left the nursing home" VARCHAR,
    "Nursing Case-Mix Index" VARCHAR,
    "Case-Mix RN Staffing Hours per Resident per Day" VARCHAR,
    "Case-Mix Total Nurse Staffing Hours per Resident per Day" VARCHAR,
    "Case-Mix Weekend Total Nurse Staffing Hours per Resident per Day" VARCHAR,
    "Number of Fines" VARCHAR,
    "Fine Amount in Dollars" VARCHAR,
    "Percentage of long stay residents whose need for help with daily activities has increased" VARCHAR,
    "Percentage of long stay residents who lose too much weight" VARCHAR,
    "Percentage of low risk long stay residents who lose control of their bowels or bladder" VARCHAR,
    "Percentage of long stay residents with a catheter inserted and left in their bladder" VARCHAR,
    "Percentage of long stay residents with a urinary tract infection" VARCHAR,
    "Percentage of long stay residents who have depressive symptoms" VARCHAR,
    "Percentage of long stay residents who were physically restrained" VARCHAR,
    "Percentage of long stay residents experiencing one or more falls with major injury" VARCHAR,
    "Percentage of long stay residents assessed and appropriately given the pneumococcal vaccine" VARCHAR,
    "Percentage of long stay residents who received an antipsychotic medication" VARCHAR,
    "Percentage of short stay residents assessed and appropriately given the pneumococcal vaccine" VARCHAR,
    "Percentage of short stay residents who newly received an antipsychotic medication" VARCHAR,
    "Percentage of long stay residents whose ability to move independently worsened" VARCHAR,
    "Percentage of long stay residents who received an antianxiety or hypnotic medication" VARCHAR,
    "Percentage of high risk long stay residents with pressure ulcers" VARCHAR,
    "Percentage of long stay residents assessed and appropriately given the seasonal influenza vaccine" VARCHAR,
    "Percentage of short stay residents who made improvements in function" VARCHAR,
    "Percentage of short stay residents who were assessed and appropriately given the seasonal influenza vaccine" VARCHAR,
    "Percentage of short stay residents who were rehospitalized after a nursing home admission" VARCHAR,
    "Percentage of short stay residents who had an outpatient emergency department visit" VARCHAR,
    "Number of hospitalizations per 1000 long-stay resident days" VARCHAR,
    "Number of outpatient emergency department visits per 1000 long-stay resident days" VARCHAR,
    "Processing Date" VARCHAR
);

COPY INTO NH_STATEUSAVERAGES_OCT2024
FROM @HEALTHCARE_S3_STAGE/NH_StateUSAverages_Oct2024.csv
FILE_FORMAT = (FORMAT_NAME = 'MY_CSV_FORMAT')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- Table 16: NH_SURVEYDATES_OCT2024
-- Source CSV: NH_SurveyDates_Oct2024.csv
-- Columns: 5
CREATE OR REPLACE TABLE NH_SURVEYDATES_OCT2024 (
    "CMS Certification Number (CCN)" VARCHAR,
    "Survey Date" VARCHAR,
    "Type of Survey" VARCHAR,
    "Survey Cycle" VARCHAR,
    "Processing Date" VARCHAR
);

COPY INTO NH_SURVEYDATES_OCT2024
FROM @HEALTHCARE_S3_STAGE/NH_SurveyDates_Oct2024.csv
FILE_FORMAT = (FORMAT_NAME = 'MY_CSV_FORMAT')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- Table 17: NH_SURVEYSUMMARY_OCT2024
-- Source CSV: NH_SurveySummary_Oct2024.csv
-- Columns: 41
CREATE OR REPLACE TABLE NH_SURVEYSUMMARY_OCT2024 (
    "CMS Certification Number (CCN)" VARCHAR,
    "Provider Name" VARCHAR,
    "Provider Address" VARCHAR,
    "City/Town" VARCHAR,
    "State" VARCHAR,
    "ZIP Code" VARCHAR,
    "Inspection Cycle" VARCHAR,
    "Health Survey Date" VARCHAR,
    "Fire Safety Survey Date" VARCHAR,
    "Total Number of Health Deficiencies" VARCHAR,
    "Total Number of Fire Safety Deficiencies" VARCHAR,
    "Count of Freedom from Abuse and Neglect and Exploitation Deficiencies" VARCHAR,
    "Count of Quality of Life and Care Deficiencies" VARCHAR,
    "Count of Resident Assessment and Care Planning Deficiencies" VARCHAR,
    "Count of Nursing and Physician Services Deficiencies" VARCHAR,
    "Count of Resident Rights Deficiencies" VARCHAR,
    "Count of Nutrition and Dietary Deficiencies" VARCHAR,
    "Count of Pharmacy Service Deficiencies" VARCHAR,
    "Count of Environmental Deficiencies" VARCHAR,
    "Count of Administration Deficiencies" VARCHAR,
    "Count of Infection Control Deficiencies" VARCHAR,
    "Count of Emergency Preparedness Deficiencies" VARCHAR,
    "Count of Automatic Sprinkler Systems Deficiencies" VARCHAR,
    "Count of Construction Deficiencies" VARCHAR,
    "Count of Services Deficiencies" VARCHAR,
    "Count of Corridor Walls and Doors Deficiencies" VARCHAR,
    "Count of Egress Deficiencies" VARCHAR,
    "Count of Electrical Deficiencies" VARCHAR,
    "Count of Emergency Plans and Fire Drills Deficiencies" VARCHAR,
    "Count of Fire Alarm Systems Deficiencies" VARCHAR,
    "Count of Smoke Deficiencies" VARCHAR,
    "Count of Interior Deficiencies" VARCHAR,
    "Count of Gas and Vacuum and Electrical Systems Deficiencies" VARCHAR,
    "Count of Hazardous Area Deficiencies" VARCHAR,
    "Count of Illumination and Emergency Power Deficiencies" VARCHAR,
    "Count of Laboratories Deficiencies" VARCHAR,
    "Count of Medical Gases and Anaesthetizing Areas Deficiencies" VARCHAR,
    "Count of Smoking Regulations Deficiencies" VARCHAR,
    "Count of Miscellaneous Deficiencies" VARCHAR,
    "Location" VARCHAR,
    "Processing Date" VARCHAR
);

COPY INTO NH_SURVEYSUMMARY_OCT2024
FROM @HEALTHCARE_S3_STAGE/NH_SurveySummary_Oct2024.csv
FILE_FORMAT = (FORMAT_NAME = 'MY_CSV_FORMAT')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- Table 18: SKILLED_NURSING_FACILITY_QUALITY_REPORTING_PROGRAM_NATIONAL_DATA_OCT2024
-- Source CSV: Skilled_Nursing_Facility_Quality_Reporting_Program_National_Data_Oct2024.csv
-- Columns: 7
CREATE OR REPLACE TABLE SKILLED_NURSING_FACILITY_QUALITY_REPORTING_PROGRAM_NATIONAL_DATA_OCT2024 (
    "CMS Certification Number (CCN)" VARCHAR,
    "Measure Code" VARCHAR,
    "Score" VARCHAR,
    "Footnote" VARCHAR,
    "Start Date" VARCHAR,
    "End Date" VARCHAR,
    "Measure Date Range" VARCHAR
);

COPY INTO SKILLED_NURSING_FACILITY_QUALITY_REPORTING_PROGRAM_NATIONAL_DATA_OCT2024
FROM @HEALTHCARE_S3_STAGE/Skilled_Nursing_Facility_Quality_Reporting_Program_National_Data_Oct2024.csv
FILE_FORMAT = (FORMAT_NAME = 'MY_CSV_FORMAT')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- Table 19: SKILLED_NURSING_FACILITY_QUALITY_REPORTING_PROGRAM_PROVIDER_DATA_OCT2024
-- Source CSV: Skilled_Nursing_Facility_Quality_Reporting_Program_Provider_Data_Oct2024.csv
-- Columns: 15
CREATE OR REPLACE TABLE SKILLED_NURSING_FACILITY_QUALITY_REPORTING_PROGRAM_PROVIDER_DATA_OCT2024 (
    "CMS Certification Number (CCN)" VARCHAR,
    "Provider Name" VARCHAR,
    "Address Line 1" VARCHAR,
    "City/Town" VARCHAR,
    "State" VARCHAR,
    "ZIP Code" VARCHAR,
    "County/Parish" VARCHAR,
    "Telephone Number" VARCHAR,
    "CMS Region" VARCHAR,
    "Measure Code" VARCHAR,
    "Score" VARCHAR,
    "Footnote" VARCHAR,
    "Start Date" VARCHAR,
    "End Date" VARCHAR,
    "Measure Date Range" VARCHAR,
    "LOCATION1" VARCHAR
);

COPY INTO SKILLED_NURSING_FACILITY_QUALITY_REPORTING_PROGRAM_PROVIDER_DATA_OCT2024
FROM @HEALTHCARE_S3_STAGE/Skilled_Nursing_Facility_Quality_Reporting_Program_Provider_Data_Oct2024.csv
FILE_FORMAT = (FORMAT_NAME = 'MY_CSV_FORMAT')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- Table 20: SWING_BED_SNF_DATA_OCT2024
-- Source CSV: Swing_Bed_SNF_data_Oct2024.csv
-- Columns: 16
CREATE OR REPLACE TABLE SWING_BED_SNF_DATA_OCT2024 (
    "CMS Certification Number (CCN)" VARCHAR,
    "Provider Name" VARCHAR,
    "Address Line 1" VARCHAR,
    "Address Line 2" VARCHAR,
    "City/Town" VARCHAR,
    "State" VARCHAR,
    "ZIP Code" VARCHAR,
    "County/Parish" VARCHAR,
    "Telephone Number" VARCHAR,
    "CMS Region" VARCHAR,
    "Measure Code" VARCHAR,
    "Score" VARCHAR,
    "Footnote" VARCHAR,
    "Start Date" VARCHAR,
    "End Date" VARCHAR,
    "MeasureDateRange" VARCHAR
);

COPY INTO SWING_BED_SNF_DATA_OCT2024
FROM @HEALTHCARE_S3_STAGE/Swing_Bed_SNF_data_Oct2024.csv
FILE_FORMAT = (FORMAT_NAME = 'MY_CSV_FORMAT')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- ============================================================
-- COMPLETE: 20 tables created with all VARCHAR columns
-- ============================================================