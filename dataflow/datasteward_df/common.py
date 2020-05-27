# AOU required CDM tables
CARE_SITE = 'care_site'
CONDITION_OCCURRENCE = 'condition_occurrence'
DEATH = 'death'
DEVICE_EXPOSURE = 'device_exposure'
DRUG_EXPOSURE = 'drug_exposure'
FACT_RELATIONSHIP = 'fact_relationship'
LOCATION = 'location'
MEASUREMENT = 'measurement'
NOTE = 'note'
OBSERVATION = 'observation'
PERSON = 'person'
PROCEDURE_OCCURRENCE = 'procedure_occurrence'
PROVIDER = 'provider'
SPECIMEN = 'specimen'
VISIT_OCCURRENCE = 'visit_occurrence'
AOU_REQUIRED = [
    CARE_SITE, CONDITION_OCCURRENCE, DEATH, DEVICE_EXPOSURE, DRUG_EXPOSURE,
    FACT_RELATIONSHIP, LOCATION, MEASUREMENT, NOTE, OBSERVATION, PERSON,
    PROCEDURE_OCCURRENCE, PROVIDER, SPECIMEN, VISIT_OCCURRENCE
]

# Standardized clinical data tables in OMOP. All should contain a person_id column. See
# https://github.com/OHDSI/CommonDataModel/wiki/Standardized-Clinical-Data-Tables

# Clinical tables which do not have a corresponding mapping table.
MAPPED_CLINICAL_DATA_TABLES = [
    VISIT_OCCURRENCE, CONDITION_OCCURRENCE, DRUG_EXPOSURE, MEASUREMENT,
    PROCEDURE_OCCURRENCE, OBSERVATION, DEVICE_EXPOSURE, SPECIMEN
]
# Clinical tables which do not have a corresponding mapping table.
UNMAPPED_CLINICAL_DATA_TABLES = [DEATH]
# All clinical tables.
CLINICAL_DATA_TABLES = MAPPED_CLINICAL_DATA_TABLES + UNMAPPED_CLINICAL_DATA_TABLES

# other CDM tables
ATTRIBUTE_DEFINITION = 'attribute_definition'
COHORT_DEFINITION = 'cohort_definition'
CONDITION_ERA = 'condition_era'
DRUG_ERA = 'drug_era'
DOSE_ERA = 'dose_era'
DRUG_COST = 'drug_cost'
VISIT_COST = 'visit_cost'
DEVICE_COST = 'device_cost'
PROCEDURE_COST = 'procedure_cost'
OBSERVATION_PERIOD = 'observation_period'
PAYER_PLAN_PERIOD = 'payer_plan_period'
