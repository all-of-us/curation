"""
File is intended to store 'constants' that will be used multiple times
throughout the entirety of the 'metrics_over_time' library.

This file should reduce the possibility of potential typos and lead
to a cleaner-looking code (as strings tend to 'bog down' the script
in general. Beyond legibility, this allows us to "mass change" certain
attributes that appear many times in the file.
"""


# Table Names
# -----------
visit_occurrence = 'visit_occurrence'
condition_occurrence = 'condition_occurrence'
drug_exposure = 'drug_exposure'
observation = 'observation'
measurement = 'measurement'
procedure_occurrence = 'procedure_occurrence'

visit_occurrence_full = 'Visit Occurrence'
condition_occurrence_full = 'Condition Occurrence'
drug_exposure_full = 'Drug Exposure'
measurement_full = 'Measurement'
procedure_full = 'Procedure Occurrence'
observation_full = 'Observation'

all_canonical_tables = [
    visit_occurrence, condition_occurrence, drug_exposure,
    observation, measurement, procedure_occurrence]

tables_except_visit = [
    condition_occurrence, drug_exposure,
    observation, measurement, procedure_occurrence]

tables_with_one_date = [
    visit_occurrence, condition_occurrence, drug_exposure]


# Measurement Class Names
# -----------------------
physical_measurement = 'Physical_Measurement'
cmp = 'CMP'
cbc_w_diff = 'CBCwDiff'
cbc = 'CBC'
lipid = 'Lipid'
all_measurements = 'All_Measurements'

measurement_categories = [
    physical_measurement, cmp, cbc_w_diff,
    cbc, lipid, all_measurements]

physical_measurements_full = 'Physical Measurements'
all_measurements_full = 'All Measurements'
cmp_full = 'Comprehensive Metabolic Panel (CMP)'
cbc_w_diff_full = 'Complete Blood Count (CBC) with Differential'
cbc_full = 'Complete Blood Count (CBC)'
lipid_full = 'Lipid'


# Drug Exposure Class Names
# -----------------------
ace_inhibs = 'ace_inhibitors'
pain_nsaids = 'painnsaids'
msk_nsaids = 'msknsaids'
statins = 'statins'
antibiotics = 'antibiotics'
opioids = 'opioids'
oral_hypo = 'oralhypoglycemics'
vaccine = 'vaccine'
ccb = 'ccb'
diuretics = 'diuretics'
all_drugs = 'all_drugs'

drug_categories = [
    ace_inhibs, pain_nsaids, msk_nsaids, statins,
    antibiotics, opioids, oral_hypo, vaccine, ccb,
    diuretics, all_drugs]


ace_inhibs_full = 'ACE Inhibitors'
pain_nsaids_full = 'Pain NSAIDS'
msk_nsaids_full = 'MSK NSAIDS'
statins_full = 'Statins'
antibiotics_full = 'Antibiotics'
opioids_full = 'Opioids'
oral_hypo_full = 'Oral Hypoglycemics'
vaccine_full = 'Vaccine'
ccb_full = 'Calcium Channel Blockers'
diuretics_full = 'Diuretics'
all_drugs_full = 'All Drugs'


# Values
# ------
achilles_max_value = 0.01
integration_minimum = 90
field_population_minimum = 85
foreign_key_max_value = 0.01


# Metric Names
# ------------
duplicates = 'duplicates'
data_after_death = 'data_after_death'
end_before_begin = 'end_before_begin'
concept = 'concept'
unit_success_rate = 'unit_success_rate'
drug_routes = 'drug_routes'
drug_success = 'drug_success'
sites_measurement = 'sites_measurement'
visit_date_disparity = 'visit_date_disparity'
date_datetime_disparity = 'date_datetime_disparity'
erroneous_dates = 'erroneous_dates'
person_id_failure_rate = 'person_id_failure_rate'
achilles_errors = 'achilles_errors'
diabetes = 'diabetes'
visit_id_failure_rate = 'visit_occ_id_failure_rate'

duplicates_full = 'Duplicate Records'
data_after_death_full = 'Data After Death'
end_before_begin_full = 'End Dates Preceding Start Dates'
concept_full = 'Concept ID Success Rate'
unit_success_full = 'Unit Concept ID Success Rate'
drug_routes_full = 'Route Concept ID Success Rate'
drug_success_full = 'Drug Ingredient Integration'
sites_measurement_full = 'Measurement Integration'
visit_date_disparity_full = 'Visit Date Disparity'
date_datetime_disparity_full = 'Date/Datetime Disparity'
erroneous_dates_full = 'Erroneous Dates'
person_id_failure_rate_full = 'Person ID Failure Rate'
achilles_errors_full = 'Number of ACHILLES Errors'
diabetes_full = 'Diabetes Completeness'
visit_id_failure_rate_full = 'Visit ID Failure Rate'


# Dimensions of Data Quality
# -------------------------
conformance = 'Conformance'
plausibility = 'Plausibility'
completeness = 'Completeness'


# Threshold Keys
# -------------------------
concept_success_min = 'concept_success_min'
duplicates_max = 'duplicates_max'
end_before_begin_max = 'end_before_begin_max'
data_after_death_max = 'data_after_death_max'
drug_ingredient_integration_min = 'drug_ingredient_integration_min'
measurement_integration_min = 'measurement_integration_min'
unit_success_min = 'unit_success_min'
route_success_min = 'route_success_min'
date_datetime_disparity_max = 'date_datetime_disparity_max'
erroneous_dates_max = 'erroneous_dates_max'
person_failure_rate_max = 'person_failure_rate_max'
achilles_errors_max = 'achilles_errors_max'
visit_date_disparity_max = 'visit_date_disparity_max'
visit_id_failure_rate_max = 'visit_occ_id_failure_rate_max'


# Other Column Names
# ------------------
observation_success = 'observation_success_rate'
drug_success_col = 'drug_success_rate'
procedure_success = 'procedure_success_rate'
condition_success = 'condition_success_rate'
measurement_success = 'measurement_success_rate'
visit_success = 'visit_success_rate'

concept_success_rate_columns = [
    condition_success, drug_success_col,
    condition_success, measurement_success, procedure_success]

total_unit_success_rate = 'total_unit_success_rate'
total_route_success_rate = 'total_route_success_rate'

hpo_col_name = 'HPO'
table_class_col_name = 'Table/Class'
metric_type_col_name = 'Metric Type'
data_quality_dimension_col_name = 'Data Quality Dimension'
link_col_name = 'Link'
first_reported_col_name = 'First Reported'


# Other Strings
# -------------
date_format = '%B_%d_%Y'
output_file_ending = "_data_quality_issues.xlsx"
xl_writer = 'xlsxwriter'
