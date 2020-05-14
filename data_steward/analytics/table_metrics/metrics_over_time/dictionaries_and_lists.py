"""
This file is intended to serve as a 'storing place' for pieces of information
that may change in the future. This also is a great means to sequester pieces
of information that may otherwise 'bog down' the regular code.

Dictionaries
------------
thresholds: thresholds: the point at which a data quality metric (whether too
    high or too low) would be flagged as 'erroneous'. not used in the
    metrics_over_time script yet but has potential future implementations.

choice_dict: correlates the user-specified choice to the corresponding
    page on the analytics report

percentage_dict: correlated a particular analysis choice with whether or
    not it is intended to report out a fixed number (as in the case of
    duplicate records) or a  'percentage' (namely success or failure
    rates)

target_low_dict: indicates whether the metric is intended to be
    minimized (in the case of an 'error') or maximized (in the
    case of a 'success rate')

columns_to_document_for_sheet: indicates which columns contain
    information that should be stored for the particular data
    quality metric that is being analyzed

table_based_on_column_provided: allows us to determine the table that
    should be associated with a particular Data Quality Dimension object
    based upon the column that was used to get the associated 'value'
    float

data_quality_dimension_dict: shows which attribute of Kahn's Data Quality
    framework the particular 'data quality metric' at hand relates to

metric_type_to_english_dict: allows one to translate the 'metric type'
    that is normally associated with a 'DataQualityMetric' object to
    'English'. this is useful for printing the columns on a new
    dashboard

full_names: allows one to use the hpo_id (shorter) name to find
    the longer (more human-readable) name

Lists
-----
row_count_col_names: shows the column names where one can find the
    total row count for a particular date for each table

unweighted_metric_already_integrated_for_hpo: shows which
    'unweighted metrics' do not need to be calculated for
    each HPO. these metrics do NOT need to be calculated because
    there already is an appropriate 'aggregate metric'
    established in the sheet.

aggregate_metric_class_names: contains the 'names' of the aggregate
    metric objects that one can use

no_aggregate_metric_needed_for_table_sheets: indicates instances where
    no 'aggregate' row needs to be calculated for the 'table' sheets
"""

import constants

# ---------- Dictionaries ---------- #
thresholds = {
    constants.concept_success_min: 90,
    constants.duplicates_max: 5,

    constants.end_before_begin_max: 0,
    constants.data_after_death_max: 0,

    constants.drug_ingredient_integration_min: constants.integration_minimum,
    constants.measurement_integration_min: constants.integration_minimum,

    constants.unit_success_min: constants.field_population_minimum,
    constants.route_success_min: constants.field_population_minimum,

    constants.date_datetime_disparity_max: constants.achilles_max_value,
    constants.erroneous_dates_max: constants.achilles_max_value,
    constants.person_failure_rate_max: constants.achilles_max_value,

    constants.achilles_errors_max: 15
}


choice_dict = {
    'a': constants.duplicates,
    'b': constants.data_after_death,
    'c': constants.end_before_begin,
    'd': constants.concept,
    'e': constants.measurement_units,
    'f': constants.drug_routes,
    'g': constants.drug_success,
    'h': constants.sites_measurement,
    'i': constants.visit_date_disparity,
    'j': constants.date_datetime_disparity,
    'k': constants.erroneous_dates,
    'l': constants.person_id_failure_rate,
    'm': constants.achilles_errors}

percentage_dict = {
    constants.duplicates: constants.false,
    constants.data_after_death: constants.true,
    constants.end_before_begin: constants.true,
    constants.concept: constants.true,
    constants.measurement_units: constants.true,
    constants.drug_routes: constants.true,
    constants.drug_success: constants.true,
    constants.sites_measurement: constants.true,
    constants.visit_date_disparity: constants.true,
    constants.date_datetime_disparity: constants.true,
    constants.erroneous_dates: constants.true,
    constants.person_id_failure_rate: constants.true,
    constants.achilles_errors: constants.false
}


# FIXME: for errors, we want target_low value to be true

target_low_dict = {
    # non-percentage values
    constants.duplicates: constants.true,
    constants.achilles_errors: constants.true,

    # success rates
    constants.concept: constants.false,
    constants.measurement_units: constants.false,
    constants.drug_routes: constants.false,
    constants.drug_success: constants.false,
    constants.sites_measurement: constants.false,

    constants.data_after_death: constants.true,
    constants.end_before_begin: constants.true,
    constants.visit_date_disparity: constants.false,

    constants.date_datetime_disparity: constants.true,
    constants.erroneous_dates: constants.true,
    constants.person_id_failure_rate: constants.true,
}

columns_to_document_for_sheet = {
    constants.measurement_units: [constants.total_unit_success_rate],

    constants.sites_measurement:
        constants.measurement_categories,

    constants.end_before_begin:
        constants.tables_with_one_date,

    constants.duplicates:
        constants.all_canonical_tables,

    constants.drug_routes: [constants.total_route_success_rate],

    constants.drug_success:
        constants.drug_categories,

    constants.data_after_death:
        constants.all_canonical_tables,

    constants.diabetes: [
        'diabetics_w_drugs', 'diabetics_w_glucose',
        'diabetics_w_a1c', 'diabetics_w_insulin'],

    constants.concept: [
        constants.observation_success, constants.drug_success_col,
        constants.procedure_success, constants.condition_success,
        constants.measurement_success, constants.visit_success],

    constants.date_datetime_disparity:
        constants.all_canonical_tables,

    constants.erroneous_dates:
        constants.all_canonical_tables,

    constants.person_id_failure_rate:
        constants.all_canonical_tables,

    constants.achilles_errors:
        ['num_distinct_ids']
}


table_based_on_column_provided = {
    # canonical columns
    constants.visit_occurrence: constants.visit_occurrence_full,
    constants.condition_occurrence: constants.condition_occurrence_full,
    constants.drug_exposure: constants.drug_exposure_full,
    constants.measurement: constants.measurement_full,
    constants.procedure_occurrence: constants.procedure_full,
    constants.observation: constants.observation_full,

    # general concept success rate columns
    constants.observation_success: constants.observation_full,
    constants.drug_success_col: constants.drug_exposure_full,
    constants.procedure_success: constants.procedure_full,
    constants.condition_success: constants.condition_occurrence_full,
    constants.measurement_success: constants.measurement_full,
    constants.visit_success: constants.visit_occurrence_full,

    # field concept success rate columns
    constants.total_unit_success_rate: constants.measurement_full,
    constants.total_route_success_rate: constants.drug_exposure_full,

    # drug integration columns
    constants.all_drugs: 'All Drugs',
    constants.ace_inhibs: 'ACE Inhibitors',
    constants.pain_nsaids: 'Pain NSAIDS',
    constants.msk_nsaids: 'MSK NSAIDS',
    constants.statins: 'Statins',
    constants.antibiotics: 'Antibiotics',
    constants.opioids: 'Opioids',
    constants.oral_hypo: 'Oral Hypoglycemics',
    constants.vaccine: 'Vaccine',
    constants.ccb: 'Calcium Channel Blockers',
    constants.diuretics: 'Diuretics',

    # measurement integration columns
    constants.all_measurements: 'All Measurements',
    constants.physical_measurement: 'Physical Measurements',
    constants.cmp: 'Comprehensive Metabolic Panel',
    constants.cbc_w_diff: 'CBC with Differential',
    constants.cbc: 'Complete Blood Count (CBC)',
    constants.lipid: 'Lipid',

    'num_distinct_ids': 'All Tables'
}

data_quality_dimension_dict = {
    constants.concept: constants.conformance,
    constants.duplicates: constants.plausibility,
    constants.end_before_begin: constants.plausibility,
    constants.data_after_death: constants.plausibility,
    constants.sites_measurement: constants.completeness,
    constants.drug_success: constants.completeness,
    constants.drug_routes: constants.completeness,
    constants.measurement_units: constants.completeness,
    constants.date_datetime_disparity: constants.conformance,
    constants.erroneous_dates: constants.plausibility,
    constants.person_id_failure_rate: constants.conformance,
    constants.achilles_errors: constants.conformance
}

metric_type_to_english_dict = {
    # field population metrics
    constants.measurement_units: constants.measurement_units_full,
    constants.drug_routes: constants.drug_routes_full,

    # integration metrics
    constants.drug_success: constants.drug_success_full,
    constants.sites_measurement: constants.sites_measurement_full,

    # ACHILLES errors
    constants.end_before_begin: constants.end_before_begin_full,
    constants.data_after_death: constants.data_after_death_full,
    constants.date_datetime_disparity: constants.date_datetime_disparity_full,

    # other metrics
    constants.concept: constants.concept_full,
    constants.duplicates: constants.duplicates_full,
    constants.erroneous_dates: constants.erroneous_dates_full,
    constants.person_id_failure_rate: constants.person_id_failure_rate_full,
    constants.achilles_errors: constants.achilles_errors_full
}

metrics_to_weight = [
    constants.measurement_units, constants.drug_routes,
    constants.end_before_begin, constants.data_after_death,
    constants.concept, constants.duplicates,
    constants.date_datetime_disparity,
    constants.erroneous_dates, constants.person_id_failure_rate]

full_names = {
    "saou_uab_selma": "UAB Selma",
    "saou_uab_hunt": "UAB Huntsville",
    "saou_tul": "Tulane University",
    "pitt_temple": "Temple University",
    "saou_lsu": "Louisiana State University",
    "trans_am_meyers": "Reliant Medical Group (Meyers Primary Care)",
    "trans_am_essentia": "Essentia Health Superior Clinic",
    "saou_ummc": "University of Mississippi",
    "seec_miami": "SouthEast Enrollment Center Miami",
    "seec_morehouse": "SouthEast Enrollment Center Morehouse",
    "seec_emory": "SouthEast Enrollment Center Emory",
    "uamc_banner": "Banner Health",
    "pitt": "University of Pittsburgh",
    "nyc_cu": "Columbia University Medical Center",
    "ipmc_uic": "University of Illinois Chicago",
    "trans_am_spectrum": "Spectrum Health",
    "tach_hfhs": "Henry Ford Health System",
    "nec_bmc": "Boston Medical Center",
    "cpmc_uci": "UC Irvine",
    "nec_phs": "Partners HealthCare",
    "nyc_cornell": "Weill Cornell Medical Center",
    "ipmc_nu": "Northwestern Memorial Hospital",
    "nyc_hh": "Harlem Hospital",
    "ipmc_uchicago": "University of Chicago",
    "aouw_mcri": "Marshfield Clinic",
    "syhc": "San Ysidro Health Center",
    "cpmc_ceders": "Cedars-Sinai",
    "seec_ufl": "University of Florida",
    "saou_uab": "University of Alabama at Birmingham",
    "trans_am_baylor": "Baylor",
    "cpmc_ucsd": "UC San Diego",
    "ecchc": "Eau Claire Cooperative Health Center",
    "chci": "Community Health Center, Inc.",
    "aouw_uwh": "UW Health (University of Wisconsin Madison)",
    "cpmc_usc": "University of Southern California",
    "hrhc": "HRHCare",
    "ipmc_northshore": "NorthShore University Health System",
    "chs": "Cherokee Health Systems",
    "cpmc_ucsf": "UC San Francisco",
    "jhchc": "Jackson-Hinds CHC",
    "aouw_mcw": "Medical College of Wisconsin",
    "cpmc_ucd": "UC Davis",
    "ipmc_rush": "Rush University",
    "va": "United States Department of Veterans Affairs - Boston",
    "saou_umc": "University Medical Center (UA Tuscaloosa)"
}

row_count_col_names = [
    constants.observation_total_row,
    constants.drug_total_row,
    constants.procedure_total_row,
    constants.condition_total_row,
    constants.measurement_total_row,
    constants.visit_total_row]

# ---------- Lists ---------- #
unweighted_metric_already_integrated_for_hpo = [
    constants.drug_routes,
    constants.measurement_units, constants.achilles_errors]

no_aggregate_metric_needed_for_table_sheets = [
    constants.drug_success, constants.sites_measurement]

aggregate_metric_class_names = ['All Measurements', 'All Drugs']
