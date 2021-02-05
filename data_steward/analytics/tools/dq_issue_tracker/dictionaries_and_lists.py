"""
File is meant to contain dictionaries that can be used for the
primary file and/or the HPO class functions.

The dictionaries are as follows:
--------------------------------
relevant_links: the relevant links for the output file
    that is established in create_dq_issue_site_dfs.py. This will
    help maintain the overall readability of the aforementioned
    script

thresholds: the point at which a data quality metric (whether too
    high or too low) would be flagged as 'erroneous'

full_names: allows one to use the hpo_id (shorter) name to find
    the longer (more human-readable) name

metric_names: keys for the sheet in the dataframe and values
    for the name of the corresponding attribute for the HPO
    object

desired_columns_dict: determines which column(s) should be used
    for a particular data quality metric. mulitple columns indicate
    that multiple dimensions (likely different tables) are being
    investigated.

data_quality_dimension_dict: shows which attribute of Kahn's Data Quality
    framework the particular 'data quality metric' at hand relates to

table_based_on_column_provided: allows us to determine the table that
    should be associated with a particular Data Quality Dimension object
    based upon the column that was used to get the associated 'value'
    float

metric_type_to_english_dict: allows one to translate the 'metric type'
    that is normally associated with a 'DataQualityMetric' object to
    'English'. this is useful for printing the columns on a new
    dashboard

english_to_metric_type_dict: effectively reverse engineers the
    dicitonary above. useful for going from an 'old' dashboard to
    comparing to the attributes of DataQualityMetrics

new_metric: list of 'new metrics' that one would not expect to
    see in older reports in terms of dates. see the application
    in the cross_reference_functions file
"""

import constants

relevant_links = {
    constants.concept:
    "https://sites.google.com/view/ehrupload/data-quality-metrics/"
    "concept-success-rate?authuser=0",
   
    constants.duplicates:
    "https://sites.google.com/view/ehrupload/data-quality-metrics/"
    "duplicates?authuser=0",

    constants.date_datetime_disparity:
    "https://sites.google.com/view/ehrupload/data-quality-metrics/"
    "datedatetime-disparity?authuser=0",

    constants.end_before_begin:
    "https://sites.google.com/view/ehrupload/data-quality-metrics/"
    "end-dates-preceding-start-dates?authuser=0",

    constants.data_after_death:
    "https://sites.google.com/view/ehrupload/data-quality-metrics/"
    "data-after-death?authuser=0",

    constants.unit_success_rate:
    "https://sites.google.com/view/ehrupload/data-quality-metrics/"
    "unit-concept-success-rate?authuser=0",

    constants.drug_routes:
    "https://sites.google.com/view/ehrupload/data-quality-metrics/"
    "route-concept-success-rate?authuser=0",

    constants.sites_measurement:
    "https://sites.google.com/view/ehrupload/data-quality-metrics/"
    "measurement-integration-rate?authuser=0",

    constants.drug_success:
    "https://sites.google.com/view/ehrupload/data-quality-metrics/"
    "drug-ingredient-integration-rate?authuser=0",

    constants.erroneous_dates:
    "https://sites.google.com/view/ehrupload/data-quality-metrics/"
    "erroneous-dates?authuser=0",

    constants.person_id_failure_rate:
    "https://sites.google.com/view/ehrupload/data-quality-metrics/"
    "person-id-failure-rate?authuser=0",

    constants.visit_date_disparity:
    "https://sites.google.com/view/ehrupload/data-quality-metrics/"
    "visit-date-disparity?authuser=0",

    constants.visit_id_failure_rate:
    "https://sites.google.com/view/ehrupload/data-quality-metrics/"
    "visit-id-failure-rate?authuser=0"
}


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

    constants.visit_id_failure_rate_max: constants.foreign_key_max_value,
    constants.person_failure_rate_max: constants.foreign_key_max_value,

    constants.achilles_errors_max: 15,
    constants.visit_date_disparity_max: constants.achilles_max_value
}

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
    "saou_umc": "University Medical Center (UA Tuscaloosa)",
    "saou_usahs": "University of South Alabama",
    "saou_cgmhs": "Cooper Green Mercy Hospital",
    "illinois_near_north": "Alliance Chicago",
    'wisconsin_medical_college_sshc': 'Sixteenth Street Community Health Center Parkway Clinic'
}


metric_names = [
    # field population metrics
    constants.unit_success_rate,
    constants.drug_routes,

    # integration metrics
    constants.drug_success,
    constants.sites_measurement,

    # ACHILLES errors
    constants.end_before_begin,
    constants.data_after_death,

    # other metrics
    constants.concept,
    constants.duplicates,
    constants.erroneous_dates,
    constants.person_id_failure_rate,
    constants.date_datetime_disparity,
    constants.visit_date_disparity,
    constants.visit_id_failure_rate]


columns_to_document_for_sheet = {
    constants.unit_success_rate: [constants.total_unit_success_rate],

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

    constants.concept: constants.concept_success_rate_columns,

    constants.date_datetime_disparity:
        constants.all_canonical_tables,

    constants.erroneous_dates:
        constants.all_canonical_tables,

    constants.person_id_failure_rate:
        constants.all_canonical_tables,

    constants.achilles_errors:
        ['num_distinct_ids'],

    constants.visit_date_disparity:
        constants.tables_except_visit,

    constants.visit_id_failure_rate:
        constants.tables_except_visit
}

table_or_class_based_on_column_provided = {
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
    constants.all_drugs: constants.all_drugs_full,
    constants.ace_inhibs: constants.ace_inhibs_full,
    constants.pain_nsaids: constants.pain_nsaids_full,
    constants.msk_nsaids: constants.msk_nsaids_full,
    constants.statins: constants.statins_full,
    constants.antibiotics: constants.antibiotics_full,
    constants.opioids: constants.opioids_full,
    constants.oral_hypo: constants.oral_hypo_full,
    constants.vaccine: constants.vaccine_full,
    constants.ccb: constants.ccb_full,
    constants.diuretics: constants.diuretics_full,

    # measurement integration columns
    constants.all_measurements: constants.all_measurements_full,
    constants.physical_measurement: constants.physical_measurements_full,
    constants.cmp: constants.cmp_full,
    constants.cbc_w_diff: constants.cbc_w_diff_full,
    constants.cbc: constants.cbc_full,
    constants.lipid: constants.lipid_full,

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
    constants.unit_success_rate: constants.completeness,
    constants.date_datetime_disparity: constants.conformance,
    constants.erroneous_dates: constants.plausibility,
    constants.person_id_failure_rate: constants.conformance,
    constants.achilles_errors: constants.conformance,
    constants.visit_date_disparity: constants.conformance,
    constants.visit_id_failure_rate: constants.conformance
}

metric_type_to_english_dict = {
    # field population metrics
    constants.unit_success_rate: constants.unit_success_full,
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
    constants.achilles_errors: constants.achilles_errors_full,
    constants.visit_date_disparity: constants.visit_date_disparity_full,
    constants.visit_id_failure_rate: constants.visit_id_failure_rate_full
}

# currently blank - no new metrics expected
new_metric_types = []


