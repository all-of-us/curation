"""
This file is intended to serve as a 'storing place' for pieces of information
that may change in the future. This also is a great means to sequester pieces
of information that may otherwise 'bog down' the regular code.

Dictionaries
------------
thresholds: the point at which a data quality metric (whether too
    high or too low) would be flagged as 'erroneous'. not used in the
    metrics_over_time script yet but has potential future implementations.

thresholds_full_name: shows the 'full name' of the different data quality
    metrics and their corresponding thresholds. this allows us to access
    the 'thesholds' from the DataQualityMetric objects.

percentage_dict: correlated a particular analysis choice with whether or
    not it is intended to report out a fixed number (as in the case of
    duplicate records) or a  'percentage' (namely success or failure
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
"""

# ---------- Dictionaries ---------- #
thresholds = {
    'concept_success_min': 90,
    'duplicates_max': 5,

    'end_before_begin_max': 0,
    'data_after_death_max': 0,

    'drug_ingredient_integration_min': 90,
    'measurement_integration_min': 90,

    'unit_success_min': 85,
    'route_success_min': 85,

    'date_datetime_disparity_max': 0,
    'erroneous_dates_max': 0.01,
    'person_failure_rate_max': 0.01}

thresholds_full_name = {
    # field population metrics
    'Unit Concept ID Success Rate': 85,
    'Route Concept ID Success Rate': 85,

    # integration metrics
    'Drug Ingredient Integration': 90,
    'Measurement Integration': 90,

    # ACHILLES errors
    'End Dates Preceding Start Dates': 0,
    'Data After Death': 0,
    'Date/Datetime Disparity': 0,

    # other metrics
    'Concept ID Success Rate': 90,
    'Duplicate Records': 5,
    'Erroneous Dates': 0.01,
    'Person ID Failure Rate': 0.01}

min_or_max = {
    # field population metrics
    'Unit Concept ID Success Rate': 'minimum',
    'Route Concept ID Success Rate': 'minimum',

    # integration metrics
    'Drug Ingredient Integration': 'minimum',
    'Measurement Integration': 'minimum',

    # ACHILLES errors
    'End Dates Preceding Start Dates': 'maximum',
    'Data After Death': 'maximum',
    'Date/Datetime Disparity': 'maximum',

    # other metrics
    'Concept ID Success Rate': 'minimum',
    'Duplicate Records': 'maximum',
    'Erroneous Dates': 'maximum',
    'Person ID Failure Rate': 'maximum'}

percentage_dict = {
    'duplicates': False,
    'data_after_death': True,
    'end_before_begin': True,
    'concept': True,
    'measurement_units': True,
    'drug_routes': True,
    'drug_success': True,
    'sites_measurement': True,
    'visit_date_disparity': True,
    'date_datetime_disparity': True,
    'erroneous_dates': True,
    'person_id_failure_rate': True}

target_low_dict = {
    'duplicates': True,
    'data_after_death': True,
    'end_before_begin': True,
    'concept': False,
    'measurement_units': False,
    'drug_routes': False,
    'drug_success': False,
    'sites_measurement': False,
    'visit_date_disparity': False,

    # FIXME: the three below - by logic - should
    # be 'True' but were calculated as showing
    # the % of errors rather than (100 - % of errors)
    # in the DQM scripts. This means they should be
    # logged as 'False' here to make it an effective
    # double negative.
    'date_datetime_disparity': False,
    'erroneous_dates': False,
    'person_id_failure_rate': False}


# NOTE: This is actually different than what one would
# find in the metrics_over_time generator. This is because
# we want less granularity for the 'integration metrics'.

columns_to_document_for_sheet_email = {
    'measurement_units': ['total_unit_success_rate'],

    'sites_measurement': [
        'Physical_Measurement',	'CMP', ' CBCwDiff',
        'CBC', 'Lipid',	'All_Measurements'],

    'end_before_begin': [
        'visit_occurrence', 'condition_occurrence',
        'drug_exposure', 'device_exposure'],

    'duplicates': ['visit_occurrence', 'condition_occurrence',
                   'drug_exposure', 'measurement',
                   'procedure_occurrence', 'device_exposure',
                   'observation'],

    'drug_routes': ['total_route_success_rate'],

    'drug_success': [
        'ace_inhibitors', 'painnsaids', 'msknsaids',
        'statins', 'antibiotics', 'opioids', 'oralhypoglycemics',
        'vaccine', 'ccb', 'diuretics', 'all_drugs'],

    'data_after_death': [
        'visit_occurrence', 'condition_occurrence',
        'drug_exposure', 'measurement',
        'procedure_occurrence', 'observation'],

    'diabetes': [
        'diabetics_w_drugs', 'diabetics_w_glucose',
        'diabetics_w_a1c', 'diabetics_w_insulin'],

    'concept': [
        'observation_success_rate', 'drug_success_rate',
        'procedure_success_rate', 'condition_success_rate',
        'measurement_success_rate', 'visit_success_rate'],

    'date_datetime_disparity': [
        'visit_occurrence', 'condition_occurrence',
        'drug_exposure', 'measurement',
        'procedure_occurrence', 'observation'],

    'erroneous_dates': [
        'visit_occurrence', 'condition_occurrence',
        'drug_exposure', 'measurement',
        'procedure_occurrence', 'observation'],

    'person_id_failure_rate': [
        'visit_occurrence', 'condition_occurrence',
        'drug_exposure', 'measurement',
        'procedure_occurrence', 'observation']}


table_based_on_column_provided = {
    # canonical columns
    'visit_occurrence': 'Visit Occurrence',
    'condition_occurrence': 'Condition Occurrence',
    'drug_exposure': 'Drug Exposure',
    'device_exposure': 'Device Exposure',
    'measurement': 'Measurement',
    'procedure_occurrence': 'Procedure Occurrence',
    'observation': 'Observation',

    # general concept success rate columns
    'observation_success_rate': 'Observation',
    'drug_success_rate': 'Drug Exposure',
    'procedure_success_rate': 'Procedure Occurrence',
    'condition_success_rate': 'Condition Occurrence',
    'measurement_success_rate': 'Measurement',
    'visit_success_rate': 'Visit Occurrence',

    # field concept success rate columns
    'total_unit_success_rate': 'Measurement',
    'total_route_success_rate': 'Drug Exposure',

    # drug integration columns
    'all_drugs': 'All Drugs',
    'ace_inhibitors': 'ACE Inhibitors',
    'painnsaids': 'Pain NSAIDS',
    'msknsaids': 'MSK NSAIDS',
    'statins': 'Statins',
    'antibiotics': 'Antibiotics',
    'opioids': 'Opioids',
    'oralhypoglycemics': 'Oral Hypoglycemics',
    'vaccine': 'Vaccine',
    'ccb': 'Calcium Channel Blockers',
    'diuretics': 'Diuretics',

    # measurement integration columns
    'All_Measurements': 'All Measurements',
    "Physical_Measurement": 'Physical Measurements',
    'CMP': 'Comprehensive Metabolic Panel',
    'CBCwDiff': 'CBC with Differential',
    'CBC': 'Complete Blood Count (CBC)',
    'Lipid': 'Lipid'}

data_quality_dimension_dict = {
    'concept': 'Conformance',
    'duplicates': 'Plausibility',
    'end_before_begin': 'Plausibility',
    'data_after_death': 'Plausibility',
    'sites_measurement': 'Completeness',
    'drug_success': 'Completeness',
    'drug_routes': 'Completeness',
    'measurement_units': 'Completeness',
    'date_datetime_disparity': 'Conformance',
    'erroneous_dates': 'Plausibility',
    'person_id_failure_rate': 'Conformance'}

metric_type_to_english_dict = {
    # field population metrics
    'measurement_units': 'Unit Concept ID Success Rate',
    'drug_routes': 'Route Concept ID Success Rate',

    # integration metrics
    'drug_success': 'Drug Ingredient Integration',
    'sites_measurement': 'Measurement Integration',

    # ACHILLES errors
    'end_before_begin': 'End Dates Preceding Start Dates',
    'data_after_death': 'Data After Death',
    'date_datetime_disparity': 'Date/Datetime Disparity',

    # other metrics
    'concept': 'Concept ID Success Rate',
    'duplicates': 'Duplicate Records',
    'erroneous_dates': 'Erroneous Dates',
    'person_id_failure_rate': 'Person ID Failure Rate'}

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
    "saou_umc": "University Medical Center (UA Tuscaloosa)"}


# ---------- Lists ---------- #

row_count_col_names = [
    'observation_total_row',
    'drug_total_row',
    'procedure_total_row',
    'condition_total_row',
    'measurement_total_row',
    'visit_total_row']
