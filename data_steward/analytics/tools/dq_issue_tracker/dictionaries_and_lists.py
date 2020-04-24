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
"""

relevant_links = {
    "concept":
    "https://sites.google.com/view/ehrupload/"
    "data-quality-metrics/concept-success-rate?authuser=0",

    "duplicates":
    "https://sites.google.com/view/ehrupload/"
    "data-quality-metrics/duplicates?authuser=0",

    "date_datetime_disparity":
    "https://sites.google.com/view/ehrupload/"
    "data-quality-metrics/datedatetime-disparity?authuser=0",

    "end_before_begin":
    "https://sites.google.com/view/ehrupload/"
    "data-quality-metrics/end-dates-preceding-start-dates?authuser=0",

    "data_after_death":
    "https://sites.google.com/view/ehrupload/"
    "data-quality-metrics/data-after-death?authuser=0",

    "measurement_units":
    "https://sites.google.com/view/ehrupload/"
    "data-quality-metrics/unit-concept-success-rate?authuser=0",

    "drug_routes":
    "https://sites.google.com/view/ehrupload/"
    "data-quality-metrics/route-concept-success-rate?authuser=0",

    "sites_measurement":
    "https://sites.google.com/view/ehrupload/"
    "data-quality-metrics/measurement-integration-rate?authuser=0",

    "drug_success":
    "https://sites.google.com/view/ehrupload/"
    "data-quality-metrics/drug-ingredient-integration-rate?authuser=0",
    
    "erroneous_dates":
    "https://sites.google.com/view/ehrupload/"
    "data-quality-metrics/erroneous-dates?authuser=0",

    "person_id_failure_rate":
    "https://sites.google.com/view/ehrupload/"
    "data-quality-metrics/person-id-failure-rate?authuser=0"
}


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


metric_names = {
    # field population metrics
    'measurement_units': 'unit_success',
    'drug_routes': 'route_success',

    # integration metrics
    'drug_success': 'drug_integration',
    'sites_measurement': 'measurement_integration',

    # ACHILLES errors
    'end_before_begin': 'end_before_start',
    'data_after_death': 'data_after_death',

    # other metrics
    'concept': 'concept_success',
    'duplicates': 'duplicates',

    'erroneous_dates': 'erroneous_dates',
    'person_id_failure_rate': 'person_id_failure_rate',
    'date_datetime_disparity': 'date_datetime_disparity'}


desired_columns_dict = {
    # field population metrics
    'measurement_units': ['total_unit_success_rate'],

    'drug_routes': ['total_route_success_rate'],

    # integration metrics
    'drug_success': [
        'ace_inhibitors', 'painnsaids', 'msknsaids',
        'statins', 'antibiotics', 'opioids', 'oralhypoglycemics',
        'vaccine', 'ccb', 'diuretics', 'all_drugs'],

    'sites_measurement': [
        'Physical_Measurement',	'CMP', 'CBCwDiff',
        'CBC', 'Lipid',	'All_Measurements'],

    # ACHILLES errors
    'end_before_begin': [
        'visit_occurrence', 'condition_occurrence',
        'drug_exposure'],

    'data_after_death': [
        'visit_occurrence', 'condition_occurrence',
        'drug_exposure', 'measurement',
        'procedure_occurrence', 'observation'],

    # other metrics
    'concept': [
        'observation_success_rate', 'drug_success_rate',
        'procedure_success_rate', 'condition_success_rate',
        'measurement_success_rate', 'visit_success_rate'],

    'duplicates': ['visit_occurrence', 'condition_occurrence',
                   'drug_exposure', 'measurement',
                   'procedure_occurrence',
                   'observation'],

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
        'procedure_occurrence', 'observation']
}

table_or_class_based_on_column_provided = {
    'total_unit_success_rate': 'Measurement',
    'measurement_success_rate': 'Measurement',
    'measurement': 'Measurement',

    'total_route_success_rate': 'Drug Exposure',
    'drug_exposure': 'Drug Exposure',
    'drug_success_rate': 'Drug Exposure',


    'condition_occurrence': 'Condition Occurrence',
    'condition_success_rate': 'Condition Occurrence',

    'procedure_occurrence': 'Procedure Occurrence',

    'observation': 'Observation',
    'observation_success_rate': 'Observation',

    'procedure_success_rate': 'Procedure',

    'visit_success_rate': 'Visit Occurrence',
    'visit_occurrence': 'Visit Occurrence',

    'ace_inhibitors': 'ACE Inhibitors',
    'painnsaids': "Pain NSAIDs",
    "msknsaids": "MSK NSAIDs",
    "statins": "Statins",
    "antibiotics": "Antibiotics",
    "opioids": "Opioids",
    "oralhypoglycemics": "Oral Hypoglycemics",
    "vaccine": "Vaccines",
    "ccb": "Calcium Channel Blockers",
    "diuretics": "Diuretics",
    'all_drugs': 'All Drugs',

    'Physical_Measurement': "Physical Measurement",
    'CMP': "Comprehensive Metabolic Panel (CMP)",
    'CBCwDiff': "Complete Blood Count (CBC) with Differential",
    'CBC': "Complete Blood Count (CBC)",
    'Lipid': "Lipid",
    'All_Measurements': "All Measurements"
}

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
    'person_id_failure_rate': 'Conformance'
}


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

    # other metrics
    'concept': 'Concept ID Success Rate',
    'duplicates': 'Duplicate Records',

    'erroneous_dates': 'Erroneous Dates',
    'date_datetime_disparity': 'Date/Datetime Disparity',
    'person_id_failure_rate': 'Person ID Failure Rate'
}


english_to_metric_type_dict = {
    # field population metrics
    'Unit Concept ID Success Rate': 'measurement_units',
    'Route Concept ID Success Rate': 'drug_routes',

    # integration metrics
    'Drug Ingredient Integration': 'drug_success',
    'Measurement Integration': 'sites_measurement',

    # ACHILLES errors
    'End Dates Preceding Start Dates': 'end_before_begin',
    'Data After Death': 'data_after_death',

    # other metrics
    'Concept ID Success Rate': 'concept',
    'Duplicate Records': 'duplicates',

    'Erroneous Dates': 'erroneous_dates',
    'Date/Datetime Disparity': 'date_datetime_disparity',
    'Person ID Failure Rate': 'person_id_failure_rate'
}
