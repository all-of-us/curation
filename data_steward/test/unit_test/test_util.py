import os

import common
import resources
from validation import main

FAKE_HPO_ID = 'fake'
VALIDATE_HPO_FILES_URL = main.PREFIX + 'ValidateHpoFiles/' + FAKE_HPO_ID
TEST_DATA_PATH = os.path.join(resources.base_path, 'test', 'test_data')
EMPTY_VALIDATION_RESULT = os.path.join(TEST_DATA_PATH, 'empty_validation_result.csv')
ALL_FILES_UNPARSEABLE_VALIDATION_RESULT = os.path.join(TEST_DATA_PATH, 'all_files_unparseable_validation_result.csv')
ALL_FILES_UNPARSEABLE_VALIDATION_RESULT_NO_HPO_JSON = os.path.join(TEST_DATA_PATH, 'all_files_unparseable_validation_result_no_hpo.json')
BAD_PERSON_FILE_BQ_LOAD_ERRORS_CSV = os.path.join(TEST_DATA_PATH, 'bq_errors_bad_person.csv')


# Test files for five person sample
FIVE_PERSONS_PATH = os.path.join(TEST_DATA_PATH, 'five_persons')
FIVE_PERSONS_PERSON_CSV = os.path.join(FIVE_PERSONS_PATH, 'person.csv')
FIVE_PERSONS_VISIT_OCCURRENCE_CSV = os.path.join(FIVE_PERSONS_PATH, 'visit_occurrence.csv')
FIVE_PERSONS_CONDITION_OCCURRENCE_CSV = os.path.join(FIVE_PERSONS_PATH, 'condition_occurrence.csv')
FIVE_PERSONS_PROCEDURE_OCCURRENCE_CSV = os.path.join(FIVE_PERSONS_PATH, 'procedure_occurrence.csv')
FIVE_PERSONS_DRUG_EXPOSURE_CSV = os.path.join(FIVE_PERSONS_PATH, 'drug_exposure.csv')
FIVE_PERSONS_MEASUREMENT_CSV = os.path.join(FIVE_PERSONS_PATH, 'measurement.csv')
FIVE_PERSONS_FILES = [FIVE_PERSONS_PERSON_CSV,
                      FIVE_PERSONS_VISIT_OCCURRENCE_CSV,
                      FIVE_PERSONS_CONDITION_OCCURRENCE_CSV,
                      FIVE_PERSONS_PROCEDURE_OCCURRENCE_CSV,
                      FIVE_PERSONS_DRUG_EXPOSURE_CSV,
                      FIVE_PERSONS_MEASUREMENT_CSV]

FIVE_PERSONS_SUCCESS_RESULT_CSV = os.path.join(TEST_DATA_PATH, 'five_persons_success_result.csv')
FIVE_PERSONS_SUCCESS_RESULT_NO_HPO_JSON = os.path.join(TEST_DATA_PATH, 'five_persons_success_result_no_hpo.json')


def _create_five_persons_success_result():
    """
    Generate the expected result payload for five_persons data set. For internal testing only.
    """
    import csv

    field_names = ['cdm_file_name', 'found', 'parsed', 'loaded']

    expected_result_items = []
    for cdm_file in common.CDM_FILES:
        expected_item = dict(cdm_file_name=cdm_file, found="1", parsed="1", loaded="1")
        expected_result_items.append(expected_item)
    with open(FIVE_PERSONS_SUCCESS_RESULT_CSV, 'w') as f:
        writer = csv.DictWriter(f, field_names, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        writer.writerows(expected_result_items)
