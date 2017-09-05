import os

import resources
from validation import main

FAKE_HPO_ID = 'foo'
VALIDATE_HPO_FILES_URL = main.PREFIX + 'ValidateHpoFiles/' + FAKE_HPO_ID
TEST_DATA_PATH = os.path.join(resources.base_path, 'test', 'test_data')
EMPTY_VALIDATION_RESULT = os.path.join(TEST_DATA_PATH, 'empty_validation_result.csv')
ALL_FILES_UNPARSEABLE_VALIDATION_RESULT = os.path.join(TEST_DATA_PATH, 'all_files_unparseable_validation_result.csv')

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
