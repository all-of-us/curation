"""
Integration test for clean height and weight cleaning rule.

Original Issues: DC-416

Normalizes all height and weight data into cm and kg and removes invalid/implausible data points (rows).
The intent of this cleaning rule is to delete zero/null/implausible height/weight rows
and inserting normalized rows (cm and kg). This cleaning rule also expects `measurement_ext` table to be
present in the dataset

Test Data is generated using the following conditions:

CLEAN HEIGHT

1. “Flag” and “Clean” any implausible height values (plausible values are 90 cm – 230 cm):
    a. If height value is 0.9 – 2.3, assume it is in meters and multiply by 100.
    b.	If height value is 3.0 – 7.5, assume it is in feet and multiply by (12 x 2.54) =30.48.
    c.	If height value is 36.0 – 89.9, assume it is in inches and multiply by 2.54.

2.   “Flag”  height values that are outliers:
    a.  Calculate median height for all records.
    b.  “Flag” those heights that differ more than 3% from the median, UNLESS,
        i. Subject has osteroperosis (1 or more ICD- 9 codes 733.* OR 1 or more ICD- 10 codes M80.*, M81.* ),
         spinal stenosis (1 or more ICD-9 codes 724.0*, 724.1, 724.2, 724.3, 724.4 OR 1 or more ICD-10 codes M48.0*,
         arthroplasty (1 or more ICD- 9 Codes 81.51; 81.52; 81.53, 81.54, 81.55 OR  1 or more ICD-10 codes 0SR9*, 0SRA*,
          0SRB*, 0SRE* , 0SRR*,0SRS* , 0SW9*, 0SWB*, 0SRC*, 0SRD*,0SRT*, 0SRU*, 0SRV*, 0SRW*, 0SWC*, 0SWD*),
         amputation of lower limb (1 or more ICD-9 codes 897.* OR  1 or more ICD-10 codes S78.*, S88.*), OR
         wheel-chair bound (1 or more ICD-9 codes V46.3 OR 1 or more ICD-10 codes Z99.3).
    c.  “Flag” values for individuals for whom there are only 2 height measurements and are in disagreement;
     disagreement = weight standard deviations greater than 10 away from the mean (median).

CLEAN WEIGHT

1.  Clean weight data (i.e. fix high variability due to assigning weight in pounds and not kgs).
    a.  Any weight that is negative make these weights Positive.
    b.  Calculate median weight for all records.
    c.  Any weight greater than 1.5 x the median weight is assumed to be in lbs and not kgs – do conversion.
    d.  Recalculate median weight for only those records which have been cleaned/converted (see step 1 b).
    e.  “Flag” values in cleaned/converted records.  Weight must be > 30 kg and < 250 kg
        (Original range from Schildcrout Method), UNLESS
        i. If subject is diagnosed as having dwarfism (1 or more ICD-9 codes 259.4, 253.3 OR 1 or more ICD-10 codes
         E23.0, E34.3) OR If subject is diagnosed as having anorexia or extreme weight loss (1 or more ICD 9 codes
          783.0, 783.2* OR 1 or more ICD-10 codes R63.0, R63.5, R63.4, R36.6),   THEN use the lower  range >20kg
        ii. If subject is diagnosed with extreme obesity or extreme weight gain (1 or more ICD codes 278.01, 783.1 OR 1
         or more ICD-10 codes E66.01, R63.5), THEN use the upper range >30kg and <450kg
        iii. If subject has 3 or more weights that are >250, THEN use the following range >30kg and <450kg

2.  “Flag” non-pregnant weight values that are outliers:
    a.  “Flag” weight values that represent a loss/gain of > 33%
        i. If we have >2 years of data, calculate median from above cleaned weight for every 2 years.  ‘Flag” weight
         values that represent a loss/gain of > 33% of that 2 year median
        ii. If we have <=2 years of data, calculate median from above cleaned weight.  ‘Flag” weight values that
        represent a loss/gain of > 33% of median.
    b.  “Flag” values for individuals for whom there are only 2 weight measurements but the values are in disagreement;
        disagreement = weight standard deviations greater than 10.
    c.  For individuals for whom there are multiple conflicting weight measurements on the same day, flag the weight
        measure if the value is equal the previous weight measurement.
    d.  Within individual patient records, “flag” values that appear out of place (i.e. weight goes up/down by > 12%
        for the median of weights over 21 days1, > 14% for the median of weights over 30 days, 20% for the median
        of weights over 60 days)
"""

# Python Imports
import os

# Third party imports
from google.api_core.exceptions import ClientError

import constants.cdr_cleaner.clean_cdr as cdr_consts
from app_identity import PROJECT_ID
from cdr_cleaner.clean_cdr_engine import generate_job_config
from cdr_cleaner.cleaning_rules.clean_height_weight import (
    CleanHeightAndWeight,)
from common import JINJA_ENV
# Project Imports
from common import MEASUREMENT
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import \
    BaseTest

MEASUREMENT_DATA_TEMPLATE = JINJA_ENV.from_string("""
    INSERT INTO `{{project_id}}.{{dataset_id}}.measurement` (
      measurement_id,
      person_id,
      measurement_concept_id,
      measurement_date,
      measurement_datetime,
      measurement_type_concept_id,
      operator_concept_id,
      value_as_number,
      value_as_concept_id,
      unit_concept_id,
      range_low,
      range_high,
      provider_id,
      visit_occurrence_id,
      measurement_source_value,
      measurement_source_concept_id,
      unit_source_value,
      value_source_value)
    VALUES
       (1,1,3036277,"2017-03-25","2017-03-25 01:00:00 UTC",44818701,4172703,5.6,0,9330,null,null,null,1,"8302-2",3036277,"in",""),
        (2,2,3036277,"2017-10-18","2017-10-18 01:00:00 UTC",44818701,4172703,62.5,0,9330,null,null,null,2,"8302-2",3036277,"in",""),
        (3,3,3036277,"2017-02-07","2017-02-07 01:00:00 UTC",44818701,4172703,1.8,null,8582,null,null,null,3,"8302-2",3036277,"cm",""),
        (4,4,3036277,"2017-01-06","2017-01-06 01:00:00 UTC",44818701,4172703,193.0,0,8582,null,null,null,4,"8302-2",3036277,"cm",""),
        (5,5,3036277,"2018-01-11","2018-01-11 01:00:00 UTC",44818701,4172703,165.0,0,8582,null,null,null,5,"8302-2",3036277,"cm",""),
        (6,5,3036277,"2018-02-07","2018-02-07 01:00:00 UTC",44818701,4172703,165.0,0,8582,null,null,null,6,"8302-2",3036277,"cm",""),
        (7,6,3036277,"2018-11-14","2018-11-14 01:00:00 UTC",44818701,4172703,170.18,0,8582,null,null,null,7,"8302-2",3036277,"cm",""),
        (8,6,3036277,"2017-05-20","2017-05-20 01:00:00 UTC",44818701,4172703,210.0,0,8582,null,null,null,8,"8302-2",3036277,"cm",""),
        (9,7,3036277,"2019-08-20","2019-08-20 01:00:00 UTC",44818701,4172703,120,0,8582,null,null,null,9,"8302-2",3036277,"cm",""),
        (10,8,3036277,"2018-09-10","2018-09-10 01:00:00 UTC",44818701,4172703,125,0,8582,null,null,null,10,"8302-2",3036277,"cm",""),
        (11,9,3036277,"2017-01-11","2017-01-11 01:00:00 UTC",44818701,4172703,173.0,0,8582,null,null,null,11,"8302-2",3036277,"cm",""),
        (12,10,3036277,"2018-02-10","2018-02-10 01:00:00 UTC",44818701,4172703,189.0,0,8582,null,null,null,12,"8302-2",3036277,"cm",""),
        (13,8,3013762,"2018-07-31","2018-07-31 01:00:00 UTC",44818701,4172703,-252.0,0,8739,null,null,null,13,"3141-9", 3025315,"LBS",""),
        (14,7,3025315,"2018-06-11","2018-06-11 01:00:00 UTC",44818701,4172703,-85.729,0,9529,null,null,null,14,"29463-7",3025315,"kg",""),
        (15,3,3025315,"2017-03-17","2017-03-17 01:00:00 UTC",44818702,4172703,250.0,0,8739,null,null,25570,15,"29463-7",3025315,"LBS",""),
        (16,3,3025315,"2017-04-08","2017-04-08 01:00:00 UTC",44818702,4172703,225.0,null,8739,null,null,null,16,"29463-7",3025315,"LBS",""),
        (17,3,3025315,"2018-10-22","2018-10-22 01:00:00 UTC",44818702,4172703,298.0,null,null,null,null,50921,17,"29463-7",3025315,"LBS",""),
        (18,3,3025315,"2018-01-03","2018-01-03 01:00:00 UTC",44818702,4172703,222.0,null,null,null,null,50921,18,"29463-7",3025315,"LBS",""),
        (19,5,3025315,"2018-07-20","2018-07-20 01:00:00 UTC",44818701,4172703,78.8,0,9529,null,null,null,19,"29463-7",3025315,"KG",""),
        (20,10,3025315,"2018-01-17","2018-01-17 01:00:00 UTC",44818701,4172703,83.0,0,9529,null,null,null,20,"29463-7",3025315,"KG",""),
        (21,5,3025315,"2018-06-19","2018-06-19 01:00:00 UTC",44818701,4172703,73.9,0,9529,null,null,null,21,"29463-7",3025315,"KG",""),
        (22,5,3025315,"2017-10-10","2017-10-10 01:00:00 UTC",44818701,4172703,79.4,0,9529,null,null,null,22,"29463-7",3025315,"KG",""),
        (23,10,3025315,"2018-02-01","2018-02-01 01:00:00 UTC",44818701,4172703,82.3,0,9529,null,null,null,24,"29463-7",3025315,"KG",""),
        (24,5,3025315,"2019-11-25","2019-11-25 01:00:00 UTC",44818701,4172703,77.7,0,9529,null,null,null,25,"29463-7",3025315,"KG",""),
        (25,5,3025315,"2019-11-04","2019-11-04 01:00:00 UTC",44818701,4172703,78.0,0,9529,null,null,null,26,"29463-7",3025315,"KG",""),
        (26,10,3025315,"2019-12-09","2019-12-09 01:00:00 UTC",44818701,4172703,85.5,0,9529,null,null,null,27,"29463-7",3025315,"KG",""),
        (27,2,3025315,"2019-10-15","2019-10-15 01:00:00 UTC",44818701,4172703,92.4,0,9529,null,null,null,28,"29463-7",3025315,"KG",""),
        (28,4,3025315,"2017-01-17","2017-01-17 01:00:00 UTC",44818701,4172703,268.0,0,9529,null,null,null,29,"29463-7",3025315,"KG",""),
        (29,4,3025315,"2018-06-19","2018-06-19 01:00:00 UTC",44818701,4172703,274.9,0,9529,null,null,null,30,"29463-7",3025315,"KG",""),
        (30,4,3025315,"2018-10-10","2018-10-10 01:00:00 UTC",44818701,4172703,279.4,0,9529,null,null,null,31,"29463-7",3025315,"KG",""),
        (31,1,3025315,"2018-04-01","2018-04-01 01:00:00 UTC",44818701,4172703,24.9,0,9529,null,null,null,32,"29463-7",3025315,"KG",""),
        (32,6,3025315,"2016-02-01","2016-02-01 01:00:00 UTC",44818701,4172703,102.3,0,9529,null,null,null,32,"29463-7",3025315,"KG",""),
        (33,6,3025315,"2017-11-25","2017-11-25 01:00:00 UTC",44818701,4172703,77.7,0,9529,null,null,null,33,"29463-7",3025315,"KG",""),
        (34,6,3025315,"2018-11-04","2018-11-04 01:00:00 UTC",44818701,4172703,72.0,0,9529,null,null,null,34,"29463-7",3025315,"KG",""),
        (35,6,3025315,"2019-12-09","2019-12-09 01:00:00 UTC",44818701,4172703,72.5,0,9529,null,null,null,35,"29463-7",3025315,"KG",""),
        (36,9,3025315,"2019-10-15","2019-10-15 01:00:00 UTC",44818701,4172703,82.4,0,9529,null,null,null,36,"29463-7",3025315,"KG",""),
        (37,9,3025315,"2017-01-17","2017-01-17 01:00:00 UTC",44818701,4172703,255.0,0,9529,null,null,null,37,"29463-7",3025315,"KG",""),
        (38,4,3025315,"2018-06-19","2018-06-19 01:00:00 UTC",44818701,4172703,67.9,0,9529,null,null,null,38,"29463-7",3025315,"KG",""),
        (39,4,3025315,"2018-06-19","2018-06-19 01:00:00 UTC",44818701,4172703,81.4,0,9529,null,null,null,39,"29463-7",3025315,"KG",""),
        (40,2,3025315,"2018-04-01","2018-04-01 01:00:00 UTC",44818701,4172703,24.9,0,9529,null,null,null,40,"29463-7",3025315,"KG",""),
        (41,2,3025315,"2018-04-20","2018-04-20 01:00:00 UTC",44818701,4172703,40.0,0,9529,null,null,null,40,"29463-7",3025315,"KG","")
""")

CONDITION_OCCURRENCE_DATA_TEMPLATE = JINJA_ENV.from_string("""
    INSERT INTO `{{project_id}}.{{dataset_id}}.condition_occurrence` (
    condition_occurrence_id,
    person_id,
    condition_concept_id,
    condition_start_date,
    condition_start_datetime,
    condition_end_date,
    condition_end_datetime,
    condition_type_concept_id)
    VALUES
        (1,7,80502,"2019-08-20","2019-08-20 01:00:00 UTC",null,null,38000245),
        (2,8,321661,"2018-09-10","2018-09-10 01:00:00 UTC",null,null,38000245),
        (3,1,435928,"2017-08-15","2017-08-15 00:00:00 UTC",null,null,38000245),
        (4,4,434005,"2018-08-03","2018-08-03 05:00:00 UTC",null,null,32020)
""")

MEASUREMENT_EXT_TEMPLATE = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{dataset_id}}.measurement_ext` (
      measurement_id,
      src_id)
VALUES
        (1,"EHR site 111"),
        (2,"EHR site 111"),
        (3,"EHR site 111"),
        (4,"EHR site 111"),
        (5,"EHR site 111"),
        (6,"EHR site 111"),
        (7,"EHR site 111"),
        (8,"EHR site 111"),
        (9,"EHR site 111"),
        (10,"EHR site 111"),
        (11,"EHR site 111"),
        (12,"EHR site 111"),
        (13,"EHR site 111"),
        (14,"EHR site 111"),
        (15,"EHR site 111"),
        (16,"EHR site 111"),
        (17,"EHR site 111"),
        (18,"EHR site 111"),
        (19,"EHR site 111"),
        (20,"EHR site 111"),
        (21,"EHR site 111"),
        (22,"EHR site 111"),
        (23,"EHR site 111"),
        (24,"EHR site 111"),
        (25,"EHR site 111"),
        (26,"EHR site 111"),
        (27,"EHR site 111"),
        (28,"EHR site 111"),
        (29,"EHR site 111"),
        (30,"EHR site 111"),
        (31,"EHR site 111"),
        (32,"EHR site 111"),
        (33,"EHR site 111"),
        (34,"EHR site 111"),
        (35,"EHR site 111"),
        (36,"EHR site 111"),
        (37,"EHR site 111"),
        (38,"EHR site 111"),
        (39,"EHR site 111"),
        (40,"EHR site 111"),
        (41,"EHR site 111")
""")

PERSON_DATA_TEMPLATE = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{dataset_id}}.person` (
      person_id,
      gender_concept_id,
      birth_datetime,
      year_of_birth,
      race_concept_id,
      ethnicity_concept_id)
    VALUES
        (1,8507,"1990-03-03 00:00:00 UTC", 1990, 0, 0),
        (2,8507,"1984-02-08 00:00:00 UTC", 1984, 0, 0),
        (3,8532,"1983-10-29 00:00:00 UTC", 1983, 0, 0),
        (4,8532,"1971-05-31 00:00:00 UTC", 1971, 0, 0),
        (5,8507,"1990-05-07 00:00:00 UTC", 1990, 0, 0),
        (6,8532,"1969-04-29 00:00:00 UTC", 1969, 0, 0),
        (7,8532,"1975-09-23 00:00:00 UTC", 1975, 0, 0),
        (8,8507,"1976-03-17 00:00:00 UTC", 1976, 0, 0),
        (9,8532,"1993-03-20 00:00:00 UTC", 1993, 0, 0),
        (10,8532,"1982-03-08 00:00:00 UTC", 1983, 0, 0)
""")

CONCEPT_TEMPLATE = JINJA_ENV.from_string("""
INSERT INTO
  `{{project_id}}.{{dataset_id}}.concept` (
    concept_id,
    concept_name,
    domain_id,
    vocabulary_id,
    concept_class_id,
    standard_concept,
    concept_code,
    valid_start_date,
    valid_end_date,
    invalid_reason
) 
  SELECT
    concept_id,
    concept_name,
    domain_id,
    vocabulary_id,
    concept_class_id,
    standard_concept,
    concept_code,
    valid_start_date,
    valid_end_date,
    invalid_reason
  FROM
    `{{project_id}}.{{vocab_dataset}}.concept`
""")

CONCEPT_ANCESTOR_TEMPLATE = JINJA_ENV.from_string("""
INSERT INTO
  `{{project_id}}.{{dataset_id}}.concept_ancestor`(
    ancestor_concept_id,
    descendant_concept_id,
    min_levels_of_separation,
    max_levels_of_separation
)
  SELECT
    ancestor_concept_id,
    descendant_concept_id,
    min_levels_of_separation,
    max_levels_of_separation
  FROM
    `{{project_id}}.{{vocab_dataset}}.concept_ancestor`
""")


class CleanHeightWeightTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        # Set the test project identifier
        cls.project_id = os.environ.get(PROJECT_ID)
        cls.vocab_dataset = os.environ.get('VOCABULARY_DATASET')

        # Set the expected test datasets
        cls.dataset_id = os.environ.get('COMBINED_DEID_DATASET_ID')
        cls.sandbox_id = cls.dataset_id + '_sandbox'

        cls.rule_instance = CleanHeightAndWeight(cls.project_id, cls.dataset_id,
                                                 cls.sandbox_id)

        # Generates list of fully qualified table names and their corresponding sandbox table names
        cls.fq_table_names.extend([
            f'{cls.project_id}.{cls.dataset_id}.{MEASUREMENT}',
            f'{cls.project_id}.{cls.dataset_id}.measurement_ext',
            f'{cls.project_id}.{cls.dataset_id}.condition_occurrence',
            f'{cls.project_id}.{cls.dataset_id}.person',
            f'{cls.project_id}.{cls.dataset_id}.concept',
            f'{cls.project_id}.{cls.dataset_id}.concept_ancestor'
        ])
        sandbox_table_names = cls.rule_instance.get_sandbox_tablenames()
        for table in sandbox_table_names:
            cls.fq_sandbox_table_names.append(
                f'{cls.project_id}.{cls.sandbox_id}.{table}')

        # call super to set up the client, create datasets
        cls.up_class = super().setUpClass()

    def setUp(self):
        """
        Create tables with test for the rule to run on
        """
        # Create the Measurement, measurement_ext, condition_occurrence, concept, and concept_ancestor
        # tables required for the test
        super().setUp()

        person_data_query = PERSON_DATA_TEMPLATE.render(
            project_id=self.project_id, dataset_id=self.dataset_id)
        measurement_data_query = MEASUREMENT_DATA_TEMPLATE.render(
            project_id=self.project_id, dataset_id=self.dataset_id)
        measurement_ext_data_query = MEASUREMENT_EXT_TEMPLATE.render(
            project_id=self.project_id, dataset_id=self.dataset_id)
        condition_data_query = CONDITION_OCCURRENCE_DATA_TEMPLATE.render(
            project_id=self.project_id, dataset_id=self.dataset_id)
        concept_data_query = CONCEPT_TEMPLATE.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            vocab_dataset=self.vocab_dataset)
        concept_ancestor_data_query = CONCEPT_ANCESTOR_TEMPLATE.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            vocab_dataset=self.vocab_dataset)

        # Load test data
        self.load_test_data([
            person_data_query, measurement_data_query,
            measurement_ext_data_query, condition_data_query,
            concept_data_query, concept_ancestor_data_query
        ])

    def assert_get_query_specs(self):
        """
        Tests that queries run successfully and sandbox tables are generated
        Note: This does NOT validate logic
        """
        # Ensure the rule generates syntactically correct queries.
        for spec in self.rule_instance.get_query_specs():
            job_config = generate_job_config(self.project_id, spec)
            query = spec.get(cdr_consts.QUERY)
            try:
                self.client.query(query, job_config).result()
            except ClientError as e:
                self.fail(
                    f"The following client error was raised likely due to incorrect query syntax: "
                    f"{e.message}")
        # Ensure the sandbox tables were created
        table_ids = [
            table.table_id for table in self.client.list_tables(self.sandbox_id)
        ]
        for sandbox_table in self.rule_instance.get_sandbox_tablenames():
            self.assertIn(sandbox_table, table_ids)

    def assert_height_and_weight_cleaning(self):
        """
        Tests the Height and weight cleaning for the loaded test data
        """
        # Expected results list
        tables_and_counts = [{
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{MEASUREMENT}',
            'loaded_ids': [
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,
                19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34,
                35, 36, 37, 38, 39, 40, 41
            ],
            'fields': ['measurement_id', 'value_as_number', 'unit_concept_id'],
            'cleaned_values': [(1, 170.688, 8582), (2, 158.75, 8582),
                               (3, 180.0, 8582), (4, 193.0, 8582),
                               (5, 165.0, 8582), (6, 165.0, 8582),
                               (9, 120.0, 8582), (10, 125.0, 8582),
                               (11, 173.0, 8582), (12, 189.0, 8582),
                               (13, 114.30645014968701, 9529),
                               (14, 85.729, 9529),
                               (15, 113.39925610087997, 9529),
                               (16, 102.05933049079198, 9529),
                               (17, 135.17191327224893, 9529),
                               (18, 100.69853941758142, 9529), (19, 78.8, 9529),
                               (20, 83.0, 9529), (21, 73.9, 9529),
                               (22, 79.4, 9529), (23, 82.3, 9529),
                               (24, 77.7, 9529), (25, 78.0, 9529),
                               (26, 85.5, 9529), (27, 41.91236505488524, 9529),
                               (28, 268.0, 9529), (31, 24.9, 9529),
                               (32, 102.3, 9529), (33, 77.7, 9529),
                               (34, 72.0, 9529), (35, 72.5, 9529),
                               (41, 40.0, 9529)]
        }]

        self.default_test(tables_and_counts)

    def test_cleaning_results(self):
        with self.subTest("height_and_weight_cleaning"):
            self.assert_height_and_weight_cleaning()
        with self.subTest("get_query_specs"):
            self.assert_get_query_specs()
