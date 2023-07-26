"""
Integration test for valid_death_dates.py module

This cleaning rule removes data containing death_dates which fall outside of the AoU program dates or
    after the current date.

Original Issue: DC-1376, DC-1206

Ensures that any records that have death_dates falling outside of the AoU program start date or after the current date
    are sandboxed and dropped.
"""

# Python Imports
import os

# Third party imports
from dateutil.parser import parse

# Project imports
from common import AOU_DEATH, DEATH, OBSERVATION, CONCEPT, JINJA_ENV
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.valid_death_dates import ValidDeathDates
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest

DEATH_DATA_QUERY = JINJA_ENV.from_string("""
  INSERT INTO `{{fq_dataset_name}}.death`
      (person_id, death_date, death_type_concept_id)
    VALUES
      -- records will be dropped because death_date is before AoU start date (Jan 1, 2017) --
      (101, '2015-01-01', 1),
      (102, '2016-01-01', 2),
      -- records will be dropped because death_date is in the future --
      (103, DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY), 3),
      -- death_date will be one day in the future --
      (104, DATE_ADD(CURRENT_DATE(), INTERVAL 5 DAY), 4),
      -- death_date will be five days in the future --
      -- records won't be dropped because death_date is between AoU program start date and current date 
        and before first ppi date --
      (105, '2017-01-01', 5),
      (106, '2020-01-01', 6),
      -- records will be dropped because death date is before first ppi date --
      (107, '2019-01-01', 7),
      (108, '2019-01-01', 8),
      -- record should be dropped because a PPI record exists after this date --
      (109, '2020-08-01', 0)
""")

AOU_DEATH_DATA_QUERY = JINJA_ENV.from_string("""
  INSERT INTO `{{fq_dataset_name}}.aou_death`
      (aou_death_id, person_id, death_date, death_type_concept_id, src_id, primary_death_record)
    VALUES
      -- records will be dropped because death_date is before AoU start date (Jan 1, 2017) --
      ('a101', 101, '2015-01-01', 0, 'hpo_a', True),
      ('a102', 102, '2016-01-01', 0, 'hpo_b', True),
      -- records will be dropped because death_date is in the future --
      ('a103', 101, DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY), 0, 'hpo_c', False),
      ('a104', 102, DATE_ADD(CURRENT_DATE(), INTERVAL 5 DAY), 0, 'hpo_d', False),
      -- records won't be dropped because death_date is between AoU program start date and current date 
        and before first ppi date --
      ('a105', 101, '2023-01-01', 0, 'hpo_e', False),
      ('a106', 109, '2023-01-01', 0, 'hpo_f', False),
      -- records will be dropped because death date is before first ppi date --
      ('a107', 101, '2019-01-01', 0, 'hpo_g', False),
      ('a108', 102, '2019-01-01', 0, 'hpo_h', False),
      -- record should be dropped because a PPI record exists after this date --
      ('a109', 109, '2020-08-01', 0, 'hpo_i', True),
      -- record won't be dropped because NULL death_date records should be kept for aou_death --
      ('a110', 109, NULL, 0, 'hpo_e', False)
""")

INSERT_OBSERVATIONS_QUERY = JINJA_ENV.from_string("""
    INSERT INTO `{{fq_dataset_name}}.observation`
        (observation_id, person_id, observation_concept_id, observation_source_concept_id, observation_date, 
        observation_type_concept_id)
    VALUES
        (1, 101, 1585250, 1585250, date('2020-05-05'), 1),
        (2, 102, 1585250, 1585250, date('2020-05-05'), 2),
        (3, 103, 1585250, 1585250, date('2020-05-05'), 3),
        (4, 104, 1585250, 1585250, date('2020-05-05'), 4),
        (5, 105, 1585250, 1585250, date('2016-05-05'), 5),
        (6, 106, 1585250, 1585250, date('2019-05-05'), 6),
        (7, 107, 1585250, 1585250, date('2019-01-03'), 7),
        (8, 108, 1585250, 1585250, date('2019-01-02'), 8),
        (9, 109, 1585250, 1585250, '2020-01-01', 0),
        (10, 109, 1585250, 1585250, '2021-01-01', 0)
""")


class ValidDeathDatesTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        # set the test project identifier
        project_id = os.environ.get(PROJECT_ID)
        cls.project_id = project_id

        # set the expected test datasets
        dataset_id = os.environ.get('COMBINED_DATASET_ID')
        cls.dataset_id = dataset_id
        sandbox_id = dataset_id + '_sandbox'
        cls.sandbox_id = sandbox_id
        cls.vocabulary_id = os.environ.get('VOCABULARY_DATASET')

        cls.rule_instance = ValidDeathDates(project_id, dataset_id, sandbox_id)
        cls.vocab_tables = [CONCEPT]

        cls.fq_sandbox_table_names = [
            f'{cls.project_id}.{cls.sandbox_id}.{cls.rule_instance.sandbox_table_for(DEATH)}',
            f'{cls.project_id}.{cls.sandbox_id}.{cls.rule_instance.sandbox_table_for(AOU_DEATH)}',
        ]

        cls.fq_table_names = [
            f'{project_id}.{cls.dataset_id}.{DEATH}',
            f'{project_id}.{cls.dataset_id}.{AOU_DEATH}',
            f'{project_id}.{cls.dataset_id}.{OBSERVATION}',
            f'{project_id}.{cls.dataset_id}.{CONCEPT}',
        ]

        # call super to set up the client, create datasets, and create
        # empty test tables
        # NOTE:  does not create empty sandbox tables.
        super().setUpClass()

    def setUp(self):
        """
        Create common information for tests.

        Creates common expected parameter types from cleaned tables and a common
        fully qualified (fq) dataset name string to load the data.
        """
        fq_dataset_name = self.fq_table_names[0].split('.')
        self.fq_dataset_name = '.'.join(fq_dataset_name[:-1])

        #Copy all needed vocab tables to the dataset
        for table in self.vocab_tables:
            self.client.copy_table(
                f'{self.project_id}.{self.vocabulary_id}.{table}',
                f'{self.project_id}.{self.dataset_id}.{table}')

        super().setUp()

    def test_valid_death_dates(self):
        """
        Tests that the specifications for the KEEP_VALID_DEATH_DATE_ROWS and SANDBOX_INVALID_DEATH_DATE_ROWS
        perform as designed.

        Validates pre conditions, tests execution, and post conditions based on the load
        statements and the tables_and_counts variable.
        """
        input_death_data = DEATH_DATA_QUERY.render(
            fq_dataset_name=self.fq_dataset_name)

        input_aou_death_data = AOU_DEATH_DATA_QUERY.render(
            fq_dataset_name=self.fq_dataset_name)

        insert_observation_query = INSERT_OBSERVATIONS_QUERY.render(
            fq_dataset_name=self.fq_dataset_name)

        self.load_test_data(
            [input_death_data, input_aou_death_data, insert_observation_query])

        tables_and_counts = [{
            'fq_table_name':
                '.'.join([self.fq_dataset_name, DEATH]),
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[0],
            'loaded_ids': [101, 102, 103, 104, 105, 106, 107, 108, 109],
            'sandboxed_ids': [101, 102, 103, 104, 107, 108, 109],
            'fields': ['person_id', 'death_date', 'death_type_concept_id'],
            'cleaned_values': [(105, parse('2017-01-01').date(), 5),
                               (106, parse('2020-01-01').date(), 6)]
        }, {
            'fq_table_name': '.'.join([self.fq_dataset_name, AOU_DEATH]),
            'fq_sandbox_table_name': self.fq_sandbox_table_names[1],
            'loaded_ids': [
                'a101', 'a102', 'a103', 'a104', 'a105', 'a106', 'a107', 'a108',
                'a109', 'a110'
            ],
            'sandboxed_ids': [
                'a101', 'a102', 'a103', 'a104', 'a107', 'a108', 'a109'
            ],
            'fields': ['aou_death_id'],
            'cleaned_values': [('a105',), ('a106',), ('a110',)]
        }]

        self.default_test(tables_and_counts)
