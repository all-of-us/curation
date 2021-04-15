# Python Imports
import os
from datetime import datetime

# Project Imports
from app_identity import PROJECT_ID
import cdr_cleaner.cleaning_rules.deid.ct_pid_rid_map as cr
from cdr_cleaner.cleaning_rules.deid.pid_rid_map import LOGGER
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest
from common import DEID_MAP, CONDITION_OCCURRENCE, PERSON


class CtPIDtoRIDTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        # Set the test project identifier
        project_id = os.environ.get(PROJECT_ID)
        cls.project_id = project_id

        # Set the expected test datasets
        # using unioned since we don't declare a deid dataset
        cls.dataset_id = os.environ.get('UNIONED_DATASET_ID')
        cls.sandbox_id = cls.dataset_id + '_sandbox'

        cls.fq_deid_map_table = f'{project_id}.{cls.sandbox_id}.{DEID_MAP}'

        cls.rule_instance = cr.CtPIDtoRID(project_id, cls.dataset_id,
                                          cls.sandbox_id)

        cls.fq_sandbox_table_names = []

        cls.fq_table_names = [
            f'{project_id}.{cls.dataset_id}.{table_id}'
            for table_id in [CONDITION_OCCURRENCE]
        ] + [cls.fq_deid_map_table
            ] + [f'{project_id}.{cls.sandbox_id}.{PERSON}']

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
        self.value_as_number = None

        fq_dataset_name = self.fq_table_names[0].split('.')
        self.fq_dataset_name = '.'.join(fq_dataset_name[:-1])

        super().setUp()

    def test_field_cleaning(self):
        """
        Tests that the specifications for the SANDBOX_QUERY and CLEAN_PPI_NUMERIC_FIELDS_QUERY
        perform as designed.

        Validates pre conditions, tests execution, and post conditions based on the load
        statements and the tables_and_counts variable.
        """

        queries = []
        co_query = self.jinja_env.from_string(
            """
        INSERT INTO `{{fq_dataset_name}}.{{cdm_table}}`
        (condition_occurrence_id,person_id,condition_concept_id,condition_start_date,
        condition_start_datetime,condition_type_concept_id)
        VALUES
            (50001, 1234, 100, date('2020-08-17'), '2020-08-17 15:00:00', 10),
            (50002, 5678, 200, date('2020-08-17'), '2020-08-17 14:00:00', 11),
            (50003, 2345, 500, date('2020-08-17'), '2020-08-17 13:00:00', 12),
            (50004, 6789, 800, date('2020-08-17'), '2020-08-17 12:00:00', 13),
            (50005, 3456, 1000, date('2020-08-17'), '2020-08-17 11:00:00', 14)"""
        ).render(fq_dataset_name=self.fq_dataset_name,
                 cdm_table=CONDITION_OCCURRENCE)
        queries.append(co_query)

        pid_query = self.jinja_env.from_string("""
        INSERT INTO `{{fq_dataset_name}}.person`
        (person_id, gender_concept_id, year_of_birth, race_concept_id, ethnicity_concept_id)
        VALUES
            (1234, 0, 1960, 0, 0),
            (5678, 0, 1970, 0, 0),
            (2345, 0, 1980, 0, 0),
            (6789, 0, 1990, 0, 0),
            (3456, 0, 1965, 0, 0)""").render(
            fq_dataset_name=f'{self.project_id}.{self.sandbox_id}')
        queries.append(pid_query)

        map_query = self.jinja_env.from_string("""
        INSERT INTO `{{fq_table}}`
        (person_id,research_id,shift)
        VALUES
            (1234, 234, 256),
            (5678, 678, 250),
            (2345, 345, 255),
            (6789, 789, 256)""").render(fq_table=self.fq_deid_map_table)
        queries.append(map_query)

        self.load_test_data(queries)

        self.rule_instance.setup_rule(self.client)

        # verify pid 3456 is excluded and logged
        log_module = 'cdr_cleaner.cleaning_rules.deid.pid_rid_map'
        log_level = 'WARNING'
        log_message = 'Records for PIDs [3456] will be deleted since no mapped research_ids found'
        expected_log_msg = f"{log_level}:{log_module}:{log_message}"
        with self.assertLogs(LOGGER, level='WARN') as ir:
            self.rule_instance.inspect_rule(self.client)
        self.assertEqual(ir.output, [expected_log_msg])

        # Expected results list
        tables_and_counts = [{
            'fq_table_name':
                '.'.join([self.fq_dataset_name, CONDITION_OCCURRENCE]),
            'fields': [
                'condition_occurrence_id', 'person_id', 'condition_concept_id',
                'condition_start_date', 'condition_start_datetime',
                'condition_type_concept_id'
            ],
            'loaded_ids': [234, 678, 345, 789, 3456],
            'cleaned_values': [
                (50001, 234, 100, datetime.fromisoformat('2020-08-17').date(),
                 datetime.fromisoformat('2020-08-17 15:00:00+00:00'), 10),
                (50002, 678, 200, datetime.fromisoformat('2020-08-17').date(),
                 datetime.fromisoformat('2020-08-17 14:00:00+00:00'), 11),
                (50003, 345, 500, datetime.fromisoformat('2020-08-17').date(),
                 datetime.fromisoformat('2020-08-17 13:00:00+00:00'), 12),
                (50004, 789, 800, datetime.fromisoformat('2020-08-17').date(),
                 datetime.fromisoformat('2020-08-17 12:00:00+00:00'), 13)
            ]
        }]

        self.default_test(tables_and_counts)
