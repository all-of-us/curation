"""
Integration test for cleaning_rules.null_invalid_foreign_keys.py module

Original Issue - DC-1169
"""

# Python imports
import os
from mock import patch

# Third party imports
from dateutil import parser

# Project imports
from app_identity import PROJECT_ID
from common import (AOU_DEATH, CARE_SITE, LOCATION, PERSON,
                    PROCEDURE_OCCURRENCE, PROVIDER, VISIT_OCCURRENCE)
from cdr_cleaner.cleaning_rules.null_invalid_foreign_keys import NullInvalidForeignKeys
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest

sandbox_table_names = [AOU_DEATH, PROCEDURE_OCCURRENCE, PERSON]


class NullInvalidForeignKeysTest(BaseTest.CleaningRulesTestBase):

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
        dataset_id = os.environ.get('UNIONED_DATASET_ID')
        cls.dataset_id = dataset_id
        sandbox_id = f'{dataset_id}_sandbox'
        cls.sandbox_id = sandbox_id

        cls.rule_instance = NullInvalidForeignKeys(project_id, dataset_id,
                                                   sandbox_id)

        selected_table_names = [
            PROVIDER, VISIT_OCCURRENCE, LOCATION, CARE_SITE,
            PROCEDURE_OCCURRENCE, PERSON, AOU_DEATH
        ]

        for table_name in sandbox_table_names:
            sandbox_table_name = cls.rule_instance.sandbox_table_for(table_name)
            cls.fq_sandbox_table_names.append(
                f'{cls.project_id}.{cls.sandbox_id}.{sandbox_table_name}')

        cls.fq_table_names = [
            f'{project_id}.{cls.dataset_id}.{table_name}'
            for table_name in selected_table_names
        ]

        # call super to set up the client, create datasets, and create
        # empty test tables
        # NOTE:  does not create empty sandbox tables.
        super().setUpClass()

    def setUp(self):
        """
        Create common information for tests.

        Creates common expected parameter types from cleaned tables and
        a common fully qualified dataset name string used to load
        the data.
        """

        self.date = parser.parse('2020-03-06').date()
        self.datetime = parser.parse('2020-03-06 11:00:00 UTC')

        fq_dataset_name = self.fq_table_names[0].split('.')
        self.fq_dataset_name = '.'.join(fq_dataset_name[:-1])

        super().setUp()

    @patch.object(NullInvalidForeignKeys, 'get_affected_tables')
    def test_get_query_specs(self, mock_get_affected_tables):
        """
        Tests that the specifications for the SANDBOX_QUERY and INVALID_FOREIGN_KEY_QUERY
        perform as designed.

        Validates pre conditions, tests execution, and post conditions based on the load
        statements and the tables_and_counts variable.
        """
        # mocks the return value of get_affected_tables as we only want to loop through the
        # procedure_occurrence, person, and aou_death tables, not all of the CDM tables
        mock_get_affected_tables.return_value = sandbox_table_names

        provider = self.jinja_env.from_string("""
        INSERT INTO `{{fq_dataset_name}}.provider` (provider_id)
        VALUES (1)""").render(fq_dataset_name=self.fq_dataset_name)

        visit_occurrence = self.jinja_env.from_string("""
        INSERT INTO `{{fq_dataset_name}}.visit_occurrence`
        (visit_occurrence_id, person_id, visit_concept_id, visit_start_date, visit_end_date, visit_type_concept_id)
        VALUES (2, 666, 0, '2023-01-01', '2023-01-01', 0)""").render(
            fq_dataset_name=self.fq_dataset_name)

        location = self.jinja_env.from_string("""
        INSERT INTO `{{fq_dataset_name}}.location` (location_id)
        VALUES (3)""").render(fq_dataset_name=self.fq_dataset_name)

        care_site = self.jinja_env.from_string("""
        INSERT INTO `{{fq_dataset_name}}.care_site` (care_site_id)
        VALUES (4)""").render(fq_dataset_name=self.fq_dataset_name)

        # load statements for tables to be cleaned through cleaning rule (procedure_occurrence, person)
        procedure_occurrence = self.jinja_env.from_string(
            """
        INSERT INTO `{{fq_dataset_name}}.procedure_occurrence`
            (procedure_occurrence_id, person_id, procedure_concept_id, procedure_date, procedure_datetime, 
            procedure_type_concept_id, provider_id, visit_occurrence_id)
        VALUES
            -- invalid person_id foreign key. will be sandboxed and deleted --
            (111, 0, 101, date('2020-03-06'), timestamp('2020-03-06 11:00:00'), 101, 1, 2),

            -- invalid provider_id foreign key. will be sandboxed and nulled --
            (222, 555, 101, date('2020-03-06'), timestamp('2020-03-06 11:00:00'), 101, 0, 2),

            -- invalid visit_occurrence_id foreign key, will be sandboxed and nulled --
            (333, 666, 101, date('2020-03-06'), timestamp('2020-03-06 11:00:00'), 101, 1, 0),

            -- NULL visit_occurrence_id and valid provider_id. nothing will be sandboxed or nulled --
            (443, 777, 101, date('2020-03-06'), timestamp('2020-03-06 11:00:00'), 101, 1, NULL),

            -- all foreign keys valid. nothing will be sandboxed or nulled --
            (444, 777, 101, date('2020-03-06'), timestamp('2020-03-06 11:00:00'), 101, 1, 2)"""
        ).render(fq_dataset_name=self.fq_dataset_name)

        person = self.jinja_env.from_string("""
        INSERT INTO `{{fq_dataset_name}}.person`
            (person_id, gender_concept_id, year_of_birth, race_concept_id, ethnicity_concept_id, location_id, 
            provider_id, care_site_id)
        VALUES
            -- invalid location_id foreign key. will be sandboxed and nulled --
            (555, 101, 1985, 101, 101, 0, 1, 4),

            -- invalid care_site_id foreign key. will be sandboxed and nulled --
            (666, 101, 1989, 101, 101, 3, 1, 0),

            -- invalid location_id AND provider_id foreign key. will be sandboxed and nulled --
            (667, 101, 1989, 101, 101, 999, 999, 4),

            -- all foreign keys valid. nothing will be sandboxed or nulled --
            (777, 101, 1995, 101, 101, 3, 1, 4)""").render(
            fq_dataset_name=self.fq_dataset_name)

        aou_death = self.jinja_env.from_string("""
        INSERT INTO `{{fq_dataset_name}}.aou_death`
            (aou_death_id, person_id, death_date, death_type_concept_id, cause_concept_id, cause_source_concept_id, src_id, primary_death_record)
        VALUES
            -- Valid person_id. Nothing happens. --
            ('a1', 555, date('2020-05-05'), 0, 0, 0, 'rdr', False),
            ('a2', 555, date('2021-05-05'), 0, 0, 0, 'hpo_b', False),
            -- Invalid person_id. Deleted --
            ('a3', 9999, date('2020-05-05'), 0, 0, 0, 'rdr', False),
            ('a4', 9999, date('2021-05-05'), 0, 0, 0, 'hpo_b', False)
        """).render(fq_dataset_name=self.fq_dataset_name)

        self.load_test_data([
            provider, visit_occurrence, location, care_site,
            procedure_occurrence, person, aou_death
        ])

        tables_and_counts = [
            {
                'fq_table_name':
                    '.'.join([self.fq_dataset_name, PROCEDURE_OCCURRENCE]),
                'fq_sandbox_table_name':
                    self.fq_sandbox_table_names[1],
                'fields': [
                    'procedure_occurrence_id', 'person_id',
                    'procedure_concept_id', 'procedure_date',
                    'procedure_datetime', 'procedure_type_concept_id',
                    'provider_id', 'visit_occurrence_id'
                ],
                'loaded_ids': [111, 222, 333, 443, 444],
                'sandboxed_ids': [111, 222, 333],
                'cleaned_values': [
                    # cleaned invalid test values
                    (222, 555, 101, self.date, self.datetime, 101, None, 2),
                    (333, 666, 101, self.date, self.datetime, 101, 1, None),
                    # valid test values, no changes
                    (443, 777, 101, self.date, self.datetime, 101, 1, None),
                    (444, 777, 101, self.date, self.datetime, 101, 1, 2)
                ]
            },
            {
                'fq_table_name':
                    '.'.join([self.fq_dataset_name, PERSON]),
                'fq_sandbox_table_name':
                    self.fq_sandbox_table_names[2],
                'fields': [
                    'person_id', 'gender_concept_id', 'year_of_birth',
                    'race_concept_id', 'ethnicity_concept_id', 'location_id',
                    'provider_id', 'care_site_id'
                ],
                'loaded_ids': [555, 666, 667, 777],
                'sandboxed_ids': [555, 666, 667],
                'cleaned_values': [
                    # cleaned invalid test values
                    (555, 101, 1985, 101, 101, None, 1, 4),
                    (666, 101, 1989, 101, 101, 3, 1, None),
                    (667, 101, 1989, 101, 101, None, None, 4),
                    # valid test values, no changes
                    (777, 101, 1995, 101, 101, 3, 1, 4)
                ]
            },
            {
                'fq_table_name': '.'.join([self.fq_dataset_name, AOU_DEATH]),
                'fq_sandbox_table_name': self.fq_sandbox_table_names[0],
                'fields': ['aou_death_id', 'person_id'],
                'loaded_ids': ['a1', 'a2', 'a3', 'a4'],
                'sandboxed_ids': ['a3', 'a4'],
                'cleaned_values': [('a1', 555), ('a2', 555)]
            }
        ]

        self.default_test(tables_and_counts)
