"""
Rule should be applied to the RDR export
"""

# Python Imports
import os

# Project Imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.update_family_history_qa_codes import UpdateFamilyHistoryCodes
from common import OBSERVATION
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest


class UpdateFamilyHistoryQACodesTest(BaseTest.CleaningRulesTestBase):

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
        dataset_id = os.environ.get('RDR_DATASET_ID')
        cls.dataset_id = dataset_id
        sandbox_id = dataset_id + '_sandbox'
        cls.sandbox_id = sandbox_id

        # instantiate the rule to test
        cls.rule_instance = UpdateFamilyHistoryCodes(project_id, dataset_id,
                                                     sandbox_id, 'rdr')

        # must set table_namer as a keywork arg for now
        cls.kwargs = {'table_namer': 'rdr'}

        sb_table_names = cls.rule_instance.get_sandbox_tablenames()
        for table_name in sb_table_names:
            cls.fq_sandbox_table_names.append(
                f'{project_id}.{sandbox_id}.{table_name}')

        cls.fq_table_names = [f'{project_id}.{dataset_id}.{OBSERVATION}']

        # call super to set up the client, create datasets, and create
        # empty test tables
        # NOTE:  does not create empty sandbox tables.
        super().setUpClass()

    def setUp(self):
        """
        Add data to the tables for the rule to run on
        """
        self.load_statements = []

        insert_data_tmpl = [
            self.jinja_env.from_string("""
            INSERT INTO
            `{{fq_table_name}}`
            (observation_id,
            person_id,
            observation_concept_id,
            observation_date,
            observation_type_concept_id,
            observation_source_concept_id,
            value_source_concept_id)
            VALUES
            -- These should be sandboxed and updated --
            (1, 11, 43529632, '2018-09-20', 45905771, 43529632, 43529091),
            (2, 22, 43529637, '2019-09-20', 45905771, 43529637, 43529094),
            (3, 33, 43529636, '2020-01-20', 45905771, 43529636, 702787),
            -- These shouldn't change --
            (4, 44, 43529655, '2017-11-20', 45905771, 43529655, 43529090),
            (5, 55, 43529660, '2015-10-13', 45905771, 43529660, 43529093),
            (6, 66, 43529659, '2015-10-13', 45905771, 43529659, 43529088)
            """)
        ]
        for tmpl in insert_data_tmpl:
            query = tmpl.render(fq_table_name=self.fq_table_names[0])
        self.load_statements.append(query)

        super().setUp()

    def test(self):
        """
        Tests that the specifications for the SANDBOX_QUERY and NUMBERS_AS_STRINGS_QUERY
        perform as designed.

        Validates pre conditions, tests execution, and post conditions based on the load
        statements and the tables_and_counts variable.
        """

        self.load_test_data(self.load_statements)

        tables_and_counts = [{
            'fq_table_name':
                self.fq_table_names[0],
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[0],
            'loaded_ids': [1, 2, 3, 4, 5, 6],
            'sandboxed_ids': [1, 2, 3],
            'fields': [
                'observation_id', 'observation_source_concept_id',
                'value_source_concept_id'
            ],
            'cleaned_values': [(1, 43529655, 43529090), (2, 43529660, 43529093),
                               (3, 43529659, 43529088), (4, 43529655, 43529090),
                               (5, 43529660, 43529093), (6, 43529659, 43529088)]
        }]

        self.default_test(tables_and_counts)
