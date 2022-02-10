"""
DC-2146
"""
# Python imports
import os

# Third party imports

# Project imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.suppress_combined_pfmh_survey import (
    CombinedPersonalFamilyHealthSurveySuppression)
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest


class CombinedPersonalFamilyHealthSurveySuppressionTest(
        BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        cls.insert_fake_participants_tmpls = [
            cls.jinja_env.from_string("""
        INSERT INTO `{{fq_table_name}}`
        (observation_id, person_id, observation_concept_id, observation_date,
         observation_type_concept_id, observation_source_value)
        VALUES
          -- keep --
          (801, 337361, 1585899, '2021-10-31', 45905771, 'infectiousdiseases_urinarytractcurrently'),
          -- drop --
          (802, 129884, 1585899, '2021-11-01', 45905771, 'otherhealthcondition_reactionstoanesthesia_yes'),
          -- keep --
          (803, 337361, 1585899, '2021-10-31', 45905771, null),
          -- keep, b/c not part of either survey and before the cutoff date --
          (804, 129884, 1585899, '2021-10-31', 45905771, 'foo'),
          -- keep, b/c not part of either survey even though after the cutoff date --
          (805, 337361, 1585899, '2021-11-01', 45905771, 'baz')
        """)
        ]
        # set the test project identifier
        project_id = os.environ.get(PROJECT_ID)
        cls.project_id = project_id

        # set the expected test datasets
        dataset_id = os.environ.get('RDR_DATASET_ID')
        cls.dataset_id = dataset_id
        sandbox_id = dataset_id + '_sandbox'
        cls.sandbox_id = sandbox_id

        cls.rule_instance = CombinedPersonalFamilyHealthSurveySuppression(
            project_id, dataset_id, sandbox_id)

        sb_table_names = cls.rule_instance.get_sandbox_tablenames()
        for table_name in sb_table_names:
            cls.fq_sandbox_table_names.append(
                f'{project_id}.{sandbox_id}.{table_name}')

        cls.fq_table_names = [f"{project_id}.{dataset_id}.observation"]
        # call super to set up the client, create datasets, and create
        # empty test tables
        # NOTE:  does not create empty sandbox tables.
        super().setUpClass()

    def setUp(self):
        """
        Add data to the tables for the rule to run on.
        """
        self.load_statements = []
        # create the string(s) to load the data
        for tmpl in self.insert_fake_participants_tmpls:
            query = tmpl.render(fq_table_name=self.fq_table_names[0])
            self.load_statements.append(query)

        super().setUp()

    def test(self):
        """
        Use the default drop rows test function.

        Validates pre-conditions, test execution and post conditions based on
        the load statements and the tables_and_counts variable.
        """
        self.load_test_data(self.load_statements)

        # Using the 0 position because there is only one sandbox table and
        # one affected OMOP table
        tables_and_counts = [{
            'name': self.fq_table_names[0].split('.')[-1],
            'fq_table_name': self.fq_table_names[0],
            'fq_sandbox_table_name': self.fq_sandbox_table_names[0],
            'fields': ['observation_id'],
            'loaded_ids': [801, 802, 803, 804, 805],
            'sandboxed_ids': [802],
            'cleaned_values': [(801,), (803,), (804,), (805,)]
        }]

        self.default_test(tables_and_counts)
