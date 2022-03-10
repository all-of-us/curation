"""
Rule applied to the RDR export.

DC-1894

A question changed in the nov, dec, and feb cope surveys but the concept code
was not changed correctly.  This rule fixes the concept code for specific surveys.
"""

# Python Imports
import os

# Project Imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.update_cope_flu_concepts import UpdateCopeFluQuestionConcept
from common import COPE_SURVEY_MAP, OBSERVATION
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest


class UpdateCopeFluQuestionConceptTest(BaseTest.CleaningRulesTestBase):

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
        sandbox_id = f'{dataset_id}_sandbox'
        cls.sandbox_id = sandbox_id

        # instantiate the rule to test
        cls.rule_instance = UpdateCopeFluQuestionConcept(
            project_id, dataset_id, sandbox_id, 'rdr')

        # must set table_namer as a keywork arg for now
        cls.kwargs = {'table_namer': 'rdr'}

        sb_table_names = cls.rule_instance.get_sandbox_tablenames()
        for table_name in sb_table_names:
            cls.fq_sandbox_table_names.append(
                f'{project_id}.{sandbox_id}.{table_name}')

        print(cls.fq_sandbox_table_names)

        cls.fq_table_names = [
            f'{project_id}.{dataset_id}.{OBSERVATION}',
            f'{project_id}.{dataset_id}.{COPE_SURVEY_MAP}'
        ]

        # call super to set up the client, create datasets, and create
        # empty test tables
        # NOTE:  does not create empty sandbox tables.
        super().setUpClass()

    def setUp(self):
        """
        Add data to the tables for the rule to run on
        """
        self.load_statements = []

        # define load statements in same order as fq_table_names to make loading easier.
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
            observation_source_value,
            questionnaire_response_id)
            VALUES
            -- These should not be altered --
            (1, 1, 1332742, '2020-05-01', 45905771, 1332742, 'cdc_covid_19_9b', 1),
            (2, 1, 1332742, '2020-06-01', 45905771, 1332742, 'cdc_covid_19_9b', 2),
            -- This one shouldn't be possible in real data.  
            Still making sure it will remain unchanged if encountered. --
            (3, 1, 1332742, '2020-07-01', 45905771, 1332742, 'cdc_covid_19_9b', 3),
            -- These three should be sandboxed and updated. --
            (4, 1, 1332742, '2020-11-01', 45905771, 1332742, 'cdc_covid_19_9b', 4),
            (5, 1, 1332742, '2020-12-01', 45905771, 1332742, 'cdc_covid_19_9b', 5),
            (6, 1, 1332742, '2021-02-01', 45905771, 1332742, 'cdc_covid_19_9b', 6),
            -- Represents answer to another type of survey.  Should remain unchanged. --
            (7, 1, 1234567, '2021-06-01', 45905771, 1234567, 'alpha_beta', 7)
            """),
            self.jinja_env.from_string("""
            INSERT INTO `{{fq_table_name}}`
            (participant_id,
            questionnaire_response_id,
            semantic_version,
            cope_month)
            VALUES
            -- valid uses of cdc_covid_19_9b --
            (1, 1, 'xyz', 'may'),
            (1, 2, 'xya', 'june'),
            (1, 3, 'xyb', 'july'),
            -- invalid uses of cdc_covid_19_9b --
            -- also verifying the check is case insensitive --
            (1, 4, 'xyc', 'Nov'),
            (1, 5, 'xyd', 'dec'),
            (1, 6, 'xye', 'feb'),
            -- minute surveys mixed into the semantic version map --
            (1, 7, 'xyfoo', 'vavoom1')"""),
        ]
        for index, tmpl in enumerate(insert_data_tmpl):
            query = tmpl.render(fq_table_name=self.fq_table_names[index])
            self.load_statements.append(query)

        super().setUp()

    def test(self):
        """
        Tests that the specifications for the sandbox query and update query work as designed.

        Validates pre conditions, tests execution, and post conditions based on the load
        statements and the tables_and_counts variable.
        """
        # load the data
        self.load_test_data(self.load_statements)

        tables_and_counts = [{
            'fq_table_name':
                self.fq_table_names[
                    0
                ],  # only modifying data in Observation, so one test is sufficient
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[0],
            'loaded_ids': [1, 2, 3, 4, 5, 6, 7],
            'sandboxed_ids': [4, 5, 6],
            'fields': [
                'observation_id', 'observation_concept_id',
                'observation_source_concept_id', 'observation_source_value',
                'questionnaire_response_id'
            ],
            'cleaned_values': [(1, 1332742, 1332742, 'cdc_covid_19_9b', 1),
                               (2, 1332742, 1332742, 'cdc_covid_19_9b', 2),
                               (3, 1332742, 1332742, 'cdc_covid_19_9b', 3),
                               (4, 705047, 705047, 'dmfs_27', 4),
                               (5, 705047, 705047, 'dmfs_27', 5),
                               (6, 705047, 705047, 'dmfs_27', 6),
                               (7, 1234567, 1234567, 'alpha_beta', 7)]
        }]

        self.default_test(tables_and_counts)