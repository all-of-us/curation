"""
Integration Test for the cope survey versioning info module.

The intent is to add survey versioning info to the observation_ext table
based on a static file from the RDR team.
"""
# Python imports
import os

# Project imports
from app_identity import PROJECT_ID
from cdr_cleaner.manual_cleaning_rules.survey_version_info import (
    COPESurveyVersionTask)
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest

# Third party imports


class COPESurveyVersionTaskTest(BaseTest.DeidRulesTestBase):

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
        # intended to be run on the deid_base dataset.  The combined dataset
        # environment variable should be guaranteed to exist
        cls.mapping_dataset_id = os.environ.get('COMBINED_DATASET_ID')

        # setting the cope lookup to the same dataset as the qrid map for testing purposes
        # in production, they are likely different datasets
        cls.cope_dataset_id = cls.mapping_dataset_id
        cls.cope_tablename = 'cope_survey_test_data'

        dataset_id = os.environ.get('COMBINED_DEID_DATASET_ID')
        sandbox_id = dataset_id + '_sandbox'

        cls.kwargs.update({
            'qrid_map_dataset_id': cls.mapping_dataset_id,
            'cope_lookup_dataset_id': cls.cope_dataset_id,
            'cope_table_name': cls.cope_tablename
        })

        cls.rule_instance = COPESurveyVersionTask(project_id, dataset_id,
                                                  sandbox_id,
                                                  cls.cope_dataset_id,
                                                  cls.cope_tablename)

        cls.fq_table_names = [
            f"{project_id}.{dataset_id}.observation",
            f"{project_id}.{dataset_id}.observation_ext"
        ]

        cls.dataset_id = dataset_id
        cls.sandbox_id = sandbox_id

        cls.fq_questionnaire_tablename = f'{project_id}.{sandbox_id}._deid_questionnaire_response_map'
        # call super to set up the client, create datasets, and create
        # empty test tables
        # NOTE:  does not create empty sandbox tables.
        super().setUpClass()

    def setUp(self):
        # create tables
        super().setUp()

        # create questionnaire_response_id mapping table
        self.create_questionnaire_response_mapping_table()

        insert_fake_measurements = [
            self.jinja_env.from_string("""
        -- set up observation table data post-deid --
        INSERT INTO `{{project}}.{{dataset}}.observation`
        (observation_id, person_id, observation_concept_id, observation_date,
         observation_type_concept_id, questionnaire_response_id)
        VALUES
          -- represents COPE survey records --
          (801, 337361, 1585899, date('2016-05-01'), 45905771, 100),
          (804, 337361, 1585899, date('2020-11-01'), 45905771, 150),
          -- represents other survey record --
          (802, 337361, 1585899, date('2019-01-01'), 45905771, 200),
          -- represents an EHR observation record --
          (803, 337321, 1585899, date('2019-01-01'), 45905771, null)
        """),
            self.jinja_env.from_string("""
        -- set up observation ext table data post-deid and post-extension table creation --
        INSERT INTO `{{project}}.{{dataset}}.observation_ext`
        (observation_id, src_id, survey_version_concept_id)
        VALUES
          (801, 'PPI/PM', null),
          (802, 'PPI/PM', null),
          (803, 'EHR site 222', null),
          (804, 'PPI/PM', null)
        """),
            self.jinja_env.from_string("""
        -- set up questionnaire response mapping table, a post-deid table --
        INSERT INTO `{{project}}.{{qrid_map_dataset_id}}._deid_questionnaire_response_map`
        (questionnaire_response_id, research_response_id)
        VALUES
          (10, 100),
          (20, 200),
          (30, 150)
        """),
            self.jinja_env.from_string("""
        CREATE OR REPLACE TABLE `{{project}}.{{cope_dataset}}.{{cope_table_name}}` AS (
        SELECT
        -- participant id has not been de-identified by RDR --
        700 AS participant_id,
        -- questionnaire_response_id from RDR has not been de-identified --
        10 AS questionnaire_response_id,
        -- semantic version provided by RDR but not strictly used by curation --
        'V2020.05.06' AS semantic_version,
        -- cope month provided by RDR team --
        'may' AS cope_month
        UNION ALL
        SELECT
        700 AS participant_id,
        30 AS questionnaire_response_id,
        'V2020.11.06' AS semantic_version,
        'nov' AS cope_month)
        """)
        ]

        load_statements = []
        for statement in insert_fake_measurements:
            sql = statement.render(project=self.project_id,
                                   dataset=self.dataset_id,
                                   qrid_map_dataset_id=self.sandbox_id,
                                   cope_dataset=self.cope_dataset_id,
                                   cope_table_name=self.cope_tablename)
            load_statements.append(sql)

        # load the test data
        self.load_test_data(load_statements)

    def test_adding_cope_survey_version_info(self):
        """
        Validates pre-conditions, test execution and post conditions based on the tables_and_counts variable.
        """
        tables_and_counts = [{
            # observation_ext should be updated
            'name':
                self.fq_table_names[1].split('.')[-1],
            'fq_table_name':
                self.fq_table_names[1],
            'fields': ['observation_id', 'src_id', 'survey_version_concept_id'],
            'loaded_ids': [801, 802, 803, 804],
            'cleaned_values': [(801, 'PPI/PM', 2100000002),
                               (802, 'PPI/PM', None),
                               (803, 'EHR site 222', None),
                               (804, 'PPI/PM', 2100000005)]
        }]

        self.default_test(tables_and_counts)

    def tearDown(self):
        cope_survey_table = f"{self.project_id}.{self.cope_dataset_id}.{self.cope_tablename}"
        self.client.delete_table(cope_survey_table, not_found_ok=True)
        super().tearDown()
