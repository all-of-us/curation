"""
Integration Test for the cope survey versioning info module.

The intent is to add survey versioning info to the observation_ext table
based on a static file from the RDR team.
"""
# Python imports
import os

# Project imports
from app_identity import PROJECT_ID
from common import COPE_SURVEY_MAP
from cdr_cleaner.cleaning_rules.deid.survey_version_info import (
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
        cls.project_id = os.environ.get(PROJECT_ID)

        # set the expected test datasets
        # intended to be run on the deid_base dataset.  The combined dataset
        # environment variable should be guaranteed to exist
        cls.mapping_dataset_id = os.environ.get('COMBINED_DATASET_ID')
        cls.deid_questionnaire_response_map_dataset = os.environ.get(
            'RDR_DATASET_ID')

        # setting the cope lookup to the same dataset as the qrid map for testing purposes
        # in production, they are likely different datasets
        cls.cope_dataset_id = cls.mapping_dataset_id

        dataset_id = os.environ.get('COMBINED_DATASET_ID')
        sandbox_id = f'{dataset_id}_sandbox'
        cls.kwargs.update({'clean_survey_dataset_id': cls.cope_dataset_id})

        cls.rule_instance = COPESurveyVersionTask(
            cls.project_id,
            dataset_id,
            sandbox_id,
            clean_survey_dataset_id=cls.cope_dataset_id)

        cls.fq_table_names = [
            f"{cls.project_id}.{dataset_id}.observation",
            f"{cls.project_id}.{dataset_id}.observation_ext",
            f"{cls.project_id}.{cls.cope_dataset_id}.{COPE_SURVEY_MAP}"
        ]

        cls.dataset_id = dataset_id
        cls.sandbox_id = sandbox_id

        cls.fq_questionnaire_tablename = f'{cls.project_id}.{cls.deid_questionnaire_response_map_dataset}._deid_questionnaire_response_map'
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
        -- set up observation table data post-deid  --
        -- this occurs prior to remapping the questionnaire_response_ids --
        INSERT INTO `{{project}}.{{dataset}}.observation`
        (observation_id, person_id, observation_concept_id, observation_date,
         observation_type_concept_id, questionnaire_response_id)
        VALUES
          -- represents COPE survey records --
          (801, 337361, 1585899, date('2016-05-01'), 45905771, 10),
          (804, 337361, 1585899, date('2020-11-01'), 45905771, 30),
          -- represents Minute survey records --
          (805, 337361, 1585899, date('2021-06-10'), 45905771, 40),
          (806, 337361, 1585899, date('2021-06-10'), 45905771, 50),
          -- represents other survey record --
          (802, 337361, 1585899, date('2019-01-01'), 45905771, 20),
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
          (804, 'PPI/PM', null),
          (805, 'PPI/PM', null),
          (806, 'PPI/PM', null)
        """),
            self.jinja_env.from_string("""
        -- set up questionnaire response mapping table, a post-deid table --
        INSERT INTO `{{project}}.{{qrid_map_dataset_id}}._deid_questionnaire_response_map`
        (questionnaire_response_id, research_response_id)
        VALUES
          (10, 100),
          (20, 200),
          (30, 150),
          (40, 250),
          (50, 300)
        """),
            self.jinja_env.from_string("""
        INSERT INTO `{{project}}.{{cope_dataset}}.{{cope_table_name}}`
        (participant_id, questionnaire_response_id, semantic_version, cope_month)
        VALUES
        -- participant id has not been de-identified by RDR --
        -- questionnaire_response_id from RDR has not been de-identified --
        -- semantic version provided by RDR but not strictly used by curation --
        -- cope month provided by RDR team --
        (700, 10, 'V2020.05.06', 'may'),
        (700, 30, 'V2020.11.06', 'nov'),
        (700, 40, 'V2021.06.10', 'vaccine1'),
        (700, 50, 'V2021.10.28', 'vaccine3')
        """)
        ]

        load_statements = []
        for statement in insert_fake_measurements:
            sql = statement.render(project=self.project_id,
                                   dataset=self.dataset_id,
                                   qrid_map_dataset_id=self.
                                   deid_questionnaire_response_map_dataset,
                                   cope_dataset=self.cope_dataset_id,
                                   cope_table_name=COPE_SURVEY_MAP)
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
            'loaded_ids': [801, 802, 803, 804, 805, 806],
            'cleaned_values': [(801, 'PPI/PM', 2100000002),
                               (802, 'PPI/PM', None),
                               (803, 'EHR site 222', None),
                               (804, 'PPI/PM', 2100000005),
                               (805, 'PPI/PM', 905047), (806, 'PPI/PM', 765936)]
        }]

        self.default_test(tables_and_counts)
