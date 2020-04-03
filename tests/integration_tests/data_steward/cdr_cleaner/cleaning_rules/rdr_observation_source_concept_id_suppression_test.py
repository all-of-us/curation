"""
Integration Test for the rdr_observation_source_concept_id_suppression module.

Remove three irrelevant observation_source_concept_ids from the RDR dataset.

Original Issue:  DC-734 implements integration tests for DC-529

The intent is to remove PPI records from the observation table in the RDR
export where observation_source_concept_id in (43530490, 43528818, 43530333).
The records for removal should be archived in the dataset sandbox.  It should
also ensure that records that have null values or do not match the specified
ids are not removed.
"""
# Python imports
import os

# Third party imports
from google.cloud import bigquery
import google.api_core.exceptions as exc
import google.cloud.exceptions as gc_exc
from jinja2 import Template

# Project imports
import resources
from app_identity import PROJECT_ID
from constants.bq_utils import WRITE_TRUNCATE
from constants.cdr_cleaner import clean_cdr as clean_consts
from cdr_cleaner.cleaning_rules.rdr_observation_source_concept_id_suppression import ObservationSourceConceptIDRowSuppression
from cdr_cleaner import clean_cdr_engine as engine
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.cleaning_tests_base import BaseTest
from utils import bq


class ObservationSourceConceptIDRowSuppressionTest(
        BaseTest.CleaningRulesTestBase):
    insert_fake_participants_tmpls = [
        Template("""
    INSERT INTO `{{fq_table_name}}` (observation_id, person_id, observation_concept_id, observation_date, observation_type_concept_id, observation_source_concept_id)
    VALUES
      (801, 337361, 1585899, date('2016-05-01'), 45905771, {{age_prediabetes}}),
      (802, 129884, 1585899, date('2016-05-01'), 45905771, {{meds_prediabetes}}),
      (803, 337361, 1585899, date('2016-05-01'), 45905771, {{now_prediabetes}}),
      (804, 129884, 1585899, date('2016-05-01'), 45905771, null),
      (805, 337361, 1585899, date('2016-05-01'), 45905771, 45)
    """)
    ]

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        # set the test project identifier
        cls.project_id = os.environ.get(PROJECT_ID)

        # set the table names, this could also be a list that is zipped and
        # iterated through.
        table_name = 'observation'

        # set the expected test datasets
        cls.dataset_ids = [os.environ.get('RDR_DATASET_ID')]
        for ds_id in cls.dataset_ids:
            cls.sandbox_ids.append(ds_id + '_sandbox')
            # create fully qualified table names for easy use
            cls.fq_table_names.append(f'{cls.project_id}.{ds_id}.{table_name}')

        # specific to this class for easy testing, this is not setting
        # a base class variable
        cls.dataset_id = cls.dataset_ids[0]
        cls.sandbox_id = cls.sandbox_ids[0]
        cls.fq_table_name = f'{cls.project_id}.{cls.dataset_id}.{table_name}'

        # call super to set up the client, create datasets, and create
        # empty test tables
        super().setUpClass()

    def setUp(self):
        """
        Add data to the tables for the rule to run on.
        """
        print(f"{self.__class__.__name__} test setup extending class function")
        self.age_prediabetes = 43530490
        self.meds_prediabetes = 43528818
        self.now_prediabetes = 43530333

        self.sandboxed_ids = [801, 802, 803]
        self.output_ids = [804, 805]

        # create the string(s) to load the data
        for tmpl in self.insert_fake_participants_tmpls:
            query = tmpl.render(fq_table_name=self.fq_table_name,
                                age_prediabetes=self.age_prediabetes,
                                meds_prediabetes=self.meds_prediabetes,
                                now_prediabetes=self.now_prediabetes)
            self.sql_load_statements.append(query)

        self.query_class = ObservationSourceConceptIDRowSuppression(
            self.project_id, self.dataset_id, self.sandbox_id)

        table_name = self.query_class.get_sandbox_tablenames()[0]
        self.fq_sandbox_table = f'{self.project_id}.{self.sandbox_id}.{table_name}'
        ObservationSourceConceptIDRowSuppressionTest.fq_sandbox_table_names = [
            self.fq_sandbox_table
        ]

        super().setUp()

    def test_expected_cleaning_performance(self):
        """
        Test getting the query specifications.

        This should test that the specifications for the query perform
        as designed.  The rule should drop only what it is designed to
        drop.  No more and no less.
        """

        print(f"{self.__class__.__name__} test")
        # pre-conditions
        # validate sandbox table doesn't exist yet
        response = bq.query(f"select count(*) from `{self.fq_sandbox_table}`",
                            self.project_id)
        self.assertRaises(gc_exc.GoogleCloudError, response.result, timeout=15)

        # validate only 5 records exist before starting
        response = bq.query(
            f"select observation_id, observation_source_concept_id from `{self.fq_table_name}`",
            self.project_id)
        result_list = list(response.result())
        self.assertEqual(
            5, len(result_list),
            "The pre-condition query did not return expected number of rows")

        for item in result_list:
            self.assertIn(item[0], self.sandboxed_ids + self.output_ids,
                          "The test data did not load as expected")

        # test
        query_list = self.query_class.get_query_specs()

        # run the queries
        engine.clean_dataset(self.project_id, query_list)

        # post conditions
        # validate three records are dropped
        response = bq.query(
            f"select observation_id from `{self.fq_table_name}`",
            self.project_id)
        result_list = list(response.result())
        self.assertEqual(2, len(result_list))

        rows_kept = []
        for row in result_list:
            rows_kept.append(row[0])

        # assert the contents are equal regardless of the order
        self.assertCountEqual(self.output_ids, rows_kept)

        for row_id in self.sandboxed_ids:
            self.assertNotIn(row_id, rows_kept)

        # validate three records are saved
        response = bq.query(
            f"select observation_id from `{self.fq_sandbox_table}`",
            self.project_id)
        result_list = list(response.result())
        self.assertEqual(3, len(result_list))

        rows_kept = []
        for row in result_list:
            rows_kept.append(row[0])

        # assert the contents are equal regardless of the order
        self.assertCountEqual(self.sandboxed_ids, rows_kept)

        for row_id in self.output_ids:
            self.assertNotIn(row_id, rows_kept)
