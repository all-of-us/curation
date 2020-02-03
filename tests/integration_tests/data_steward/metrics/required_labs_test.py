import unittest
import os
import gcs_utils
import bq_utils
import common
import app_identity
import resources
from tests import test_util
from tests.test_util import (FAKE_HPO_ID)
from validation.metrics import required_labs as required_labs
import validation.sql_wrangle as sql_wrangle


class RequiredLabsTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.hpo_bucket = gcs_utils.get_hpo_bucket(FAKE_HPO_ID)
        self.project_id = app_identity.get_application_id()
        self.dataset_id = bq_utils.get_dataset_id()
        test_util.delete_all_tables(self.dataset_id)

        required_labs.load_required_lab_table(
            project_id=app_identity.get_application_id(),
            dataset_id=bq_utils.get_dataset_id())

        if not bq_utils.table_exists(common.CONCEPT, self.dataset_id):
            bq_utils.create_standard_table(common.CONCEPT, common.CONCEPT)
        if not bq_utils.table_exists(common.CONCEPT, self.dataset_id):
            bq_utils.create_standard_table(common.CONCEPT_ANCESTOR,
                                           common.CONCEPT_ANCESTOR)

    def tearDown(self):
        test_util.delete_all_tables(bq_utils.get_dataset_id())
        test_util.empty_bucket(self.hpo_bucket)

    def test_load_required_lab_table(self):

        query = sql_wrangle.qualify_tables(
            """SELECT * FROM {table_id}""".format(
                table_id=required_labs.MEASUREMENT_CONCEPT_SETS_TABLE))
        response = bq_utils.query(query)

        actual_fields = [{
            'name': field['name'].lower(),
            'type': field['type'].lower()
        } for field in response['schema']['fields']]

        expected_fields = [{
            'name': field['name'].lower(),
            'type': field['type'].lower()
        } for field in required_labs.MEASUREMENT_CONCEPT_SETS_FIELDS]

        self.assertListEqual(expected_fields, actual_fields)

        measurement_concept_sets_table_path = os.path.join(
            resources.resource_path,
            required_labs.MEASUREMENT_CONCEPT_SETS_TABLE + '.csv')
        expected_total_rows = len(
            resources.csv_to_list(measurement_concept_sets_table_path))
        self.assertEqual(expected_total_rows, int(response['totalRows']))

    def test_load_measurement_concept_sets_descendants_table(self):

        required_labs.load_measurement_concept_sets_descendants_table(
            project_id=self.project_id, dataset_id=self.dataset_id)

        query = sql_wrangle.qualify_tables(
            """SELECT * FROM {table_id}""".format(
                table_id=required_labs.
                MEASUREMENT_CONCEPT_SETS_DESCENDANTS_TABLE))
        response = bq_utils.query(query)

        actual_fields = [{
            'name': field['name'].lower(),
            'type': field['type'].lower()
        } for field in response['schema']['fields']]

        expected_fields = [{
            'name': field['name'].lower(),
            'type': field['type'].lower()
        } for field in required_labs.MEASUREMENT_CONCEPT_SETS_DESCENDANTS_FIELDS
                          ]

        self.assertListEqual(expected_fields, actual_fields)

    def test_get_lab_concept_summary_query(self):
        pass
