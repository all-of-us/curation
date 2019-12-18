import os
import time
import unittest

import mock

import app_identity
import bq_utils
from cdr_cleaner.cleaning_rules import populate_route_ids


class PopulateRouteIdsTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = app_identity.get_application_id()
        self.dataset_id = bq_utils.get_dataset_id()
        self.route_mapping_prefix = "rm"

    def test_integration_create_drug_route_mappings_table(self):
        if bq_utils.table_exists(populate_route_ids.DRUG_ROUTES_TABLE_ID, dataset_id=self.dataset_id):
            bq_utils.delete_table(populate_route_ids.DRUG_ROUTES_TABLE_ID, dataset_id=self.dataset_id)

        if not bq_utils.table_exists(populate_route_ids.DOSE_FORM_ROUTES_TABLE_ID, dataset_id=self.dataset_id):
            populate_route_ids.create_dose_form_route_mappings_table(self.project_id, self.dataset_id)

        populate_route_ids.create_drug_route_mappings_table(self.project_id,
                                                            self.dataset_id,
                                                            populate_route_ids.DOSE_FORM_ROUTES_TABLE_ID,
                                                            self.route_mapping_prefix)
        time.sleep(10)
        query = ("SELECT COUNT(*) AS n "
                 "FROM `{project_id}.{dataset_id}.{table_id}`").format(
                                                                project_id=self.project_id,
                                                                dataset_id=self.dataset_id,
                                                                table_id=populate_route_ids.DRUG_ROUTES_TABLE_ID)

        result = bq_utils.query(query)
        actual = bq_utils.response2rows(result)
        self.assertGreater(actual[0]["n"], 0)
