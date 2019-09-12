import os
import unittest

from google.appengine.api.app_identity import app_identity

import bq_utils
import common
import resources
import cdr_cleaner.cleaning_rules.populate_route_ids as populate_route_ids
from tools import retract_data_gcs as rdg
import constants.cdr_cleaner.clean_cdr as cdr_consts


class PopulateRouteIdsTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = app_identity.get_application_id()
        self.dataset_id = bq_utils.get_dataset_id()
        self.route_mappings_list = [{'drug_concept_id': '46719734', 'route_concept_id': '85738921'},
                                    {'drug_concept_id': '86340', 'route_concept_id': '52315'},
                                    {'drug_concept_id': '19082168', 'route_concept_id': '4132161'},
                                    {'drug_concept_id': '19126918', 'route_concept_id': '45956874',
                                     'route_name': 'Inhalation'}]
        self.route_mappings_string = ("(46719734, 85738921), "
                                      "(86340, 52315), "
                                      "(19082168, 4132161), "
                                      "(19126918, 45956874)")
        self.cols = (
            "de.drug_exposure_id, de.person_id, de.drug_concept_id, de.drug_exposure_start_date, "
            "de.drug_exposure_start_datetime, de.drug_exposure_end_date, de.drug_exposure_end_datetime, "
            "de.verbatim_end_date, de.drug_type_concept_id, de.stop_reason, de.refills, de.quantity, "
            "de.days_supply, de.sig, COALESCE(rm.route_concept_id, de.route_concept_id) AS route_concept_id, "
            "de.lot_number, de.provider_id, de.visit_occurrence_id, de.drug_source_value, de.drug_source_concept_id, "
            "de.route_source_value, de.dose_unit_source_value"
        )
        self.drug_exposure_prefix = "de"
        self.route_mappings_prefix = "rm"

    def test_get_mapping_list(self):
        actual = populate_route_ids.get_mapping_list(self.route_mappings_list)
        expected = self.route_mappings_string
        self.assertEqual(actual, expected)

    def test_integration_create_route_mappings_table(self):
        if bq_utils.table_exists(populate_route_ids.ROUTES_TABLE_ID, dataset_id=self.dataset_id):
            bq_utils.delete_table(populate_route_ids.ROUTES_TABLE_ID, dataset_id=self.dataset_id)
        route_mappings_csv = os.path.join(resources.resource_path, populate_route_ids.ROUTES_TABLE_ID + ".csv")
        expected = resources._csv_to_list(route_mappings_csv)
        for result_dict in expected:
            result_dict['drug_concept_id'] = rdg.get_integer(result_dict['drug_concept_id'])
            result_dict['route_concept_id'] = rdg.get_integer(result_dict['route_concept_id'])
            result_dict.pop('drug_concept_name')
            result_dict.pop('route_name')

        populate_route_ids.create_route_mappings_table(self.project_id)
        query = ("SELECT * "
                 "FROM `{project_id}.{dataset_id}.{table_id}`").format(project_id=self.project_id,
                                                                       dataset_id=self.dataset_id,
                                                                       table_id=populate_route_ids.ROUTES_TABLE_ID)
        result = bq_utils.query(query)
        actual = bq_utils.response2rows(result)
        self.assertItemsEqual(actual, expected)

    def test_get_cols_and_prefixes(self):
        expected = self.cols
        actual, drug_prefix, route_prefix = populate_route_ids.get_cols_and_prefixes()
        self.assertEqual(drug_prefix, self.drug_exposure_prefix)
        self.assertEqual(route_prefix, self.route_mappings_prefix)
        self.assertEqual(actual, expected)

    def test_integration_get_route_mapping_queries(self):
        queries = populate_route_ids.get_route_mapping_queries(self.project_id, self.dataset_id)
        expected_query = populate_route_ids.FILL_ROUTE_ID_QUERY.format(
                                                                dataset_id=self.dataset_id,
                                                                project_id=self.project_id,
                                                                drug_exposure_table=common.DRUG_EXPOSURE,
                                                                route_mapping_dataset=self.dataset_id,
                                                                route_mapping_table=populate_route_ids.ROUTES_TABLE_ID,
                                                                cols=self.cols,
                                                                drug_exposure_prefix=self.drug_exposure_prefix,
                                                                route_mapping_prefix=self.route_mappings_prefix)
        self.assertEqual(queries[0][cdr_consts.QUERY], expected_query)
        self.assertEqual(queries[0][cdr_consts.DESTINATION_DATASET], self.dataset_id)
        self.assertEqual(queries[0][cdr_consts.DESTINATION_TABLE], common.DRUG_EXPOSURE)
        self.assertEqual(queries[0][cdr_consts.DISPOSITION], 'WRITE_TRUNCATE')
