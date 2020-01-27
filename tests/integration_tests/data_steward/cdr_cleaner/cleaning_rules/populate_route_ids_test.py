import os
import time
import unittest

import mock

import app_identity
import bq_utils
import common
from cdr_cleaner.cleaning_rules import populate_route_ids
from constants.cdr_cleaner import clean_cdr as cdr_consts


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

        col_exprs = [
            'de.drug_exposure_id', 'de.person_id', 'de.drug_concept_id',
            'de.drug_exposure_start_date', 'de.drug_exposure_start_datetime',
            'de.drug_exposure_end_date', 'de.drug_exposure_end_datetime',
            'de.verbatim_end_date', 'de.drug_type_concept_id', 'de.stop_reason',
            'de.refills', 'de.quantity', 'de.days_supply', 'de.sig',
            'COALESCE(rm.route_concept_id, de.route_concept_id) AS route_concept_id',
            'de.lot_number', 'de.provider_id', 'de.visit_occurrence_id',
            'de.drug_source_value', 'de.drug_source_concept_id',
            'de.route_source_value', 'de.dose_unit_source_value'
        ]
        self.cols = ', '.join(col_exprs)

    def test_integration_create_drug_route_mappings_table(self):
        if bq_utils.table_exists(populate_route_ids.DRUG_ROUTES_TABLE_ID,
                                 dataset_id=self.dataset_id):
            bq_utils.delete_table(populate_route_ids.DRUG_ROUTES_TABLE_ID,
                                  dataset_id=self.dataset_id)

        if not bq_utils.table_exists(
                populate_route_ids.DOSE_FORM_ROUTES_TABLE_ID,
                dataset_id=self.dataset_id):
            populate_route_ids.create_dose_form_route_mappings_table(
                self.project_id, self.dataset_id)

        populate_route_ids.create_drug_route_mappings_table(
            self.project_id, self.dataset_id,
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

    @mock.patch(
        'cdr_cleaner.cleaning_rules.populate_route_ids.create_drug_route_mappings_table'
    )
    @mock.patch(
        'cdr_cleaner.cleaning_rules.populate_route_ids.create_dose_form_route_mappings_table'
    )
    def test_integration_get_route_mapping_queries(
        self, mock_create_dose_form_route_mappings_table,
        mock_create_drug_route_mappings_table):
        result = []
        mock_create_drug_route_mappings_table.return_value = (
            result, populate_route_ids.DRUG_ROUTES_TABLE_ID)
        mock_create_dose_form_route_mappings_table.return_value = (
            result, populate_route_ids.DOSE_FORM_ROUTES_TABLE_ID)
        expected_query = populate_route_ids.FILL_ROUTE_ID_QUERY.format(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            drug_exposure_table=common.DRUG_EXPOSURE,
            route_mapping_dataset_id=self.dataset_id,
            drug_route_mapping_table=populate_route_ids.DRUG_ROUTES_TABLE_ID,
            cols=self.cols,
            drug_exposure_prefix=populate_route_ids.DRUG_EXPOSURE_ALIAS,
            route_mapping_prefix=populate_route_ids.ROUTE_MAPPING_ALIAS)
        queries = populate_route_ids.get_route_mapping_queries(
            self.project_id, self.dataset_id)
        self.assertEqual(queries[0][cdr_consts.QUERY], expected_query)
        self.assertEqual(queries[0][cdr_consts.DESTINATION_DATASET],
                         self.dataset_id)
        self.assertEqual(queries[0][cdr_consts.DESTINATION_TABLE],
                         common.DRUG_EXPOSURE)
        self.assertEqual(queries[0][cdr_consts.DISPOSITION], 'WRITE_TRUNCATE')
