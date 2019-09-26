import unittest

from google.appengine.api.app_identity import app_identity

import bq_utils
from cdr_cleaner.cleaning_rules import ensure_date_datetime_consistency as eddc
import constants.cdr_cleaner.clean_cdr as cdr_consts
import constants.bq_utils as bq_consts


class EnsureDateDatetime(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = app_identity.get_application_id()
        self.dataset_id = bq_utils.get_dataset_id()
        self.cols = {
            'death': (u'person_id',
                      'death_date',
                      ' CASE WHEN EXTRACT(DATE FROM death_datetime) = death_date '
                      'THEN death_datetime ELSE NULL END AS death_datetime',
                      'death_type_concept_id',
                      'cause_concept_id',
                      'cause_source_value',
                      'cause_source_concept_id'),
            'observation': (u'observation_id',
                            'person_id',
                            'observation_concept_id',
                            'observation_date',
                            ' CASE WHEN EXTRACT(DATE FROM observation_datetime) = observation_date '
                            'THEN observation_datetime ELSE NULL END AS observation_datetime',
                            'observation_type_concept_id',
                            'value_as_number',
                            'value_as_string',
                            'value_as_concept_id',
                            'qualifier_concept_id',
                            'unit_concept_id',
                            'provider_id',
                            'visit_occurrence_id',
                            'observation_source_value',
                            'observation_source_concept_id',
                            'unit_source_value',
                            'qualifier_source_value',
                            'value_source_concept_id',
                            'value_source_value',
                            'questionnaire_response_id'),
            'procedure_occurrence': (u'procedure_occurrence_id',
                                     'person_id',
                                     'procedure_concept_id',
                                     'procedure_date',
                                     ' CASE WHEN EXTRACT(DATE FROM procedure_datetime) = procedure_date '
                                     'THEN procedure_datetime ELSE CAST(DATETIME(procedure_date',
                                     'EXTRACT(TIME FROM procedure_datetime)) AS TIMESTAMP) END AS procedure_datetime',
                                     'procedure_type_concept_id',
                                     'modifier_concept_id',
                                     'quantity',
                                     'provider_id',
                                     'visit_occurrence_id',
                                     'procedure_source_value',
                                     'procedure_source_concept_id',
                                     'qualifier_source_value'),
            'observation_period': (u'observation_period_id',
                                   'person_id',
                                   'observation_period_start_date',
                                   'observation_period_end_date',
                                   'period_type_concept_id',
                                   ' CASE WHEN EXTRACT(DATE FROM observation_period_start_datetime)'
                                   ' = observation_period_start_date THEN observation_period_start_datetime '
                                   'ELSE NULL END AS observation_period_start_datetime',
                                   ' CASE WHEN EXTRACT(DATE FROM observation_period_end_datetime)'
                                   ' = observation_period_end_date THEN observation_period_end_datetime '
                                   'ELSE NULL END AS observation_period_end_datetime'),
            'specimen': (u'specimen_id',
                         'person_id',
                         'specimen_concept_id',
                         'specimen_type_concept_id',
                         'specimen_date',
                         ' CASE WHEN EXTRACT(DATE FROM specimen_datetime) = specimen_date '
                         'THEN specimen_datetime ELSE NULL END AS specimen_datetime',
                         'quantity',
                         'unit_concept_id',
                         'anatomic_site_concept_id',
                         'disease_status_concept_id',
                         'specimen_source_id',
                         'specimen_source_value',
                         'unit_source_value',
                         'anatomic_site_source_value',
                         'disease_status_source_value'),
            'visit_occurrence': (u'visit_occurrence_id',
                                 'person_id',
                                 'visit_concept_id',
                                 'visit_start_date',
                                 ' CASE WHEN EXTRACT(DATE FROM visit_start_datetime) = visit_start_date '
                                 'THEN visit_start_datetime ELSE NULL END AS visit_start_datetime',
                                 'visit_end_date',
                                 ' CASE WHEN EXTRACT(DATE FROM visit_end_datetime) = visit_end_date '
                                 'THEN visit_end_datetime ELSE NULL END AS visit_end_datetime',
                                 'visit_type_concept_id',
                                 'provider_id',
                                 'care_site_id',
                                 'visit_source_value',
                                 'visit_source_concept_id',
                                 'admitting_source_concept_id',
                                 'admitting_source_value',
                                 'discharge_to_concept_id',
                                 'discharge_to_source_value',
                                 'preceding_visit_occurrence_id'),
            'condition_occurrence': (u'condition_occurrence_id',
                                     'person_id',
                                     'condition_concept_id',
                                     'condition_start_date',
                                     ' CASE WHEN EXTRACT(DATE FROM condition_start_datetime) = condition_start_date '
                                     'THEN condition_start_datetime ELSE CAST(DATETIME(condition_start_date',
                                     'EXTRACT(TIME FROM condition_start_datetime)) AS TIMESTAMP) '
                                     'END AS condition_start_datetime',
                                     'condition_end_date',
                                     ' CASE WHEN EXTRACT(DATE FROM condition_end_datetime) = condition_end_date '
                                     'THEN condition_end_datetime ELSE NULL END AS condition_end_datetime',
                                     'condition_type_concept_id',
                                     'stop_reason',
                                     'provider_id',
                                     'visit_occurrence_id',
                                     'condition_source_value',
                                     'condition_source_concept_id',
                                     'condition_status_source_value',
                                     'condition_status_concept_id'),
            'device_exposure': (u'device_exposure_id',
                                'person_id',
                                'device_concept_id',
                                'device_exposure_start_date',
                                ' CASE WHEN EXTRACT(DATE FROM device_exposure_start_datetime)'
                                ' = device_exposure_start_date THEN device_exposure_start_datetime '
                                'ELSE CAST(DATETIME(device_exposure_start_date',
                                'EXTRACT(TIME FROM device_exposure_start_datetime)) AS TIMESTAMP) '
                                'END AS device_exposure_start_datetime',
                                'device_exposure_end_date',
                                ' CASE WHEN EXTRACT(DATE FROM device_exposure_end_datetime)'
                                ' = device_exposure_end_date THEN device_exposure_end_datetime '
                                'ELSE NULL END AS device_exposure_end_datetime',
                                'device_type_concept_id',
                                'unique_device_id',
                                'quantity',
                                'provider_id',
                                'visit_occurrence_id',
                                'device_source_value',
                                'device_source_concept_id'),
            'measurement': (u'measurement_id',
                            'person_id',
                            'measurement_concept_id',
                            'measurement_date',
                            ' CASE WHEN EXTRACT(DATE FROM measurement_datetime) = measurement_date '
                            'THEN measurement_datetime ELSE NULL END AS measurement_datetime',
                            'measurement_type_concept_id',
                            'operator_concept_id',
                            'value_as_number',
                            'value_as_concept_id',
                            'unit_concept_id',
                            'range_low',
                            'range_high',
                            'provider_id',
                            'visit_occurrence_id',
                            'measurement_source_value',
                            'measurement_source_concept_id',
                            'unit_source_value',
                            'value_source_value'),
            'drug_exposure': (u'drug_exposure_id',
                              'person_id',
                              'drug_concept_id',
                              'drug_exposure_start_date',
                              ' CASE WHEN EXTRACT(DATE FROM drug_exposure_start_datetime) = drug_exposure_start_date '
                              'THEN drug_exposure_start_datetime ELSE CAST(DATETIME(drug_exposure_start_date',
                              'EXTRACT(TIME FROM drug_exposure_start_datetime)) AS TIMESTAMP) '
                              'END AS drug_exposure_start_datetime',
                              'drug_exposure_end_date',
                              ' CASE WHEN EXTRACT(DATE FROM drug_exposure_end_datetime) = drug_exposure_end_date '
                              'THEN drug_exposure_end_datetime ELSE NULL END AS drug_exposure_end_datetime',
                              'verbatim_end_date',
                              'drug_type_concept_id',
                              'stop_reason',
                              'refills',
                              'quantity',
                              'days_supply',
                              'sig',
                              'route_concept_id',
                              'lot_number',
                              'provider_id',
                              'visit_occurrence_id',
                              'drug_source_value',
                              'drug_source_concept_id',
                              'route_source_value',
                              'dose_unit_source_value')
        }

    def test_get_cols(self):
        for table in eddc.TABLE_DATES:
            actual = eddc.get_cols(table)
            expected = ', '.join(self.cols[table])
            self.assertEqual(actual, expected)

    def test_fix_datetime_queries(self):
        actual = eddc.get_remove_records_with_wrong_datetime_queries(self.project_id, self.dataset_id)
        expected = []
        for table in eddc.TABLE_DATES:
            query = dict()
            query[cdr_consts.QUERY] = eddc.FIX_DATETIME_QUERY.format(project_id=self.project_id,
                                                                     dataset_id=self.dataset_id,
                                                                     table_id=table,
                                                                     cols=', '.join(self.cols[table]))
            query[cdr_consts.DESTINATION_TABLE] = table
            query[cdr_consts.DISPOSITION] = bq_consts.WRITE_TRUNCATE
            query[cdr_consts.DESTINATION_DATASET] = self.dataset_id
            expected.append(query)
        self.assertListEqual(actual, expected)
