"""
Unit Test for the ensure_date_datetime_consistency module.

Remove any nullable date and/or datetimes in RDR and EHR datasets.

Original Issues: DC-614, DC-509, and DC-432

The intent is to remove any nullable date and/or datetime fields from the death, observation,
procedure_occurrence, observation_period, specimen, visit_occurrence, condition_occurrence,
device_exposure, measurement, and drug_exposure tables.
"""

import unittest
import copy

from cdr_cleaner.cleaning_rules.ensure_date_datetime_consistency import EnsureDateDatetimeConsistency, TABLE_DATES, FIX_DATETIME_QUERY, FIX_NULL_DATETIME_IN_GET_COLS_QUERY
from cdr_cleaner.cleaning_rules import field_mapping
#from cdr_cleaner.cleaning_rules import ensure_date_datetime_consistency as eddc
import constants.cdr_cleaner.clean_cdr as cdr_consts
import constants.bq_utils as bq_consts
import common
from resources import fields_for



class EnsureDateDatetime(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'foo'
        self.dataset_id = 'bar'
        self.sandbox_dataset_id = 'baz'
        self.tables = TABLE_DATES
        # self.condition_table = 'condition_occurrence'
        # self.drug_table = 'drug_exposure'
        # self.device_table = 'device_exposure'
        # self.measurement_table = 'measurement'
        # self.observation_table = 'observation'
        # self.procedure_table = 'procedure_occurrence'
        # self.death_table = 'death'
        # self.specimen_table = 'specimen'
        # self.observation_period_table = 'observation_period'
        # self.visit_table = 'visit_occurrence'
        # self.domain_tables = [self.condition_table,
        #                       self.drug_table,
        #                       self.device_table,
        #                       self.measurement_table,
        #                       self.observation_table,
        #                       self.procedure_table,
        #                       self.death_table,
        #                       self.specimen_table,
        #                       self.observation_period_table,
        #                       self.visit_table]

        self.query_class = EnsureDateDatetimeConsistency(
            self.project_id, self.dataset_id, self.sandbox_dataset_id)

        self.assertEqual(self.query_class.get_project_id(), self.project_id)
        self.assertEqual(self.query_class.get_dataset_id(), self.dataset_id)

    def test_setup_rule(self):
        # test
        self.query_class.setup_rule()

        # no errors are raised, nothing happens

    def test_get_cols(self):
        # pre conditions
        self.assertEqual(self.query_class.get_affected_datasets(),
                         [cdr_consts.RDR, cdr_consts.UNIONED, cdr_consts.COMBINED])

        # test
        result_list = self.query_class.get_cols()


        # post conditions
        table_fields = field_mapping.get_domain_fields(self)
        print(table_fields)

        expected_list = []
        for field in table_fields:
            if field in self.tables:
                expected = FIX_NULL_DATETIME_IN_GET_COLS_QUERY.format(
                    field=field,
                    date_field=self.tables[field]
                )
            else:
                expected = field
            expected_list.append(expected)

        #print(expected_list)
        self.assertEqual(expected_list, result_list)

    # def test_get_cols(self):
    #     for table in eddc.TABLE_DATES:
    #         actual = eddc.get_cols(table)
    #         expected = ', '.join(self.cols[table])
    #         #self.assertEqual(actual, expected)
    #         #print(fields_for(table))
    #         print(expected)

    def test_get_query_specs(self):
        # pre conditions
        self.assertEqual(self.query_class.get_affected_datasets(),
                         [cdr_consts.RDR, cdr_consts.UNIONED, cdr_consts.COMBINED])

        # test
        result_list = self.query_class.get_query_specs()

        # post conditions
        expected_list = []
        for table in TABLE_DATES:
            query = dict()
            query[cdr_consts.QUERY] = FIX_DATETIME_QUERY.format(
                project=self.get_project_id(),
                dataset=self.get_dataset_id(),
                table_id=self.tables,
                cols=self.get_cols())
            query[cdr_consts.DESTINATION_TABLE] = table
            query[cdr_consts.DISPOSITION] = bq_consts.WRITE_TRUNCATE
            query[cdr_consts.DESTINATION_DATASET] = self.get_dataset_id()
            expected_list.append(query)

        print(expected_list)
        self.assertEqual(expected_list, result_list)


        #self.assertEqual(result_list, expected_list)

    # def test_fix_datetime_queries(self):
    #     actual = eddc.get_fix_incorrect_datetime_to_date_queries(
    #         self.project_id, self.dataset_id)
    #     expected = []
    #     for table in eddc.TABLE_DATES:
    #         query = dict()
    #         query[cdr_consts.QUERY] = eddc.FIX_DATETIME_QUERY.format(
    #             project_id=self.project_id,
    #             dataset_id=self.dataset_id,
    #             table_id=table,
    #             cols=', '.join(self.cols[table]))
    #         query[cdr_consts.DESTINATION_TABLE] = table
    #         query[cdr_consts.DISPOSITION] = bq_consts.WRITE_TRUNCATE
    #         query[cdr_consts.DESTINATION_DATASET] = self.dataset_id
    #         expected.append(query)
    #     self.assertListEqual(actual, expected)

    # def setUp(self):
    #     self.maxDiff = None
    #     self.project_id = 'bar_project'
    #     self.dataset_id = 'foo_dataset'
    #     self.cols = {
    #         'death':
    #             (u'person_id', 'death_date',
    #              ' CASE '
    #              'WHEN death_datetime IS NULL'
    #              'THEN CAST(DATETIME(death_date, MAKETIME(00,00,00)) AS TIMESTAMP'
    #              'END AS death_datetime',
    #              'WHEN EXTRACT(DATE FROM death_datetime) = death_date '
    #              'THEN death_datetime'
    #              'ELSE CAST(DATETIME(death_date),',
    #              'EXTRACT(TIME FROM death_datetime)) AS TIMESTAMP '
    #              'END AS death_datetime',
    #              'death_type_concept_id', 'cause_concept_id',
    #              'cause_source_value', 'cause_source_concept_id'),
    #         'observation': (
    #             u'observation_id', 'person_id', 'observation_concept_id',
    #             'observation_date',
    #             ' CASE'
    #             'WHEN observation_datetime IS NULL'
    #             'THEN CAST(DATETIME(observation_date, MAKETIME(00,00,00)) AS TIMESTAMP'
    #             'END AS observation_datetime',
    #             'WHEN EXTRACT(DATE FROM observation_datetime) = observation_date '
    #             'THEN observation_datetime'
    #             'ELSE CAST(DATETIME(observation_date),',
    #             'EXTRACT(TIME FROM observation_datetime)) AS TIMESTAMP '
    #             'END AS observation_datetime',
    #             'observation_type_concept_id', 'value_as_number',
    #             'value_as_string', 'value_as_concept_id',
    #             'qualifier_concept_id', 'unit_concept_id', 'provider_id',
    #             'visit_occurrence_id', 'observation_source_value',
    #             'observation_source_concept_id', 'unit_source_value',
    #             'qualifier_source_value', 'value_source_concept_id',
    #             'value_source_value', 'questionnaire_response_id'),
    #         'procedure_occurrence': (
    #             u'procedure_occurrence_id', 'person_id', 'procedure_concept_id',
    #             'procedure_date',
    #             ' CASE'
    #             'WHEN EXTRACT(DATE FROM procedure_datetime) = procedure_date '
    #             'THEN procedure_datetime '
    #             'ELSE CAST(DATETIME(procedure_date',
    #             'EXTRACT(TIME FROM procedure_datetime)) AS TIMESTAMP'
    #             'END AS procedure_datetime',
    #             'procedure_type_concept_id', 'modifier_concept_id', 'quantity',
    #             'provider_id', 'visit_occurrence_id', 'procedure_source_value',
    #             'procedure_source_concept_id', 'qualifier_source_value'),
    #         'observation_period': (
    #             u'observation_period_id', 'person_id',
    #             'observation_period_start_date', 'observation_period_end_date',
    #             'period_type_concept_id',
    #             ' CASE'
    #             'WHEN observation_period_start_datetime IS NULL'
    #             'THEN CAST(DATETIME(observation_period_start_date, MAKETIME(00,00,00)) AS TIMESTAMP'
    #             'END AS observation_period_start_datetime',
    #             'WHEN EXTRACT(DATE FROM observation_period_start_datetime)'
    #             ' = observation_period_start_date '
    #             'THEN observation_period_start_datetime '
    #             'ELSE CAST(DATETIME(observation_period_start_date),',
    #             'EXTRACT(TIME FROM observation_period_start_datetime)) AS TIMESTAMP'
    #             'END AS observation_period_start_datetime',
    #             ' CASE'
    #             'WHEN observation_period_end_datetime IS NULL'
    #             'THEN CAST(DATETIME(observation_period_end_date, MAKETIME(00,00,00)) AS TIMESTAMP'
    #             'END AS observation_period_end_datetime'
    #             'WHEN EXTRACT(DATE FROM observation_period_end_datetime)'
    #             ' = observation_period_end_date THEN observation_period_end_datetime '
    #             'ELSE CAST(DATETIME(observation_period_end_date)),',
    #             'EXTRACT(TIME FROM observation_period_end_datetime)) AS TIMESTAMP'
    #             'END AS observation_period_end_datetime'),
    #         'specimen': (
    #             u'specimen_id', 'person_id', 'specimen_concept_id',
    #             'specimen_type_concept_id', 'specimen_date',
    #             ' CASE'
    #             'WHEN specimen_datetime IS NULL'
    #             'THEN CAST(DATETIME(specimen_date), MAKETIME(00,00,00)) AS TIMESTAMP'
    #             'END AS specimen_datetime',
    #             'WHEN EXTRACT(DATE FROM specimen_datetime) = specimen_date '
    #             'THEN specimen_datetime ELSE CAST(DATETIME(specimen_date),',
    #             'EXTRACT(TIME FROM specimen_datetime)) AS TIMESTAMP'
    #             'END AS specimen_datetime',
    #             'quantity', 'unit_concept_id', 'anatomic_site_concept_id',
    #             'disease_status_concept_id', 'specimen_source_id',
    #             'specimen_source_value', 'unit_source_value',
    #             'anatomic_site_source_value', 'disease_status_source_value'),
    #         'visit_occurrence': (
    #             u'visit_occurrence_id', 'person_id', 'visit_concept_id',
    #             'visit_start_date',
    #             ' CASE'
    #             'WHEN visit_start_datetime IS NULL'
    #             'THEN CAST(DATETIME(visit_start_date), MAKETIME(00,00,00)) AS TIMESTAMP'
    #             'END AS visit_start_datetime',
    #             'WHEN EXTRACT(DATE FROM visit_start_datetime) = visit_start_date '
    #             'THEN visit_start_datetime ELSE CAST(DATETIME(visit_start_date),',
    #             'EXTRACT(TIME FROM visit_start_datetime)) AS TIMESTAMP'
    #             'END AS visit_start_datetime',
    #             'visit_end_date',
    #             ' CASE'
    #             'WHEN visit_end_datetime IS NULL'
    #             'THEN CAST(DATETIME(visit_end_date), MAKETIME(00,00,00)) AS TIMESTAMP'
    #             'END AS visit_end_datetime',
    #             'WHEN EXTRACT(DATE FROM visit_end_datetime) = visit_end_date '
    #             'THEN visit_end_datetime ELSE CAST(DATETIME(visit_end_date),',
    #             'EXTRACT(TIME FROM visit_end_datetime)) AS TIMESTAMP'
    #             'END AS visit_end_datetime',
    #             'visit_type_concept_id', 'provider_id', 'care_site_id',
    #             'visit_source_value', 'visit_source_concept_id',
    #             'admitting_source_concept_id', 'admitting_source_value',
    #             'discharge_to_concept_id', 'discharge_to_source_value',
    #             'preceding_visit_occurrence_id'),
    #         'condition_occurrence': (
    #             u'condition_occurrence_id', 'person_id', 'condition_concept_id',
    #             'condition_start_date',
    #             ' CASE '
    #             'WHEN EXTRACT(DATE FROM condition_start_datetime) = condition_start_date '
    #             'THEN condition_start_datetime ELSE CAST(DATETIME(condition_start_date, '
    #             'EXTRACT(TIME FROM condition_start_datetime)) AS TIMESTAMP) '
    #             'END AS condition_start_datetime',
    #             'condition_end_date',
    #             ' CASE WHEN condition_end_datetime IS NULL '
    #             'THEN CAST(DATETIME(condition_end_date), MAKETIME(00,00,00)) AS TIMESTAMP'
    #             'END AS condition_end_datetime',
    #             ' CASE'
    #             'WHEN EXTRACT(DATE FROM condition_end_datetime) = condition_end_date '
    #             'THEN condition_end_datetime ELSE CAST(DATETIME(condition_end_date),',
    #             'EXTRACT(TIME FROM condition_end_datetime)) AS TIMESTAMP'
    #             'END AS condition_end_datetime',
    #             'condition_type_concept_id', 'stop_reason', 'provider_id',
    #             'visit_occurrence_id', 'condition_source_value',
    #             'condition_source_concept_id', 'condition_status_source_value',
    #             'condition_status_concept_id'),
    #         'device_exposure': (
    #             u'device_exposure_id', 'person_id', 'device_concept_id',
    #             'device_exposure_start_date',
    #             ' CASE'
    #             'WHEN EXTRACT(DATE FROM device_exposure_start_datetime)'
    #             ' = device_exposure_start_date THEN device_exposure_start_datetime '
    #             'ELSE CAST(DATETIME(device_exposure_start_date',
    #             'EXTRACT(TIME FROM device_exposure_start_datetime)) AS TIMESTAMP '
    #             'END AS device_exposure_start_datetime',
    #             'device_exposure_end_date',
    #             ' CASE'
    #             'WHEN device_exposure_end_datetime IS NULL'
    #             'THEN CAST(DATETIME(device_exposure_end_date), MAKETIME(00,00,00)) AS TIMESTAMP'
    #             'END AS device_exposure_end_datetime',
    #             'WHEN EXTRACT(DATE FROM device_exposure_end_datetime)'
    #             ' = device_exposure_end_date THEN device_exposure_end_datetime '
    #             'ELSE CAST(DATETIME(device_exposure_end_date),'
    #             'EXTRACT(TIME FROM device_exposure_end_datetime)) AS TIMESTAMP',
    #             'END AS device_exposure_end_datetime',
    #             'device_type_concept_id', 'unique_device_id', 'quantity',
    #             'provider_id', 'visit_occurrence_id', 'device_source_value',
    #             'device_source_concept_id'),
    #         'measurement': (
    #             u'measurement_id', 'person_id', 'measurement_concept_id',
    #             'measurement_date',
    #             ' CASE'
    #             'WHEN measurement_datetime IS NULL'
    #             'THEN CAST(DATETIME(measurement_date), MAKETIME(00,00,00)) AS TIMESTAMP'
    #             'END AS measurement_datetime,',
    #             'WHEN EXTRACT(DATE FROM measurement_datetime) = measurement_date '
    #             'THEN measurement_datetime ELSE CAST(DATETIME(measurement_date)'
    #             'EXTRACT(TIME FROM measurement_datetime)) AS TIMESTAMP',
    #             'END AS measurement_datetime',
    #             'measurement_type_concept_id', 'operator_concept_id',
    #             'value_as_number', 'value_as_concept_id', 'unit_concept_id',
    #             'range_low', 'range_high', 'provider_id', 'visit_occurrence_id',
    #             'measurement_source_value', 'measurement_source_concept_id',
    #             'unit_source_value', 'value_source_value'),
    #         'drug_exposure': (
    #             u'drug_exposure_id', 'person_id', 'drug_concept_id',
    #             'drug_exposure_start_date',
    #             ' CASE'
    #             'WHEN EXTRACT(DATE FROM drug_exposure_start_datetime) = drug_exposure_start_date '
    #             'THEN drug_exposure_start_datetime ELSE CAST(DATETIME(drug_exposure_start_date',
    #             'EXTRACT(TIME FROM drug_exposure_start_datetime)) AS TIMESTAMP) '
    #             'END AS drug_exposure_start_datetime', 'drug_exposure_end_date',
    #             'WHEN drug_exposure_end_datetime IS NULL'
    #             'THEN CAST(DATETIME(drug_exposure_end_date), MAKETIME(00,00,00)) AS TIMESTAMP'
    #             'END AS drug_exposure_end_datetime',
    #             ' CASE'
    #             'WHEN EXTRACT(DATE FROM drug_exposure_end_datetime) = drug_exposure_end_date '
    #             'THEN drug_exposure_end_datetime ELSE CAST(DATETIME(drug_exposure_end_date)'
    #             'EXTRACT(TIME FROM drug_exposure_end_datetime)) AS TIMESTAMP',
    #             'END AS drug_exposure_end_datetime',
    #             'verbatim_end_date', 'drug_type_concept_id', 'stop_reason',
    #             'refills', 'quantity', 'days_supply', 'sig', 'route_concept_id',
    #             'lot_number', 'provider_id', 'visit_occurrence_id',
    #             'drug_source_value', 'drug_source_concept_id',
    #             'route_source_value', 'dose_unit_source_value')}

    # def setUp(self):
    #     self.project_id = 'bar_project'
    #     self.dataset_id = 'foo_dataset'
    #     self.cols = {
    #         'death':
    #             (u'person_id', 'death_date',
    #              ' CASE WHEN EXTRACT(DATE FROM death_datetime) = death_date '
    #              'THEN death_datetime ELSE NULL END AS death_datetime',
    #              'death_type_concept_id', 'cause_concept_id',
    #              'cause_source_value', 'cause_source_concept_id'),
    #         'observation': (
    #             u'observation_id', 'person_id', 'observation_concept_id',
    #             'observation_date',
    #             ' CASE WHEN EXTRACT(DATE FROM observation_datetime) = observation_date '
    #             'THEN observation_datetime ELSE NULL END AS observation_datetime',
    #             'observation_type_concept_id', 'value_as_number',
    #             'value_as_string', 'value_as_concept_id',
    #             'qualifier_concept_id', 'unit_concept_id', 'provider_id',
    #             'visit_occurrence_id', 'observation_source_value',
    #             'observation_source_concept_id', 'unit_source_value',
    #             'qualifier_source_value', 'value_source_concept_id',
    #             'value_source_value', 'questionnaire_response_id'),
    #         'procedure_occurrence': (
    #             u'procedure_occurrence_id', 'person_id', 'procedure_concept_id',
    #             'procedure_date',
    #             ' CASE WHEN EXTRACT(DATE FROM procedure_datetime) = procedure_date '
    #             'THEN procedure_datetime ELSE CAST(DATETIME(procedure_date',
    #             'EXTRACT(TIME FROM procedure_datetime)) AS TIMESTAMP) END AS procedure_datetime',
    #             'procedure_type_concept_id', 'modifier_concept_id', 'quantity',
    #             'provider_id', 'visit_occurrence_id', 'procedure_source_value',
    #             'procedure_source_concept_id', 'qualifier_source_value'),
    #         'observation_period': (
    #             u'observation_period_id', 'person_id',
    #             'observation_period_start_date', 'observation_period_end_date',
    #             'period_type_concept_id',
    #             ' CASE WHEN EXTRACT(DATE FROM observation_period_start_datetime)'
    #             ' = observation_period_start_date THEN observation_period_start_datetime '
    #             'ELSE NULL END AS observation_period_start_datetime',
    #             ' CASE WHEN EXTRACT(DATE FROM observation_period_end_datetime)'
    #             ' = observation_period_end_date THEN observation_period_end_datetime '
    #             'ELSE NULL END AS observation_period_end_datetime'),
    #         'specimen': (
    #             u'specimen_id', 'person_id', 'specimen_concept_id',
    #             'specimen_type_concept_id', 'specimen_date',
    #             ' CASE WHEN EXTRACT(DATE FROM specimen_datetime) = specimen_date '
    #             'THEN specimen_datetime ELSE NULL END AS specimen_datetime',
    #             'quantity', 'unit_concept_id', 'anatomic_site_concept_id',
    #             'disease_status_concept_id', 'specimen_source_id',
    #             'specimen_source_value', 'unit_source_value',
    #             'anatomic_site_source_value', 'disease_status_source_value'),
    #         'visit_occurrence': (
    #             u'visit_occurrence_id', 'person_id', 'visit_concept_id',
    #             'visit_start_date',
    #             ' CASE WHEN EXTRACT(DATE FROM visit_start_datetime) = visit_start_date '
    #             'THEN visit_start_datetime ELSE NULL END AS visit_start_datetime',
    #             'visit_end_date',
    #             ' CASE WHEN EXTRACT(DATE FROM visit_end_datetime) = visit_end_date '
    #             'THEN visit_end_datetime ELSE NULL END AS visit_end_datetime',
    #             'visit_type_concept_id', 'provider_id', 'care_site_id',
    #             'visit_source_value', 'visit_source_concept_id',
    #             'admitting_source_concept_id', 'admitting_source_value',
    #             'discharge_to_concept_id', 'discharge_to_source_value',
    #             'preceding_visit_occurrence_id'),
    #         'condition_occurrence': (
    #             u'condition_occurrence_id', 'person_id', 'condition_concept_id',
    #             'condition_start_date',
    #             ' CASE WHEN EXTRACT(DATE FROM condition_start_datetime) = condition_start_date '
    #             'THEN condition_start_datetime ELSE CAST(DATETIME(condition_start_date',
    #             'EXTRACT(TIME FROM condition_start_datetime)) AS TIMESTAMP) '
    #             'END AS condition_start_datetime', 'condition_end_date',
    #             ' CASE WHEN EXTRACT(DATE FROM condition_end_datetime) = condition_end_date '
    #             'THEN condition_end_datetime ELSE NULL END AS condition_end_datetime',
    #             'condition_type_concept_id', 'stop_reason', 'provider_id',
    #             'visit_occurrence_id', 'condition_source_value',
    #             'condition_source_concept_id', 'condition_status_source_value',
    #             'condition_status_concept_id'),
    #         'device_exposure': (
    #             u'device_exposure_id', 'person_id', 'device_concept_id',
    #             'device_exposure_start_date',
    #             ' CASE WHEN EXTRACT(DATE FROM device_exposure_start_datetime)'
    #             ' = device_exposure_start_date THEN device_exposure_start_datetime '
    #             'ELSE CAST(DATETIME(device_exposure_start_date',
    #             'EXTRACT(TIME FROM device_exposure_start_datetime)) AS TIMESTAMP) '
    #             'END AS device_exposure_start_datetime',
    #             'device_exposure_end_date',
    #             ' CASE WHEN EXTRACT(DATE FROM device_exposure_end_datetime)'
    #             ' = device_exposure_end_date THEN device_exposure_end_datetime '
    #             'ELSE NULL END AS device_exposure_end_datetime',
    #             'device_type_concept_id', 'unique_device_id', 'quantity',
    #             'provider_id', 'visit_occurrence_id', 'device_source_value',
    #             'device_source_concept_id'),
    #         'measurement': (
    #             u'measurement_id', 'person_id', 'measurement_concept_id',
    #             'measurement_date',
    #             ' CASE WHEN EXTRACT(DATE FROM measurement_datetime) = measurement_date '
    #             'THEN measurement_datetime ELSE NULL END AS measurement_datetime',
    #             'measurement_type_concept_id', 'operator_concept_id',
    #             'value_as_number', 'value_as_concept_id', 'unit_concept_id',
    #             'range_low', 'range_high', 'provider_id', 'visit_occurrence_id',
    #             'measurement_source_value', 'measurement_source_concept_id',
    #             'unit_source_value', 'value_source_value'),
    #         'drug_exposure': (
    #             u'drug_exposure_id', 'person_id', 'drug_concept_id',
    #             'drug_exposure_start_date',
    #             ' CASE WHEN EXTRACT(DATE FROM drug_exposure_start_datetime) = drug_exposure_start_date '
    #             'THEN drug_exposure_start_datetime ELSE CAST(DATETIME(drug_exposure_start_date',
    #             'EXTRACT(TIME FROM drug_exposure_start_datetime)) AS TIMESTAMP) '
    #             'END AS drug_exposure_start_datetime', 'drug_exposure_end_date',
    #             ' CASE WHEN EXTRACT(DATE FROM drug_exposure_end_datetime) = drug_exposure_end_date '
    #             'THEN drug_exposure_end_datetime ELSE NULL END AS drug_exposure_end_datetime',
    #             'verbatim_end_date', 'drug_type_concept_id', 'stop_reason',
    #             'refills', 'quantity', 'days_supply', 'sig', 'route_concept_id',
    #             'lot_number', 'provider_id', 'visit_occurrence_id',
    #             'drug_source_value', 'drug_source_concept_id',
    #             'route_source_value', 'dose_unit_source_value')
    #     }
