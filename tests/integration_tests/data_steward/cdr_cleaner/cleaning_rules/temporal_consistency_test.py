"""
Integration test for Temporal Consistency CR implemented in DC-400
"""

# Python imports
import os
from datetime import datetime as dt

# Project imports
import common
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.temporal_consistency import TemporalConsistency, table_dates
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest


class TemporalConsistencyTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        project_id = os.environ.get(PROJECT_ID)
        cls.project_id = project_id

        dataset_id = os.environ.get('UNIONED_DATASET_ID')
        cls.dataset_id = dataset_id
        sandbox_id = dataset_id + '_sandbox'
        cls.sandbox_id = sandbox_id

        cls.rule_instance = TemporalConsistency(project_id, dataset_id,
                                                sandbox_id)

        cls.fq_dataset_id = f'{cls.project_id}.{cls.dataset_id}'
        cls.fq_sandbox_id = f'{cls.project_id}.{cls.sandbox_id}'
        for table_name in list(table_dates.keys()) + [common.VISIT_OCCURRENCE]:
            sandbox_table_name = cls.rule_instance.sandbox_table_for(table_name)
            cls.fq_sandbox_table_names.append(
                f'{cls.project_id}.{cls.sandbox_id}.{sandbox_table_name}')
            cls.fq_table_names.append(
                f'{cls.project_id}.{cls.dataset_id}.{table_name}')

        super().setUpClass()

    def test_temporal_consistency(self):
        queries = []
        co_insert = self.jinja_env.from_string(
            """
            INSERT INTO `{{fq_table_id}}`
            (condition_occurrence_id, person_id, condition_concept_id, condition_start_date,
                condition_start_datetime, condition_end_date, condition_end_datetime,
                condition_type_concept_id, visit_occurrence_id)
            VALUES
            (100, 1, 10, date('2019-05-06'), timestamp('2019-05-06 00:00:00'),
                date('2019-05-19'), timestamp('2019-05-19 00:00:00'), 5, 101),
            (101, 1, 11, date('2019-05-25'), timestamp('2019-05-25 00:00:00'),
                date('2019-05-07'), timestamp('2019-05-07 00:00:00'), 6, 101),
            (102, 1, 12, date('2019-05-06'), timestamp('2019-05-06 00:00:00'),
                date('2019-05-28'), timestamp('2019-05-28 00:00:00'), 7, 101),
            (103, 2, 13, date('2020-10-12'), timestamp('2021-10-12 00:00:00'),
                date('2021-10-12'), timestamp('2021-10-12 00:00:00'), 8, 102),
            (104, 2, 14, date('2021-10-12'), timestamp('2021-10-12 00:00:00'),
                date('2021-09-12'), timestamp('2021-09-12 00:00:00'), 9, 102)"""
        ).render(fq_table_id=self.fq_table_names[0])
        queries.append(co_insert)
        dre_insert = self.jinja_env.from_string(
            """
            INSERT INTO `{{fq_table_id}}`
            (drug_exposure_id, person_id, drug_concept_id, drug_exposure_start_date,
                drug_exposure_start_datetime, drug_exposure_end_date,
                drug_exposure_end_datetime, drug_type_concept_id, visit_occurrence_id)
            VALUES
            (101, 1, 10, date('2019-05-06'), timestamp('2019-05-06 00:00:00'),
                date('2019-05-01'), timestamp('2019-05-01 00:00:00'), 5, 101),
            (102, 1, 11, date('2019-05-16'), timestamp('2019-05-16 00:00:00'),
                date('2019-05-19'), timestamp('2019-05-19 00:00:00'), 6, 101),
            (103, 2, 12, date('2021-10-12'), timestamp('2021-10-12 00:00:00'),
                date('2021-10-10'), timestamp('2021-10-10 00:00:00'), 7, 102),
            (104, 2, 10, date('2021-10-12'), timestamp('2021-10-12 00:00:00'),
                date('2021-10-15'), timestamp('2021-10-15 00:00:00'), 8, 102)"""
        ).render(fq_table_id=self.fq_table_names[1])
        queries.append(dre_insert)
        dve_insert = self.jinja_env.from_string(
            """
            INSERT INTO `{{fq_table_id}}`
            (device_exposure_id, person_id, device_concept_id, device_exposure_start_date,
                device_exposure_start_datetime, device_exposure_end_date,
                device_exposure_end_datetime, device_type_concept_id, visit_occurrence_id)
            VALUES
            (101, 1, 4, date('2019-05-06'), timestamp('2019-05-06 00:00:00'),
                date('2019-05-14'), timestamp('2019-05-14 00:00:00'), 4, 101),
            (102, 1, 5, date('2019-05-10'), timestamp('2019-05-10 00:00:00'),
                date('2019-05-16'), timestamp('2019-05-16 00:00:00'), 4, 101),
            (103, 2, 6, date('2021-10-12'), timestamp('2021-10-12 00:00:00'),
                date('2021-10-15'), timestamp('2021-10-15 00:00:00'), 4, 102),
            (104, 2, 7, date('2021-10-15'), timestamp('2021-10-15 00:00:00'),
                date('2021-10-10'), timestamp('2021-10-10 00:00:00'), 4, 102)"""
        ).render(fq_table_id=self.fq_table_names[2])
        queries.append(dve_insert)
        vo_insert = self.jinja_env.from_string(
            """
            INSERT INTO `{{fq_table_id}}`
            (visit_occurrence_id, person_id, visit_concept_id, visit_start_date,
                visit_start_datetime, visit_end_date, visit_end_datetime, visit_type_concept_id)
            VALUES
            (101, 1, 9201, date('2019-05-06'), timestamp('2019-05-06 00:00:00'),
                date('2019-05-05'), timestamp('2019-05-05 00:00:00'), 7),
            (102, 2, 11, date('2021-10-12'), timestamp('2021-10-12 00:00:00'),
                date('2021-10-18'), timestamp('2021-10-18 00:00:00'), 7),
            (201, 2, 9201, date('2021-10-01'), timestamp('2021-10-01 00:00:00'),
                date('2021-09-30'), timestamp('2021-09-30 00:00:00'), 7)"""
        ).render(fq_table_id=self.fq_table_names[3])
        queries.append(vo_insert)

        self.load_test_data(queries)

        table_and_counts = [{
            'fq_table_name':
                self.fq_table_names[0],
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[0],
            'loaded_ids': [100, 101, 102, 103, 104],
            'sandboxed_ids': [101, 104],
            'fields': [
                'condition_occurrence_id', 'person_id', 'condition_concept_id',
                'condition_start_date', 'condition_start_datetime',
                'condition_end_date', 'condition_end_datetime',
                'condition_type_concept_id', 'visit_occurrence_id'
            ],
            'cleaned_values': [
                (100, 1, 10, dt.fromisoformat('2019-05-06').date(),
                 dt.fromisoformat('2019-05-06 00:00:00+00:00'),
                 dt.fromisoformat('2019-05-19').date(),
                 dt.fromisoformat('2019-05-19 00:00:00+00:00'), 5, 101),
                (101, 1, 11, dt.fromisoformat('2019-05-25').date(),
                 dt.fromisoformat('2019-05-25 00:00:00+00:00'), None,
                 dt.fromisoformat('2019-05-07 00:00:00+00:00'), 6, 101),
                (102, 1, 12, dt.fromisoformat('2019-05-06').date(),
                 dt.fromisoformat('2019-05-06 00:00:00+00:00'),
                 dt.fromisoformat('2019-05-28').date(),
                 dt.fromisoformat('2019-05-28 00:00:00+00:00'), 7, 101),
                (103, 2, 13, dt.fromisoformat('2020-10-12').date(),
                 dt.fromisoformat('2021-10-12 00:00:00+00:00'),
                 dt.fromisoformat('2021-10-12').date(),
                 dt.fromisoformat('2021-10-12 00:00:00+00:00'), 8, 102),
                (104, 2, 14, dt.fromisoformat('2021-10-12').date(),
                 dt.fromisoformat('2021-10-12 00:00:00+00:00'), None,
                 dt.fromisoformat('2021-09-12 00:00:00+00:00'), 9, 102)
            ]
        }, {
            'fq_table_name':
                self.fq_table_names[1],
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[1],
            'loaded_ids': [101, 102, 103, 104],
            'sandboxed_ids': [101, 103],
            'fields': [
                'drug_exposure_id', 'person_id', 'drug_concept_id',
                'drug_exposure_start_date', 'drug_exposure_start_datetime',
                'drug_exposure_end_date', 'drug_exposure_end_datetime',
                'drug_type_concept_id', 'visit_occurrence_id'
            ],
            'cleaned_values': [
                (101, 1, 10, dt.fromisoformat('2019-05-06').date(),
                 dt.fromisoformat('2019-05-06 00:00:00+00:00'), None,
                 dt.fromisoformat('2019-05-01 00:00:00+00:00'), 5, 101),
                (102, 1, 11, dt.fromisoformat('2019-05-16').date(),
                 dt.fromisoformat('2019-05-16 00:00:00+00:00'),
                 dt.fromisoformat('2019-05-19').date(),
                 dt.fromisoformat('2019-05-19 00:00:00+00:00'), 6, 101),
                (103, 2, 12, dt.fromisoformat('2021-10-12').date(),
                 dt.fromisoformat('2021-10-12 00:00:00+00:00'), None,
                 dt.fromisoformat('2021-10-10 00:00:00+00:00'), 7, 102),
                (104, 2, 10, dt.fromisoformat('2021-10-12').date(),
                 dt.fromisoformat('2021-10-12 00:00:00+00:00'),
                 dt.fromisoformat('2021-10-15').date(),
                 dt.fromisoformat('2021-10-15 00:00:00+00:00'), 8, 102)
            ]
        }, {
            'fq_table_name':
                self.fq_table_names[2],
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[2],
            'loaded_ids': [101, 102, 103, 104],
            'sandboxed_ids': [104],
            'fields': [
                'device_exposure_id', 'person_id', 'device_concept_id',
                'device_exposure_start_date', 'device_exposure_start_datetime',
                'device_exposure_end_date', 'device_exposure_end_datetime',
                'device_type_concept_id', 'visit_occurrence_id'
            ],
            'cleaned_values': [
                (101, 1, 4, dt.fromisoformat('2019-05-06').date(),
                 dt.fromisoformat('2019-05-06 00:00:00+00:00'),
                 dt.fromisoformat('2019-05-14').date(),
                 dt.fromisoformat('2019-05-14 00:00:00+00:00'), 4, 101),
                (102, 1, 5, dt.fromisoformat('2019-05-10').date(),
                 dt.fromisoformat('2019-05-10 00:00:00+00:00'),
                 dt.fromisoformat('2019-05-16').date(),
                 dt.fromisoformat('2019-05-16 00:00:00+00:00'), 4, 101),
                (103, 2, 6, dt.fromisoformat('2021-10-12').date(),
                 dt.fromisoformat('2021-10-12 00:00:00+00:00'),
                 dt.fromisoformat('2021-10-15').date(),
                 dt.fromisoformat('2021-10-15 00:00:00+00:00'), 4, 102),
                (104, 2, 7, dt.fromisoformat('2021-10-15').date(),
                 dt.fromisoformat('2021-10-15 00:00:00+00:00'), None,
                 dt.fromisoformat('2021-10-10 00:00:00+00:00'), 4, 102)
            ]
        }, {
            'fq_table_name':
                self.fq_table_names[3],
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[3],
            'loaded_ids': [101, 102, 201],
            'sandboxed_ids': [101, 201],
            'fields': [
                'visit_occurrence_id', 'person_id', 'visit_concept_id',
                'visit_start_date', 'visit_start_datetime', 'visit_end_date',
                'visit_end_datetime', 'visit_type_concept_id'
            ],
            'cleaned_values': [
                (101, 1, 9201, dt.fromisoformat('2019-05-06').date(),
                 dt.fromisoformat('2019-05-06 00:00:00+00:00'),
                 dt.fromisoformat('2019-05-28').date(),
                 dt.fromisoformat('2019-05-05 00:00:00+00:00'), 7),
                (102, 2, 11, dt.fromisoformat('2021-10-12').date(),
                 dt.fromisoformat('2021-10-12 00:00:00+00:00'),
                 dt.fromisoformat('2021-10-18').date(),
                 dt.fromisoformat('2021-10-18 00:00:00+00:00'), 7),
                (201, 2, 9201, dt.fromisoformat('2021-10-01').date(),
                 dt.fromisoformat('2021-10-01 00:00:00+00:00'),
                 dt.fromisoformat('2021-10-01').date(),
                 dt.fromisoformat('2021-09-30 00:00:00+00:00'), 7)
            ]
        }]
        self.default_test(table_and_counts)
