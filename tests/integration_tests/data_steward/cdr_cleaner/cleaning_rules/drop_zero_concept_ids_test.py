"""Integration test for DropZeroConceptIDs
"""
# Python imports
import datetime
import os

# Third party imports
from dateutil import parser

# Project imports
from app_identity import PROJECT_ID
from common import (JINJA_ENV, CONDITION_OCCURRENCE, PROCEDURE_OCCURRENCE,
                    VISIT_OCCURRENCE, DRUG_EXPOSURE, DEVICE_EXPOSURE,
                    OBSERVATION, MEASUREMENT)
from cdr_cleaner.cleaning_rules.drop_zero_concept_ids import DropZeroConceptIDs
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest

INSERT_STATEMENTS = {
    CONDITION_OCCURRENCE:
        JINJA_ENV.from_string("""
        INSERT INTO `{{fq_table_name}}`
        (condition_occurrence_id, 
        person_id, 
        condition_source_concept_id,
        condition_concept_id, 
        condition_start_date, 
        condition_start_datetime,
        condition_type_concept_id)
        VALUES
        (11, 101, 1111, 1111, date('2019-03-03'), timestamp('2019-03-03 00:00:00'), 111),
        (12, 102, NULL, 1111, date('2019-03-03'), timestamp('2019-03-03 00:00:00'), 111),
        (13, 103, 0, 1111, date('2019-03-03'), timestamp('2019-03-03 00:00:00'), 111),
        (14, 104, 1111, 0, date('2019-03-03'), timestamp('2019-03-03 00:00:00'), 111),
        (15, 105, NULL, 0, date('2019-03-03'), timestamp('2019-03-03 00:00:00'), 111),
        (16, 106, 0, 0, date('2019-03-03'), timestamp('2019-03-03 00:00:00'), 111)
        """),
    PROCEDURE_OCCURRENCE:
        JINJA_ENV.from_string("""
        INSERT INTO `{{fq_table_name}}`
        (procedure_occurrence_id, 
        person_id, 
        procedure_source_concept_id,
        procedure_concept_id, 
        procedure_date, 
        procedure_datetime,
        procedure_type_concept_id)
        VALUES
        (21, 201, 2222, 2222, date('2019-03-03'), timestamp('2019-03-03 00:00:00'), 222), 
        (22, 202, NULL, 2222, date('2019-03-03'), timestamp('2019-03-03 00:00:00'), 222), 
        (23, 203, 0, 2222, date('2019-03-03'), timestamp('2019-03-03 00:00:00'), 222), 
        (24, 204, 2222, 0, date('2019-03-03'), timestamp('2019-03-03 00:00:00'), 222), 
        (25, 205, NULL, 0, date('2019-03-03'), timestamp('2019-03-03 00:00:00'), 222), 
        (26, 206, 0, 0, date('2019-03-03'), timestamp('2019-03-03 00:00:00'), 222)
        """),
    VISIT_OCCURRENCE:
        JINJA_ENV.from_string("""
        INSERT INTO `{{fq_table_name}}`
        (visit_occurrence_id, 
        person_id, 
        visit_source_concept_id,
        visit_concept_id, 
        visit_start_date, 
        visit_end_date,
        visit_type_concept_id)
        VALUES
        (31, 301, 3333, 3333, date('2019-03-03'), date('2019-03-03'), 333), 
        (32, 302, NULL, 3333, date('2019-03-03'), date('2019-03-03'), 333),
        (33, 303, 0, 3333, date('2019-03-03'), date('2019-03-03'), 333),
        (34, 304, 3333, 0, date('2019-03-03'), date('2019-03-03'), 333),
        (35, 305, NULL, 0, date('2019-03-03'), date('2019-03-03'), 333),
        (36, 306, 0, 0, date('2019-03-03'), date('2019-03-03'), 333)
        """),
    DRUG_EXPOSURE:
        JINJA_ENV.from_string("""
        INSERT INTO `{{fq_table_name}}`
        (drug_exposure_id, 
        person_id, 
        drug_source_concept_id,
        drug_concept_id, 
        drug_exposure_start_date, 
        drug_exposure_start_datetime,
        drug_type_concept_id)
        VALUES
        (41, 401, 4444, 4444, date('2019-03-03'), timestamp('2019-03-03 00:00:00'), 444), 
        (42, 402, NULL, 4444, date('2019-03-03'), timestamp('2019-03-03 00:00:00'), 444), 
        (43, 403, 0, 4444, date('2019-03-03'), timestamp('2019-03-03 00:00:00'), 444), 
        (44, 404, 4444, 0, date('2019-03-03'), timestamp('2019-03-03 00:00:00'), 444), 
        (45, 405, NULL, 0, date('2019-03-03'), timestamp('2019-03-03 00:00:00'), 444), 
        (46, 406, 0, 0, date('2019-03-03'), timestamp('2019-03-03 00:00:00'), 444)
        """),
    DEVICE_EXPOSURE:
        JINJA_ENV.from_string("""
        INSERT INTO `{{fq_table_name}}`
        (device_exposure_id, 
        person_id, 
        device_source_concept_id,
        device_concept_id, 
        device_exposure_start_date, 
        device_exposure_start_datetime,
        device_type_concept_id)
        VALUES
        (51, 501, 5555, 5555, date('2019-03-03'), timestamp('2019-03-03 00:00:00'), 555), 
        (52, 502, NULL, 5555, date('2019-03-03'), timestamp('2019-03-03 00:00:00'), 555), 
        (53, 503, 0, 5555, date('2019-03-03'), timestamp('2019-03-03 00:00:00'), 555), 
        (54, 504, 5555, 0, date('2019-03-03'), timestamp('2019-03-03 00:00:00'), 555), 
        (55, 505, NULL, 0, date('2019-03-03'), timestamp('2019-03-03 00:00:00'), 555), 
        (56, 506, 0, 0, date('2019-03-03'), timestamp('2019-03-03 00:00:00'), 555) 
        """),
    OBSERVATION:
        JINJA_ENV.from_string("""
        INSERT INTO `{{fq_table_name}}`
        (observation_id, 
        person_id, 
        observation_source_concept_id, 
        observation_concept_id, 
        observation_date, 
        observation_type_concept_id)
        VALUES
        (61, 601, 6666, 6666, date('2019-03-03'), 666), 
        (62, 602, NULL, 6666, date('2019-03-03'), 666), 
        (63, 603, 0, 6666, date('2019-03-03'), 666), 
        (64, 604, 6666, 0, date('2019-03-03'), 666), 
        (65, 605, NULL, 0, date('2019-03-03'), 666), 
        (66, 606, 0, 0, date('2019-03-03'), 666)
        """),
    MEASUREMENT:
        JINJA_ENV.from_string("""
        INSERT INTO `{{fq_table_name}}`
        (measurement_id, 
        person_id, 
        measurement_source_concept_id, 
        measurement_concept_id, 
        measurement_date, 
        measurement_type_concept_id)
        VALUES
        (71, 701, 7777, 7777, date('2019-03-03'), 777), 
        (72, 702, NULL, 7777, date('2019-03-03'), 777), 
        (73, 703, 0, 7777, date('2019-03-03'), 777), 
        (74, 704, 7777, 0, date('2019-03-03'), 777), 
        (75, 705, NULL, 0, date('2019-03-03'), 777), 
        (76, 706, 0, 0, date('2019-03-03'), 777)
        """),
}


class DropZeroConceptIDsTest(BaseTest.CleaningRulesTestBase):

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
        dataset_id = os.environ.get('COMBINED_DEID_DATASET_ID')
        cls.dataset_id = dataset_id
        sandbox_id = dataset_id + '_sandbox'
        cls.sandbox_id = sandbox_id

        cls.rule_instance = DropZeroConceptIDs(project_id, dataset_id,
                                               sandbox_id)

        cls.fq_sandbox_table_names = [
            f'{cls.project_id}.{cls.sandbox_id}.{sb_table_name}'
            for sb_table_name in cls.rule_instance.get_sandbox_tablenames()
        ]

        cls.fq_table_names = [
            f'{project_id}.{dataset_id}.{table_name}'
            for table_name in cls.rule_instance.affected_tables
        ]

        super().setUpClass()

    def setUp(self):
        """
        Create common information for tests.
        Creates common expected parameter types from cleaned tables and a common fully qualified (fq)
        dataset name string used to load the data.
        """
        self.fq_dataset_name = '.'.join(self.fq_table_names[0].split('.')[:-1])
        self.fq_sandbox_dataset_name = '.'.join(
            self.fq_sandbox_table_names[0].split('.')[:-1])
        self.date = parser.parse('2019-03-03').date()
        self.datetime = datetime.datetime(2019,
                                          3,
                                          3,
                                          0,
                                          0,
                                          tzinfo=datetime.timezone.utc)

        super().setUp()

    def test_drop_zero_concept_ids(self):
        """
        Validates pre-conditions, test execution and post conditions based on the tables_and_counts variable.
        """
        queries = [
            INSERT_STATEMENTS[table_name].render(
                fq_table_name=f'{self.project_id}.{self.dataset_id}.{table_name}'
            ) for table_name in self.rule_instance.affected_tables
        ]

        self.load_test_data(queries)

        tables_and_counts = [
            {
                'fq_table_name':
                    f'{self.fq_dataset_name}.{CONDITION_OCCURRENCE}',
                'fq_sandbox_table_name':
                    f'{self.fq_sandbox_dataset_name}.'
                    f'{self.rule_instance.sandbox_table_for(CONDITION_OCCURRENCE)}',
                'fields': [
                    'condition_occurrence_id', 'person_id',
                    'condition_source_concept_id', 'condition_concept_id',
                    'condition_start_date', 'condition_start_datetime',
                    'condition_type_concept_id'
                ],
                'loaded_ids': [11, 12, 13, 14, 15, 16],
                'sandboxed_ids': [15, 16],
                'cleaned_values': [
                    (11, 101, 1111, 1111, self.date, self.datetime, 111),
                    (12, 102, None, 1111, self.date, self.datetime, 111),
                    (13, 103, 0, 1111, self.date, self.datetime, 111),
                    (14, 104, 1111, 0, self.date, self.datetime, 111),
                ],
            },
            {
                'fq_table_name':
                    f'{self.fq_dataset_name}.{PROCEDURE_OCCURRENCE}',
                'fq_sandbox_table_name':
                    f'{self.fq_sandbox_dataset_name}.'
                    f'{self.rule_instance.sandbox_table_for(PROCEDURE_OCCURRENCE)}',
                'fields': [
                    'procedure_occurrence_id', 'person_id',
                    'procedure_source_concept_id', 'procedure_concept_id',
                    'procedure_date', 'procedure_datetime',
                    'procedure_type_concept_id'
                ],
                'loaded_ids': [21, 22, 23, 24, 25, 26],
                'sandboxed_ids': [25, 26],
                'cleaned_values': [
                    (21, 201, 2222, 2222, self.date, self.datetime, 222),
                    (22, 202, None, 2222, self.date, self.datetime, 222),
                    (23, 203, 0, 2222, self.date, self.datetime, 222),
                    (24, 204, 2222, 0, self.date, self.datetime, 222),
                ],
            },
            {
                'fq_table_name':
                    f'{self.fq_dataset_name}.{VISIT_OCCURRENCE}',
                'fq_sandbox_table_name':
                    f'{self.fq_sandbox_dataset_name}.'
                    f'{self.rule_instance.sandbox_table_for(VISIT_OCCURRENCE)}',
                'fields': [
                    'visit_occurrence_id', 'person_id',
                    'visit_source_concept_id', 'visit_concept_id',
                    'visit_start_date', 'visit_end_date',
                    'visit_type_concept_id'
                ],
                'loaded_ids': [31, 32, 33, 34, 35, 36],
                'sandboxed_ids': [35, 36],
                'cleaned_values': [
                    (31, 301, 3333, 3333, self.date, self.date, 333),
                    (32, 302, None, 3333, self.date, self.date, 333),
                    (33, 303, 0, 3333, self.date, self.date, 333),
                    (34, 304, 3333, 0, self.date, self.date, 333),
                ],
            },
            {
                'fq_table_name':
                    f'{self.fq_dataset_name}.{DRUG_EXPOSURE}',
                'fq_sandbox_table_name':
                    f'{self.fq_sandbox_dataset_name}.'
                    f'{self.rule_instance.sandbox_table_for(DRUG_EXPOSURE)}',
                'fields': [
                    'drug_exposure_id', 'person_id', 'drug_source_concept_id',
                    'drug_concept_id', 'drug_exposure_start_date',
                    'drug_exposure_start_datetime', 'drug_type_concept_id'
                ],
                'loaded_ids': [41, 42, 43, 44, 45, 46],
                'sandboxed_ids': [45, 46],
                'cleaned_values': [
                    (41, 401, 4444, 4444, self.date, self.datetime, 444),
                    (42, 402, None, 4444, self.date, self.datetime, 444),
                    (43, 403, 0, 4444, self.date, self.datetime, 444),
                    (44, 404, 4444, 0, self.date, self.datetime, 444),
                ],
            },
            {
                'fq_table_name':
                    f'{self.fq_dataset_name}.{DEVICE_EXPOSURE}',
                'fq_sandbox_table_name':
                    f'{self.fq_sandbox_dataset_name}.'
                    f'{self.rule_instance.sandbox_table_for(DEVICE_EXPOSURE)}',
                'fields': [
                    'device_exposure_id', 'person_id',
                    'device_source_concept_id', 'device_concept_id',
                    'device_exposure_start_date',
                    'device_exposure_start_datetime', 'device_type_concept_id'
                ],
                'loaded_ids': [51, 52, 53, 54, 55, 56],
                'sandboxed_ids': [55, 56],
                'cleaned_values': [
                    (51, 501, 5555, 5555, self.date, self.datetime, 555),
                    (52, 502, None, 5555, self.date, self.datetime, 555),
                    (53, 503, 0, 5555, self.date, self.datetime, 555),
                    (54, 504, 5555, 0, self.date, self.datetime, 555),
                ],
            },
            {
                'fq_table_name':
                    f'{self.fq_dataset_name}.{OBSERVATION}',
                'fq_sandbox_table_name':
                    f'{self.fq_sandbox_dataset_name}.'
                    f'{self.rule_instance.sandbox_table_for(OBSERVATION)}',
                'fields': [
                    'observation_id', 'person_id',
                    'observation_source_concept_id', 'observation_concept_id',
                    'observation_date', 'observation_type_concept_id'
                ],
                'loaded_ids': [61, 62, 63, 64, 65, 66],
                'sandboxed_ids': [65, 66],
                'cleaned_values': [
                    (61, 601, 6666, 6666, self.date, 666),
                    (62, 602, None, 6666, self.date, 666),
                    (63, 603, 0, 6666, self.date, 666),
                    (64, 604, 6666, 0, self.date, 666),
                ],
            },
            {
                'fq_table_name':
                    f'{self.fq_dataset_name}.{MEASUREMENT}',
                'fq_sandbox_table_name':
                    f'{self.fq_sandbox_dataset_name}.'
                    f'{self.rule_instance.sandbox_table_for(MEASUREMENT)}',
                'fields': [
                    'measurement_id', 'person_id',
                    'measurement_source_concept_id', 'measurement_concept_id',
                    'measurement_date', 'measurement_type_concept_id'
                ],
                'loaded_ids': [71, 72, 73, 74, 75, 76],
                'sandboxed_ids': [75, 76],
                'cleaned_values': [
                    (71, 701, 7777, 7777, self.date, 777),
                    (72, 702, None, 7777, self.date, 777),
                    (73, 703, 0, 7777, self.date, 777),
                    (74, 704, 7777, 0, self.date, 777),
                ],
            },
        ]

        self.default_test(tables_and_counts)
