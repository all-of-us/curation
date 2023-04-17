"""
Integration test for DomainAlignment module
"""

# Python Imports
import os
from datetime import date
from dateutil.parser import parse
import pytz

# Project Imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.domain_alignment import DomainAlignment, LOOKUP_TABLE
from common import (CONDITION_OCCURRENCE, DEVICE_EXPOSURE, DRUG_EXPOSURE,
                    MEASUREMENT, OBSERVATION, PROCEDURE_OCCURRENCE,
                    VOCABULARY_TABLES)
from resources import mapping_table_for
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest


class DomainAlignmentTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        cls.project_id = os.environ.get(PROJECT_ID)
        cls.dataset_id = os.environ.get('COMBINED_DATASET_ID')
        cls.sandbox_id = f'{cls.dataset_id}_sandbox'
        cls.vocabulary_id = os.environ.get('VOCABULARY_DATASET')

        cls.rule_instance = DomainAlignment(cls.project_id, cls.dataset_id,
                                            cls.sandbox_id)

        for table_name in cls.rule_instance.affected_tables:
            cls.fq_table_names.append(
                f'{cls.project_id}.{cls.dataset_id}.{table_name}')

            cls.fq_sandbox_table_names.append(
                f'{cls.project_id}.{cls.sandbox_id}.{cls.rule_instance.sandbox_table_for(table_name)}'
            )

        for table_name in VOCABULARY_TABLES:
            cls.fq_table_names.append(
                f'{cls.project_id}.{cls.dataset_id}.{table_name}')

        cls.fq_sandbox_table_names.append(
            f'{cls.project_id}.{cls.sandbox_id}.{LOOKUP_TABLE}')

        cls.up_class = super().setUpClass()

    def test_domain_alignment(self):
        """
        Tests that the specifications perform as designed.

        CONDITION_OCCURRENCE and its mapping table:
            101, 102, 103: Stay in this table.
            104: condtion_occurrence -> procedure_occurrence ('is_rerouted'='1'). Moved to PROCEDURE_OCCURRENCE as 209.
            105: condtion_occurrence -> observation ('is_rerouted'='1'). Moved to OBSERVATION as 311.
            106: condtion_occurrence -> drug_exposure ('is_rerouted'='0'). Dropped.
            107: condtion_occurrence -> measurement ('is_rerouted'='0'). Dropped.
            108: condtion_occurrence -> device_exposure ('is_rerouted'='0'). Dropped.
            109: Similar to 104 but translated using value_mappigs.csv. Moved to PROCEDURE_OCCURRENCE as 210.
            110: Similar to 104 but translated using value_mappigs.csv. Moved to PROCEDURE_OCCURRENCE as 211.

        PROCEDURE_OCCURRENCE and its mapping table:
            201: Stays in this table.
            202: procedure_occurrence -> condtion_occurrence ('is_rerouted'='1'). Moved to CONDITION_OCCURRENCE as 111.
            203: procedure_occurrence -> drug_exposure ('is_rerouted'='1'). Moved to DRUG_EXPOSURE as 407.
            204: procedure_occurrence -> observation ('is_rerouted'='1'). Moved to OBSERVATION as 312.
            205: procedure_occurrence -> measurement ('is_rerouted'='0'). Dropped.
            206: procedure_occurrence -> device_exposure ('is_rerouted'='1'). Moved to DEVICE_EXPOSURE as 507.
            207: Similar to 202 but translated using value_mappigs.csv. Moved to CONDITION_OCCURRENCE as 112.
            208: Similar to 202 but translated using value_mappigs.csv. Moved to CONDITION_OCCURRENCE as 113.

        OBSERVATION and its mapping table:
            301, 302, 303, 304: Stay in this table. These are PPI records.
            305: observation -> condtion_occurrence ('is_rerouted'='1'). Moved to CONDITION_OCCURRENCE as 114.
            306: observation -> procedure_occurrence ('is_rerouted'='1'). Moved to PROCEDURE_OCCURRENCE as 212.
            307: observation -> drug_exposure ('is_rerouted'='1'). Moved to DRUG_EXPOSURE as 408.
            308: observation -> device_exposure ('is_rerouted'='1'). Moved to DEVICE_EXPOSURE as 508.
            309: observation -> measurement ('is_rerouted'='1'). Moved to MEASUREMENT as 607.
            310: observation -> measurement ('is_rerouted'='1') but not meeting rerouting_criteria. Stays.

        DRUG_EXPOSURE and its mapping table:
            401: Stays in this table.
            402: drug_exposure -> condtion_occurrence ('is_rerouted'='0'). Dropped.
            403: drug_exposure -> procedure_occurrence ('is_rerouted'='0'). Dropped.
            404: drug_exposure -> observation ('is_rerouted'='0'). Dropped.
            405: drug_exposure -> measurement ('is_rerouted'='0'). Dropped.
            406: drug_exposure -> device_exposure ('is_rerouted'='1'). Moved to DEVICE_EXPOSURE as 509.

        DEVICE_EXPOSURE and its mapping table:
            501: Stays in this table.
            502: device_exposure -> condtion_occurrence ('is_rerouted'='0'). Dropped.
            503: device_exposure -> procedure_occurrence ('is_rerouted'='1'). Moved to PROCEDURE_OCCURRENCE as 213.
            504: device_exposure -> drug_exposure ('is_rerouted'='1'). Moved to DRUG_EXPOSURE as 409.
            505: device_exposure -> observation ('is_rerouted'='1'). Moved to OBSERVATION as 314.
            506: device_exposure -> measurement ('is_rerouted'='0'). Dropped.

        MEASUREMENT and its mapping table:
            601: Stays in this table.
            602: measurement -> condtion_occurrence ('is_rerouted'='0'). Dropped.
            603: measurement -> procedure_occurrence ('is_rerouted'='0'). Dropped.
            604: measurement -> drug_exposure ('is_rerouted'='0'). Dropped.
            605: measurement -> observation ('is_rerouted'='1'). Moved to OBSERVATION as 313.
            606: measurement -> device_exposure ('is_rerouted'='0'). Dropped.
        """

        self.maxDiff = None

        self.copy_vocab_tables(self.vocabulary_id)

        INSERT_CONDITION_OCCURRENCE = self.jinja_env.from_string("""
            INSERT INTO `{{project_id}}.{{dataset_id}}.condition_occurrence`
                (condition_occurrence_id, person_id, condition_concept_id, 
                condition_start_date, condition_start_datetime, condition_type_concept_id,
                visit_occurrence_id)
            VALUES
                (101, 11, 201826, '2015-07-15', timestamp('2015-07-15'), 42894222, 1),
                (102, 12, 201826, '2015-07-15', timestamp('2015-07-15'), 42894222, 2),
                (103, 13, 201826, '2015-07-15', timestamp('2015-07-15'), 42894222, 3),
                (104, 14, 36676219, '2015-07-15', timestamp('2015-07-15'), 42865906, 4),
                (105, 15, 40488434, '2015-07-15', timestamp('2015-07-15'), 45754805, 5),
                (106, 16, 42542298, '2015-07-15', timestamp('2015-07-15'), 99999, 6),
                (107, 17, 3009160, '2015-07-15', timestamp('2015-07-15'), 42894222, 7),
                (108, 18, 40218685, '2015-07-15', timestamp('2015-07-15'), 99999, 8),
                (109, 19, 36676219, '2015-07-15', timestamp('2015-07-15'), 44786627, 9),
                (110, 10, 36676219, '2015-07-15', timestamp('2015-07-15'), 44786629, 10)
        """).render(project_id=self.project_id, dataset_id=self.dataset_id)

        INSERT_CONDITION_OCCURRENCE_MAPPING = self.jinja_env.from_string("""
            INSERT INTO `{{project_id}}.{{dataset_id}}._mapping_condition_occurrence`
                (condition_occurrence_id, src_dataset_id, src_condition_occurrence_id,
                src_hpo_id, src_table_id)
            VALUES
                (101, 'dataset', 1, 'hpo_1', 'condition_occurrence'),
                (102, 'dataset', 2, 'hpo_2', 'condition_occurrence'),
                (103, 'dataset', 3, 'hpo_3', 'condition_occurrence'),
                (104, 'dataset', 4, 'hpo_4', 'condition_occurrence'),
                (105, 'dataset', 5, 'hpo_5', 'condition_occurrence'),
                (106, 'dataset', 6, 'hpo_6', 'condition_occurrence'),
                (107, 'dataset', 7, 'hpo_7', 'condition_occurrence'),
                (108, 'dataset', 8, 'hpo_8', 'condition_occurrence'),
                (109, 'dataset', 9, 'hpo_9', 'condition_occurrence'),
                (110, 'dataset', 10, 'hpo_10', 'condition_occurrence')
        """).render(project_id=self.project_id, dataset_id=self.dataset_id)

        INSERT_PROCEDURE_OCCURRENCE = self.jinja_env.from_string("""
            INSERT INTO `{{project_id}}.{{dataset_id}}.procedure_occurrence`
                (procedure_occurrence_id, person_id, procedure_concept_id, procedure_date, 
                procedure_datetime, procedure_type_concept_id, visit_occurrence_id)
            VALUES
                (201, 21, 36676219, '2015-07-15', timestamp('2015-07-15'), 42865906, 1),
                (202, 22, 320128, '2015-07-15', timestamp('2015-07-15'), 42894222, 2),
                (203, 23, 45892531, '2015-07-15', timestamp('2015-07-15'), 44786630, 3),
                (204, 24, 1585808, '2015-07-15', timestamp('2015-07-15'), 44786630, 4),
                (205, 25, 45887635, '2015-07-15', timestamp('2015-07-15'), 99999, 5),
                (206, 26, 2211851, '2015-07-15', timestamp('2015-07-15'), 44786630, 6),
                (207, 27, 320128, '2015-07-15', timestamp('2015-07-15'), 44786630, 7),
                (208, 28, 320128, '2015-07-15', timestamp('2015-07-15'), 44786631, 8)
        """).render(project_id=self.project_id, dataset_id=self.dataset_id)

        INSERT_PROCEDURE_OCCURRENCE_MAPPING = self.jinja_env.from_string("""
            INSERT INTO `{{project_id}}.{{dataset_id}}._mapping_procedure_occurrence`
                (procedure_occurrence_id, src_dataset_id, src_procedure_occurrence_id, 
                src_hpo_id, src_table_id)
            VALUES
                (201, 'dataset', 1, 'hpo_1', 'procedure_occurrence'),
                (202, 'dataset', 2, 'hpo_2', 'procedure_occurrence'),
                (203, 'dataset', 3, 'hpo_3', 'procedure_occurrence'),
                (204, 'dataset', 4, 'hpo_4', 'procedure_occurrence'),
                (205, 'dataset', 5, 'hpo_5', 'procedure_occurrence'),
                (206, 'dataset', 6, 'hpo_6', 'procedure_occurrence'),
                (207, 'dataset', 7, 'hpo_7', 'procedure_occurrence'),
                (208, 'dataset', 8, 'hpo_8', 'procedure_occurrence')
        """).render(project_id=self.project_id, dataset_id=self.dataset_id)

        INSERT_OBSERVATION = self.jinja_env.from_string("""
            INSERT INTO `{{project_id}}.{{dataset_id}}.observation`
                (observation_id, person_id, observation_concept_id, observation_date, 
                observation_type_concept_id, observation_source_value, observation_source_concept_id,
                value_as_concept_id, value_as_number)
            VALUES
                (301, 31, 8621, '2015-07-15', 45905771, 'ipaq_1', 1332870, NULL, NULL),
                (302, 32, 8621, '2015-07-15', 45905771, 'ipaq_3', 1332871, NULL, NULL),
                (303, 33, 8621, '2015-07-15', 45905771, 'ipaq_5', 1332872, NULL, NULL),
                (304, 34, 61909002, '2015-07-15', 45905771, 'Language', 1585413, NULL, NULL),
                (305, 35, 45769242, '2015-07-15', 44814721, 'IS_ILLICIT', 99999, NULL, NULL),
                (306, 36, 42740600, '2015-07-15', 44814721, '0057T', 42740600, NULL, NULL),
                (307, 37, 1126658, '2015-07-15', 32817, '4650', 99999, NULL, NULL),
                (308, 38, 4237601, '2015-07-15', 32817, '115', 99999, NULL, NULL),
                (309, 39, 4159568, '2015-07-15', 45905771, 'DAST', 39141, NULL, 1),
                (310, 30, 4159568, '2015-07-15', 45905771, 'DAST', 39141, NULL, NULL)
        """).render(project_id=self.project_id, dataset_id=self.dataset_id)

        INSERT_OBSERVATION_MAPPING = self.jinja_env.from_string("""
            INSERT INTO `{{project_id}}.{{dataset_id}}._mapping_observation`
                (observation_id, src_dataset_id, src_observation_id, 
                 src_hpo_id, src_table_id)
            VALUES
                (301, 'dataset', 1, 'hpo_1', 'observation'),
                (302, 'dataset', 2, 'hpo_2', 'observation'),
                (303, 'dataset', 3, 'hpo_3', 'observation'),
                (304, 'dataset', 4, 'hpo_4', 'observation'),
                (305, 'dataset', 5, 'hpo_5', 'observation'),
                (306, 'dataset', 6, 'hpo_6', 'observation'),
                (307, 'dataset', 7, 'hpo_7', 'observation'),
                (308, 'dataset', 8, 'hpo_8', 'observation'),
                (309, 'dataset', 9, 'hpo_9', 'observation'),
                (310, 'dataset', 10, 'hpo_10', 'observation')
        """).render(project_id=self.project_id, dataset_id=self.dataset_id)

        INSERT_DRUG_EXPOSURE = self.jinja_env.from_string("""
            INSERT INTO `{{project_id}}.{{dataset_id}}.drug_exposure`
                (drug_exposure_id, person_id, drug_concept_id,
                drug_exposure_start_date, drug_exposure_start_datetime,
                drug_type_concept_id)
            VALUES
                (401, 41, 1126658, '2015-07-15', timestamp('2015-07-15'), 99999),
                (402, 42, 320128, '2015-07-15', timestamp('2015-07-15'), 99999),
                (403, 43, 36676219, '2015-07-15', timestamp('2015-07-15'), 99999),
                (404, 44, 1585808, '2015-07-15', timestamp('2015-07-15'), 99999),
                (405, 45, 45887635, '2015-07-15', timestamp('2015-07-15'), 99999),
                (406, 46, 45077152, '2015-07-15', timestamp('2015-07-15'), 99999)
        """).render(project_id=self.project_id, dataset_id=self.dataset_id)

        INSERT_DRUG_EXPOSURE_MAPPING = self.jinja_env.from_string("""
            INSERT INTO `{{project_id}}.{{dataset_id}}._mapping_drug_exposure`
                (drug_exposure_id, src_dataset_id, src_drug_exposure_id,
                src_hpo_id, src_table_id)
            VALUES
                (401, 'dataset', 1, 'hpo_1', 'drug_exposure'),
                (402, 'dataset', 2, 'hpo_2', 'drug_exposure'),
                (403, 'dataset', 3, 'hpo_3', 'drug_exposure'),
                (404, 'dataset', 4, 'hpo_4', 'drug_exposure'),
                (405, 'dataset', 5, 'hpo_5', 'drug_exposure'),
                (406, 'dataset', 6, 'hpo_6', 'drug_exposure')
        """).render(project_id=self.project_id, dataset_id=self.dataset_id)

        INSERT_DEVICE_EXPOSURE = self.jinja_env.from_string("""
            INSERT INTO `{{project_id}}.{{dataset_id}}.device_exposure`
                (device_exposure_id, person_id, device_concept_id,
                device_exposure_start_date, device_exposure_start_datetime,
                device_type_concept_id)
            VALUES
                (501, 51, 4206863, '2015-07-15', timestamp('2015-07-15'), 44818707),
                (502, 52, 320128, '2015-07-15', timestamp('2015-07-15'), 99999),
                (503, 53, 2101931, '2015-07-15', timestamp('2015-07-15'), 99999),
                (504, 54, 740910, '2015-07-15', timestamp('2015-07-15'), 44818707),
                (505, 55, 2106252, '2015-07-15', timestamp('2015-07-15'), 99999),
                (506, 56, 45887635, '2015-07-15', timestamp('2015-07-15'), 99999)
        """).render(project_id=self.project_id, dataset_id=self.dataset_id)

        INSERT_DEVICE_EXPOSURE_MAPPING = self.jinja_env.from_string("""
            INSERT INTO `{{project_id}}.{{dataset_id}}._mapping_device_exposure`
                (device_exposure_id, src_dataset_id, src_device_exposure_id,
                src_hpo_id, src_table_id)
            VALUES
                (501, 'dataset', 1, 'hpo_1', 'device_exposure'),
                (502, 'dataset', 2, 'hpo_2', 'device_exposure'),
                (503, 'dataset', 3, 'hpo_3', 'device_exposure'),
                (504, 'dataset', 4, 'hpo_4', 'device_exposure'),
                (505, 'dataset', 5, 'hpo_5', 'device_exposure'),
                (506, 'dataset', 6, 'hpo_6', 'device_exposure')
        """).render(project_id=self.project_id, dataset_id=self.dataset_id)

        INSERT_MEASUREMENT = self.jinja_env.from_string("""
            INSERT INTO `{{project_id}}.{{dataset_id}}.measurement`
                (measurement_id, person_id, measurement_concept_id,
                measurement_date, measurement_datetime,
                measurement_type_concept_id)
            VALUES
                (601, 61, 44782827, '2015-07-15', timestamp('2015-07-15'), 44818704),
                (602, 62, 320128, '2015-07-15', timestamp('2015-07-15'), 99999),
                (603, 63, 36676219, '2015-07-15', timestamp('2015-07-15'), 99999),
                (604, 64, 45892531, '2015-07-15', timestamp('2015-07-15'), 99999),
                (605, 65, 3045429, '2015-07-15', timestamp('2015-07-15'), 44818702),
                (606, 66, 4237601, '2015-07-15', timestamp('2015-07-15'), 99999)
        """).render(project_id=self.project_id, dataset_id=self.dataset_id)

        INSERT_MEASUREMENT_MAPPING = self.jinja_env.from_string("""
            INSERT INTO `{{project_id}}.{{dataset_id}}._mapping_measurement`
                (measurement_id, src_dataset_id, src_measurement_id,
                src_hpo_id, src_table_id)
            VALUES
                (601, 'dataset', 1, 'hpo_1', 'measurement'),
                (602, 'dataset', 2, 'hpo_2', 'measurement'),
                (603, 'dataset', 3, 'hpo_3', 'measurement'),
                (604, 'dataset', 4, 'hpo_4', 'measurement'),
                (605, 'dataset', 5, 'hpo_5', 'measurement'),
                (606, 'dataset', 6, 'hpo_6', 'measurement')
        """).render(project_id=self.project_id, dataset_id=self.dataset_id)

        self.load_test_data([
            INSERT_CONDITION_OCCURRENCE, INSERT_CONDITION_OCCURRENCE_MAPPING,
            INSERT_PROCEDURE_OCCURRENCE, INSERT_PROCEDURE_OCCURRENCE_MAPPING,
            INSERT_DRUG_EXPOSURE, INSERT_DRUG_EXPOSURE_MAPPING,
            INSERT_DEVICE_EXPOSURE, INSERT_DEVICE_EXPOSURE_MAPPING,
            INSERT_OBSERVATION, INSERT_OBSERVATION_MAPPING, INSERT_MEASUREMENT,
            INSERT_MEASUREMENT_MAPPING
        ])

        test_date = date(2015, 7, 15)
        test_datetime = parse('2015-07-15 00:00:00 UTC').astimezone(pytz.utc)

        tables_and_counts = [{
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{CONDITION_OCCURRENCE}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{self.rule_instance.sandbox_table_for(CONDITION_OCCURRENCE)}',
            'loaded_ids': [101, 102, 103, 104, 105, 106, 107, 108, 109, 110],
            'sandboxed_ids': [104, 105, 106, 107, 108, 109, 110],
            'fields': [
                'condition_occurrence_id', 'person_id', 'condition_concept_id',
                'condition_start_date', 'condition_start_datetime',
                'condition_type_concept_id', 'visit_occurrence_id'
            ],
            'cleaned_values': [
                (101, 11, 201826, test_date, test_datetime, 42894222, 1),
                (102, 12, 201826, test_date, test_datetime, 42894222, 2),
                (103, 13, 201826, test_date, test_datetime, 42894222, 3),
                (111, 22, 320128, test_date, test_datetime, 0, 2),
                (112, 27, 320128, test_date, test_datetime, 44786627, 7),
                (113, 28, 320128, test_date, test_datetime, 44786629, 8),
                (114, 35, 45769242, test_date,
                 parse('1970-01-01 00:00:00 UTC').astimezone(pytz.utc), 0,
                 None),
            ]
        }, {
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{mapping_table_for(CONDITION_OCCURRENCE)}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{self.rule_instance.sandbox_table_for(mapping_table_for(CONDITION_OCCURRENCE))}',
            'loaded_ids': [101, 102, 103, 104, 105, 106, 107, 108, 109, 110],
            'sandboxed_ids': [104, 105, 106, 107, 108, 109, 110],
            'fields': [
                'condition_occurrence_id', 'src_dataset_id',
                'src_condition_occurrence_id', 'src_hpo_id', 'src_table_id'
            ],
            'cleaned_values': [
                (101, 'dataset', 1, 'hpo_1', 'condition_occurrence'),
                (102, 'dataset', 2, 'hpo_2', 'condition_occurrence'),
                (103, 'dataset', 3, 'hpo_3', 'condition_occurrence'),
                (111, 'dataset', 2, 'hpo_2', 'procedure_occurrence'),
                (112, 'dataset', 7, 'hpo_7', 'procedure_occurrence'),
                (113, 'dataset', 8, 'hpo_8', 'procedure_occurrence'),
                (114, 'dataset', 5, 'hpo_5', 'observation'),
            ]
        }, {
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{PROCEDURE_OCCURRENCE}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{self.rule_instance.sandbox_table_for(PROCEDURE_OCCURRENCE)}',
            'loaded_ids': [201, 202, 203, 204, 205, 206, 207, 208],
            'sandboxed_ids': [202, 203, 204, 205, 206, 207, 208],
            'fields': [
                'procedure_occurrence_id', 'person_id', 'procedure_concept_id',
                'procedure_date', 'procedure_datetime',
                'procedure_type_concept_id', 'visit_occurrence_id'
            ],
            'cleaned_values': [
                (201, 21, 36676219, test_date, test_datetime, 42865906, 1),
                (209, 14, 36676219, test_date, test_datetime, 0, 4),
                (210, 19, 36676219, test_date, test_datetime, 44786630, 9),
                (211, 10, 36676219, test_date, test_datetime, 44786631, 10),
                (212, 36, 42740600, test_date,
                 parse('1970-01-01 00:00:00 UTC').astimezone(pytz.utc), 0,
                 None),
                (213, 53, 2101931, test_date, test_datetime, 0, None),
            ]
        }, {
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{mapping_table_for(PROCEDURE_OCCURRENCE)}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{self.rule_instance.sandbox_table_for(mapping_table_for(PROCEDURE_OCCURRENCE))}',
            'loaded_ids': [201, 202, 203, 204, 205, 206, 207, 208],
            'sandboxed_ids': [202, 203, 204, 205, 206, 207, 208],
            'fields': [
                'procedure_occurrence_id', 'src_dataset_id',
                'src_procedure_occurrence_id', 'src_hpo_id', 'src_table_id'
            ],
            'cleaned_values': [
                (201, 'dataset', 1, 'hpo_1', 'procedure_occurrence'),
                (209, 'dataset', 4, 'hpo_4', 'condition_occurrence'),
                (210, 'dataset', 9, 'hpo_9', 'condition_occurrence'),
                (211, 'dataset', 10, 'hpo_10', 'condition_occurrence'),
                (212, 'dataset', 6, 'hpo_6', 'observation'),
                (213, 'dataset', 3, 'hpo_3', 'device_exposure'),
            ]
        }, {
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{OBSERVATION}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{self.rule_instance.sandbox_table_for(OBSERVATION)}',
            'loaded_ids': [301, 302, 303, 304, 305, 306, 307, 308, 309, 310],
            'sandboxed_ids': [305, 306, 307, 308, 309],
            'fields': [
                'observation_id', 'person_id', 'observation_concept_id',
                'observation_date', 'observation_type_concept_id',
                'observation_source_value', 'observation_source_concept_id'
            ],
            'cleaned_values': [
                (301, 31, 8621, test_date, 45905771, 'ipaq_1', 1332870),
                (302, 32, 8621, test_date, 45905771, 'ipaq_3', 1332871),
                (303, 33, 8621, test_date, 45905771, 'ipaq_5', 1332872),
                (304, 34, 61909002, test_date, 45905771, 'Language', 1585413),
                (310, 30, 4159568, test_date, 45905771, 'DAST', 39141),
                (311, 15, 40488434, test_date, 0, None, None),
                (312, 24, 1585808, test_date, 0, None, None),
                (313, 65, 3045429, test_date, 0, None, None),
                (314, 55, 2106252, test_date, 0, None, None),
            ]
        }, {
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{mapping_table_for(OBSERVATION)}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{self.rule_instance.sandbox_table_for(mapping_table_for(OBSERVATION))}',
            'loaded_ids': [301, 302, 303, 304, 305, 306, 307, 308, 309, 310],
            'sandboxed_ids': [305, 306, 307, 308, 309],
            'fields': [
                'observation_id', 'src_dataset_id', 'src_observation_id',
                'src_hpo_id', 'src_table_id'
            ],
            'cleaned_values': [
                (301, 'dataset', 1, 'hpo_1', 'observation'),
                (302, 'dataset', 2, 'hpo_2', 'observation'),
                (303, 'dataset', 3, 'hpo_3', 'observation'),
                (304, 'dataset', 4, 'hpo_4', 'observation'),
                (310, 'dataset', 10, 'hpo_10', 'observation'),
                (311, 'dataset', 5, 'hpo_5', 'condition_occurrence'),
                (312, 'dataset', 4, 'hpo_4', 'procedure_occurrence'),
                (313, 'dataset', 5, 'hpo_5', 'measurement'),
                (314, 'dataset', 5, 'hpo_5', 'device_exposure'),
            ]
        }, {
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{DRUG_EXPOSURE}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{self.rule_instance.sandbox_table_for(DRUG_EXPOSURE)}',
            'loaded_ids': [401, 402, 403, 404, 405, 406],
            'sandboxed_ids': [402, 403, 404, 405, 406],
            'fields': [
                'drug_exposure_id', 'person_id', 'drug_concept_id',
                'drug_exposure_start_date', 'drug_exposure_start_datetime',
                'drug_type_concept_id'
            ],
            'cleaned_values': [
                (401, 41, 1126658, test_date, test_datetime, 99999),
                (407, 23, 45892531, test_date, test_datetime, 0),
                (408, 37, 1126658, test_date,
                 parse('1970-01-01 00:00:00 UTC').astimezone(pytz.utc), 0),
                (409, 54, 740910, test_date, test_datetime, 0),
            ]
        }, {
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{mapping_table_for(DRUG_EXPOSURE)}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{self.rule_instance.sandbox_table_for(mapping_table_for(DRUG_EXPOSURE))}',
            'loaded_ids': [401, 402, 403, 404, 405, 406],
            'sandboxed_ids': [402, 403, 404, 405, 406],
            'fields': [
                'drug_exposure_id', 'src_dataset_id', 'src_drug_exposure_id',
                'src_hpo_id', 'src_table_id'
            ],
            'cleaned_values': [
                (401, 'dataset', 1, 'hpo_1', 'drug_exposure'),
                (407, 'dataset', 3, 'hpo_3', 'procedure_occurrence'),
                (408, 'dataset', 7, 'hpo_7', 'observation'),
                (409, 'dataset', 4, 'hpo_4', 'device_exposure'),
            ]
        }, {
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{DEVICE_EXPOSURE}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{self.rule_instance.sandbox_table_for(DEVICE_EXPOSURE)}',
            'loaded_ids': [501, 502, 503, 504, 505, 506],
            'sandboxed_ids': [502, 503, 504, 505, 506],
            'fields': [
                'device_exposure_id', 'person_id', 'device_concept_id',
                'device_exposure_start_date', 'device_exposure_start_datetime',
                'device_type_concept_id'
            ],
            'cleaned_values': [
                (501, 51, 4206863, test_date, test_datetime, 44818707),
                (507, 26, 2211851, test_date, test_datetime, 0),
                (508, 38, 4237601, test_date,
                 parse('1970-01-01 00:00:00 UTC').astimezone(pytz.utc), 0),
                (509, 46, 45077152, test_date, test_datetime, 0),
            ]
        }, {
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{mapping_table_for(DEVICE_EXPOSURE)}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{self.rule_instance.sandbox_table_for(mapping_table_for(DEVICE_EXPOSURE))}',
            'loaded_ids': [501, 502, 503, 504, 505, 506],
            'sandboxed_ids': [502, 503, 504, 505, 506],
            'fields': [
                'device_exposure_id', 'src_dataset_id',
                'src_device_exposure_id', 'src_hpo_id', 'src_table_id'
            ],
            'cleaned_values': [
                (501, 'dataset', 1, 'hpo_1', 'device_exposure'),
                (507, 'dataset', 6, 'hpo_6', 'procedure_occurrence'),
                (508, 'dataset', 8, 'hpo_8', 'observation'),
                (509, 'dataset', 6, 'hpo_6', 'drug_exposure'),
            ]
        }, {
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{MEASUREMENT}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{self.rule_instance.sandbox_table_for(MEASUREMENT)}',
            'loaded_ids': [601, 602, 603, 604, 605, 606],
            'sandboxed_ids': [602, 603, 604, 605, 606],
            'fields': [
                'measurement_id', 'person_id', 'measurement_concept_id',
                'measurement_date', 'measurement_datetime',
                'measurement_type_concept_id'
            ],
            'cleaned_values': [
                (601, 61, 44782827, test_date, test_datetime, 44818704),
                (607, 39, 4159568, test_date, None, 0),
            ]
        }, {
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{mapping_table_for(MEASUREMENT)}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{self.rule_instance.sandbox_table_for(mapping_table_for(MEASUREMENT))}',
            'loaded_ids': [601, 602, 603, 604, 605, 606],
            'sandboxed_ids': [602, 603, 604, 605, 606],
            'fields': [
                'measurement_id', 'src_dataset_id', 'src_measurement_id',
                'src_hpo_id', 'src_table_id'
            ],
            'cleaned_values': [
                (601, 'dataset', 1, 'hpo_1', 'measurement'),
                (607, 'dataset', 9, 'hpo_9', 'observation'),
            ]
        }]

        self.default_test(tables_and_counts)
