# Python imports
import os

# Third party imports

# Project imports
import common
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.drop_orphaned_pids import DropOrphanedPIDS
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest


class DropOrphanedPIDSTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        # Set the test project identifier
        cls.project_id = os.environ.get(PROJECT_ID)

        # Set the expected test datasets
        cls.dataset_id = os.environ.get('COMBINED_DATASET_ID')
        cls.sandbox_id = f'{cls.dataset_id}_sandbox'
        cls.rule_instance = DropOrphanedPIDS(cls.project_id, cls.dataset_id,
                                             cls.sandbox_id)

        cls.affected_tables = [common.PERSON]
        supporting_tables = [
            common.CONDITION_OCCURRENCE, common.DEATH, common.DEVICE_EXPOSURE,
            common.DRUG_EXPOSURE, common.MEASUREMENT, common.NOTE,
            common.OBSERVATION, common.PROCEDURE_OCCURRENCE, common.SPECIMEN,
            common.VISIT_OCCURRENCE, common.VISIT_DETAIL
        ]
        # Generates list of fully qualified table names and their corresponding sandbox table names
        cls.fq_table_names = [
            f"{cls.project_id}.{cls.dataset_id}.{table}"
            for table in cls.affected_tables + supporting_tables
        ]

        cls.fq_sandbox_table_names = [
            f'{cls.project_id}.{cls.sandbox_id}.{cls.rule_instance.sandbox_table_for(table)}'
            for table in cls.affected_tables
        ]

        # call super to set up the client, create datasets
        cls.up_class = super().setUpClass()

    def setUp(self):
        # create tables
        super().setUp()

        person_tmpl = self.jinja_env.from_string("""
            INSERT INTO `{{project_id}}.{{dataset_id}}.person`
                (person_id, gender_concept_id, year_of_birth, race_concept_id, ethnicity_concept_id)
            VALUES 
                (1, 2, 2000, 3, 4),
                (2, 3, 2001, 4, 5),
                (3, 4, 2001, 5, 6)
            """)

        condition_occurrence_tmpl = self.jinja_env.from_string("""
            INSERT INTO `{{project_id}}.{{dataset_id}}.condition_occurrence` (
                    condition_occurrence_id, person_id, condition_concept_id, condition_start_date, 
                    condition_start_datetime, condition_type_concept_id)
            VALUES
                    (1, 1, 0, '2022-01-01', TIMESTAMP('2022-01-01'), 0),
                    (2, 2, 0, '2022-01-01', TIMESTAMP('2022-01-01'), 0),
                    (3, 1, 0, '2022-01-01', TIMESTAMP('2022-01-01'), 0)
            """)

        death_tmpl = self.jinja_env.from_string("""
            INSERT INTO `{{project_id}}.{{dataset_id}}.death` (person_id, death_date, death_type_concept_id)
            VALUES
                    (1, '2022-01-01', 0),
                    (2, '2022-01-01', 0)
            """)

        device_exposure_tmpl = self.jinja_env.from_string("""
            INSERT INTO `{{project_id}}.{{dataset_id}}.device_exposure` (
                    device_exposure_id, person_id, device_concept_id, device_exposure_start_date, 
                    device_exposure_start_datetime, device_type_concept_id)
            VALUES
                    (1, 1, 0, '2022-01-01', TIMESTAMP('2022-01-01'), 0),
                    (2, 2, 0, '2022-01-01', TIMESTAMP('2022-01-01'), 0),
                    (3, 1, 0, '2022-01-01', TIMESTAMP('2022-01-01'), 0)
            """)

        drug_exposure_tmpl = self.jinja_env.from_string("""
            INSERT INTO `{{project_id}}.{{dataset_id}}.drug_exposure`
                (drug_exposure_id, person_id, drug_concept_id, drug_exposure_start_date,
                 drug_exposure_start_datetime, drug_type_concept_id)
            VALUES
                 (1, 1, 0, '2022-01-01', TIMESTAMP('2022-01-01'), 0),
                 (2, 2, 0, '2022-01-01', TIMESTAMP('2022-01-01'), 0),
                 (3, 1, 0, '2022-01-01', TIMESTAMP('2022-01-01'), 0)
            """)

        measurement_tmpl = self.jinja_env.from_string("""
            INSERT INTO `{{project_id}}.{{dataset_id}}.measurement`
                (measurement_id, person_id, measurement_concept_id,
                 measurement_date, measurement_type_concept_id)
            VALUES
                (1, 1, 0, '2022-01-01', 0),
                (2, 2, 0, '2022-01-01', 0),
                (3, 1, 0, '2022-01-01', 0)
            """)

        note_tmpl = self.jinja_env.from_string("""
            INSERT INTO `{{project_id}}.{{dataset_id}}.note`
                (note_id, person_id, note_date, note_datetime, note_type_concept_id,
                 note_class_concept_id, note_title, note_text, encoding_concept_id, language_concept_id)
            VALUES
                (1, 1, '2022-01-01', TIMESTAMP('2022-01-01'), 0, 0, '', '', 0, 0),
                (2, 2, '2022-01-01', TIMESTAMP('2022-01-01'), 0, 0, '', '', 0, 0),
                (3, 1, '2022-01-01', TIMESTAMP('2022-01-01'), 0, 0, '', '', 0, 0)
            """)

        observation_tmpl = self.jinja_env.from_string("""
            INSERT INTO `{{project_id}}.{{dataset_id}}.observation`
            (observation_id, person_id, observation_concept_id, observation_date, observation_type_concept_id)
            VALUES
                (1, 1, 0, '2022-01-01', 0),
                (2, 2, 0, '2022-01-01', 0),
                (3, 2, 0, '2022-01-01', 0)
            """)

        procedure_occurrence_tmpl = self.jinja_env.from_string("""
            INSERT INTO `{{project_id}}.{{dataset_id}}.procedure_occurrence`
                (procedure_occurrence_id, person_id, procedure_concept_id, procedure_concept_id,
                 procedure_date, procedure_datetime, procedure_type_concept_id )
            VALUES
                (1, 1, 0, '2022-01-01', TIMESTAMP('2022-01-01'), 0),
                (2, 2, 0, '2022-01-01', TIMESTAMP('2022-01-01'), 0),
                (3, 1, 0, '2022-01-01', TIMESTAMP('2022-01-01'), 0)
            """)

        specimen_tmpl = self.jinja_env.from_string("""
            INSERT INTO `{{project_id}}.{{dataset_id}}.specimen`
                (specimen_id, person_id, specimen_concept_id, specimen_type_concept_id, specimen_date)
            VALUES
                (1, 1, 0, 0, '2022-01-01'),
                (2, 2, 0, 0, '2022-01-01'),
                (3, 1, 0, 0, '2022-01-01')
            """)

        visit_occurrence_tmpl = self.jinja_env.from_string("""
            INSERT INTO `{{project_id}}.{{dataset_id}}.visit_occurrence`
                (visit_occurrence_id, person_id, visit_concept_id, visit_start_date,
                 visit_end_date, visit_type_concept_id)
            VALUES
                (1, 1, 0, '2022-01-01', '2022-01-01', 0),
                (2, 2, 0, '2022-01-01', '2022-01-01', 0),
                (3, 1, 0, '2022-01-01', '2022-01-01', 0)
            """)

        visit_detail_tmpl = self.jinja_env.from_string("""
            INSERT INTO `{{project_id}}.{{dataset_id}}.visit_detail`
                (visit_detail_id, person_id, visit_detail_concept_id, visit_detail_start_date, 
                visit_detail_end_date, visit_detail_type_concept_id, visit_occurrence_id)
            VALUES
                (1, 1, 0, '2022-01-01', '2022-01-01', 0, 0),
                (2, 2, 0, '2022-01-01', '2022-01-01', 0, 0),
                (3, 1, 0, '2022-01-01', '2022-01-01', 0, 0)
            """)

        person_query = person_tmpl.render(project_id=self.project_id,
                                          dataset_id=self.dataset_id)
        condition_occurrence_query = condition_occurrence_tmpl.render(
            project_id=self.project_id, dataset_id=self.dataset_id)
        death_query = death_tmpl.render(project_id=self.project_id,
                                        dataset_id=self.dataset_id)
        device_exposure_query = device_exposure_tmpl.render(
            project_id=self.project_id, dataset_id=self.dataset_id)
        drug_exposure_query = drug_exposure_tmpl.render(
            project_id=self.project_id, dataset_id=self.dataset_id)
        measurement_query = measurement_tmpl.render(project_id=self.project_id,
                                                    dataset_id=self.dataset_id)
        note_query = note_tmpl.render(project_id=self.project_id,
                                      dataset_id=self.dataset_id)
        observation_query = observation_tmpl.render(project_id=self.project_id,
                                                    dataset_id=self.dataset_id)
        specimen_query = specimen_tmpl.render(project_id=self.project_id,
                                              dataset_id=self.dataset_id)
        visit_occurrence_query = visit_occurrence_tmpl.render(
            project_id=self.project_id, dataset_id=self.dataset_id)
        visit_detail_query = visit_detail_tmpl.render(
            project_id=self.project_id, dataset_id=self.dataset_id)

        # load the test data
        self.load_test_data([
            person_query, condition_occurrence_query, death_query,
            device_exposure_query, drug_exposure_query, measurement_query,
            note_query, observation_query, specimen_query,
            visit_occurrence_query, visit_detail_query
        ])

    def test_queries(self):
        """
        Validates pre-conditions, test execution and post conditions based on the tables_and_counts variable.
        """
        tables_and_counts = [{
            'name': common.PERSON,
            'fq_table_name': self.fq_table_names[0],
            'fields': [
                'person_id', 'gender_concept_id', 'year_of_birth',
                'race_concept_id', 'ethnicity_concept_id'
            ],
            'loaded_ids': [1, 2, 3],
            'sandboxed_ids': [3],
            'cleaned_values': [(1, 2, 2000, 3, 4), (2, 3, 2001, 4, 5)]
        }]

        self.default_test(tables_and_counts)
