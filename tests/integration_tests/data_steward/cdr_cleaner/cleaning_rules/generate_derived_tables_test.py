"""
integration test for generate_ext_tables
"""
# Python imports
import os

# Third Party imports
import mock

# Project imports
from app_identity import get_application_id
from common import (CONDITION_ERA, DRUG_ERA, OBSERVATION_PERIOD)
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest
from cdr_cleaner.cleaning_rules.generate_derived_tables import CreateDerivedTables


class GenerateDerivedTablesTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        # set the test project identifier
        cls.project_id = get_application_id()

        # set the expected test datasets
        cls.dataset_id = os.getenv('DEID_DATASET_ID')
        cls.sandbox_id = f'{cls.dataset_id}_sandbox'
        cls.mapping_dataset_id = cls.dataset_id

        cls.rule_instance = CreateDerivedTables(cls.project_id, cls.dataset_id,
                                                cls.sandbox_id)

        sb_table_names = cls.rule_instance.get_sandbox_tablenames()
        for table_name in sb_table_names:
            cls.fq_sandbox_table_names.append(
                f'{cls.project_id}.{cls.sandbox_id}.{table_name}')

        cls.obs_prd = f'{cls.project_id}.{cls.dataset_id}.{OBSERVATION_PERIOD}'
        cls.drug_era = f'{cls.project_id}.{cls.dataset_id}.{DRUG_ERA}'
        cls.cond_era = f'{cls.project_id}.{cls.dataset_id}.{CONDITION_ERA}'

        cls.fq_table_names = [cls.obs_prd, cls.drug_era, cls.cond_era]

        # Tables Required for All CR Queries
        cls.required_tables = [
            'person', 'visit_occurrence', 'visit_occurrence_ext',
            'condition_occurrence', 'condition_occurrence_ext',
            'procedure_occurrence', 'procedure_occurrence_ext', 'drug_exposure',
            'drug_exposure_ext', 'device_exposure', 'device_exposure_ext',
            'observation', 'observation_ext', 'measurement', 'measurement_ext',
            'concept_ancestor', 'concept'
        ]
        for table in cls.required_tables:
            cls.fq_table_names.append(
                f'{cls.project_id}.{cls.dataset_id}.{table}')

        # call super to set up the client, create datasets, and create
        # empty test tables
        # NOTE:  does not create empty sandbox tables.
        super().setUpClass()

    def setUp(self):
        super().setUp()

        # pre-conditions
        obs_prd_tmpl = self.jinja_env.from_string("""
        INSERT INTO `{{obs_prd_name}}`
            (observation_period_id, person_id, observation_period_start_date, observation_period_end_date,
             period_type_concept_id)
        VALUES
            (11, 101, '2015-07-25', '2022-07-25', 44814725),
            (12, 102, '2015-08-25', '2022-08-25', 44814725),
            (13, 103, '2015-09-25', '2022-09-25', 44814725)
        """).render(obs_prd_name=self.obs_prd)

        drug_era_tmpl = self.jinja_env.from_string("""
        INSERT INTO `{{drug_era_name}}`
           (drug_era_id, person_id, drug_concept_id, drug_era_start_date, drug_era_end_date, drug_exposure_count,
            gap_days)
        VALUES
            (21, 101, 1, '2015-01-25', '2022-01-25', ,),
            (22, 102, 2, '2015-02-25', '2022-02-25', ,),
            (23, 103, 3, '2015-03-25', '2022-03-25', ,)
        """).render(drug_era_name=self.drug_era)

        cond_era_tmpl = self.jinja_env.from_string("""
        INSERT INTO `{{cond_era_name}}`
           (condition_era_id, person_id, condition_concept_id, condition_era_start_date, condition_era_end_date,
            condition_occurrence_count)
        VALUES
            (31, 101, 1, '2015-04-25', '2022-04-25', ),
            (32, 102, 2, '2015-05-25', '2022-05-25', ),
            (33, 103, 3, '2015-06-25', '2022-06-25', )
        """).render(cond_era_name=self.cond_era)

        queries = [
            obs_prd_tmpl,
            drug_era_tmpl,
            cond_era_tmpl,
        ]
        self.load_test_data(queries)

    def test_generate_derived_tables(self):
        """
        Test that derived tables are deleted and query runs as expected.
        The cleaning rule from DC-3729 cleans and repopulate 3 derived tables.
        """
        tables_and_counts = [{
            'fq_table_name': self.obs_prd,
            'fields': [],
            'loaded_ids': [],
            'tables_created_on_setup': [
                f'{self.project_id}.{self.dataset_id}.{table}'
                for table in self.required_tables
            ],
            'check_preconditions': False,
            'cleaned_values': []
        }]

        self.default_test(tables_and_counts)
