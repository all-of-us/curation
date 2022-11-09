"""
    Integration test for MapHealthInsuranceResponses

"""
# Python imports
import os

# Third party imports
import mock

# Third party imports

# Project imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.map_health_insurance_responses import (
    MapHealthInsuranceResponses, HEALTH_INSURANCE_PIDS, INSURANCE_LOOKUP)
from common import JINJA_ENV, OBSERVATION, VOCABULARY_TABLES
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest

LOAD_QUERY = JINJA_ENV.from_string("""
        INSERT INTO `{{project_id}}.{{dataset_id}}.observation` (
        observation_id,
        person_id,
        observation_concept_id, 
        value_as_string,
        value_as_concept_id,
        observation_source_value,
        observation_source_concept_id,
        value_source_value,
        value_source_concept_id,
        observation_date,
        observation_type_concept_id
    )
    VALUES
        -- For selected participants, hcau survey rows should be sandboxed and updated in observation --
    (1,1,40766241,
    "Medicaid, Medical Assistance, or any kind of government-assistance plan for those with low incomes or disability",
    0,"Insurance_InsuranceType",1384450,"InsuranceType_GovernmentAssistancePlan", 1384441,date('2000-01-01'),0),
    (5,1,40766241,"TRICARE or other military health care",0,"Insurance_InsuranceType",1384450,
    "InsuranceType_TricareOrMilitary", 1384550,date('2000-01-01'),0), 
    
        -- For selected participants, original basics survey rows should be sandboxed and invalidated --
    (2,2,43528428,
    "Medicaid, Medical Assistance, or any kind of government-assistance plan for those with low incomes or disability"
    ,0,"HealthInsurance_InsuranceTypeUpdate",43528428,"InsuranceTypeUpdate_Medicaid",43529209,date('2000-01-01'),0),
    
        -- For other participants, original basics survey rows should be unaffected --
    (3,3,43528428,
    "Medicaid, Medical Assistance, or any kind of government-assistance plan for those with low incomes or disability"
    ,0,"HealthInsurance_InsuranceTypeUpdate",43528428,"InsuranceTypeUpdate_Medicaid",43529209,date('2000-01-01'),0),
    
        -- For other participants, hcau survey rows should be unaffected. --
     (4,4,40766241,
    "Medicaid, Medical Assistance, or any kind of government-assistance plan for those with low incomes or disability",
    0,"Insurance_InsuranceType",1384450,"InsuranceType_GovernmentAssistancePlan", 1384441,date('2000-01-01'),0)
        """)

HEALTH_INSURANCE_PIDS_QUERY = JINJA_ENV.from_string("""
    INSERT INTO `{{project_id}}.{{dataset_id}}.health_insurance_pids` (person_id)
    VALUES
        (1), (2)
    """)


class MapHealthInsuranceResponsesTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        cls.project_id = os.environ.get(PROJECT_ID)
        cls.dataset_id = os.environ.get('RDR_DATASET_ID')
        cls.sandbox_id = f'{cls.dataset_id}_sandbox'
        cls.vocabulary_id = os.environ.get('VOCABULARY_DATASET')

        cls.rule_instance = MapHealthInsuranceResponses(cls.project_id,
                                                        cls.dataset_id,
                                                        cls.sandbox_id)

        sb_table_names = cls.rule_instance.get_sandbox_tablenames()
        for table_name in sb_table_names:
            cls.fq_sandbox_table_names.append(
                f'{cls.project_id}.{cls.sandbox_id}.{table_name}')

        cls.fq_sandbox_table_names.append(
            f'{cls.project_id}.{cls.sandbox_id}.{INSURANCE_LOOKUP}')

        cls.fq_table_names = [
            f'{cls.project_id}.{cls.dataset_id}.{table}'
            for table in [OBSERVATION] + VOCABULARY_TABLES +
            [HEALTH_INSURANCE_PIDS]
        ]

        # call super to set up the client, create datasets, and create
        # empty test tables
        # NOTE:  does not create empty sandbox tables.
        super().setUpClass()

    def setUp(self):

        super().setUp()

        self.copy_vocab_tables(self.vocabulary_id)

        self.load_test_data([
            LOAD_QUERY.render(project_id=self.project_id,
                              dataset_id=self.dataset_id),
            HEALTH_INSURANCE_PIDS_QUERY.render(project_id=self.project_id,
                                               dataset_id=self.dataset_id)
        ])

    def test_setup_rule(self):
        # run setup_rule and see if the affected_table is updated
        self.rule_instance.setup_rule(self.client)

        # sees that setup worked and reset affected_tables as expected
        self.assertEqual(set([OBSERVATION]),
                         set(self.rule_instance.affected_tables))

    def test_map_health_insurance_responses(self):
        """
        Use the default drop rows test function.

        Validates pre-conditions, test execution and post conditions based on
        the load statements and the tables_and_counts variable.
        """

        tables_and_counts = [{
            'fq_table_name':
                self.fq_table_names[0],
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[0],
            'fields': [
                'observation_id', 'person_id', 'observation_concept_id',
                'value_as_string', 'value_as_concept_id',
                'observation_source_value', 'observation_source_concept_id',
                'value_source_value', 'value_source_concept_id'
            ],
            'loaded_ids': [1, 2, 3, 4, 5],
            'sandboxed_ids': [1, 2, 5],
            'cleaned_values': [
                (1, 1, 40766241, "InsuranceTypeUpdate_Medicaid", 43529209,
                 "HealthInsurance_InsuranceTypeUpdate", 43528428,
                 "InsuranceTypeUpdate_Medicaid", 43529209),
                (5, 1, 40766241, "InsuranceTypeUpdate_Military", 45876394,
                 "HealthInsurance_InsuranceTypeUpdate", 43528428,
                 "InsuranceTypeUpdate_Military", 43529920),
                (2, 2, 43528428, "Invalid", 46237613,
                 "HealthInsurance_InsuranceTypeUpdate", 43528428, "Invalid",
                 46237613),
                (3, 3, 43528428,
                 "Medicaid, Medical Assistance, or any kind of government-assistance plan for those with low incomes or disability",
                 0, "HealthInsurance_InsuranceTypeUpdate", 43528428,
                 "InsuranceTypeUpdate_Medicaid", 43529209),
                (4, 4, 40766241,
                 "Medicaid, Medical Assistance, or any kind of government-assistance plan for those with low incomes or disability",
                 0, "Insurance_InsuranceType", 1384450,
                 "InsuranceType_GovernmentAssistancePlan", 1384441),
            ]
        }]

        with mock.patch(
                'cdr_cleaner.cleaning_rules.map_health_insurance_responses.PIPELINE_TABLES',
                self.dataset_id):

            self.default_test(tables_and_counts)
