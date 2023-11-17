"""
Integration test for GeneralizeIndianHealthServices module
"""

# Python Imports

# Project imports
from app_identity import get_application_id
from cdr_cleaner.cleaning_rules.deid.generalize_indian_health_services import GeneralizeIndianHealthServices
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest
from common import COMBINED_DATASET_ID, OBSERVATION


class GeneralizeIndianHealthServicesTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        cls.project_id = get_application_id()
        cls.dataset_id = COMBINED_DATASET_ID
        cls.sandbox_id = f'{cls.dataset_id}_sandbox'

        cls.rule_instance = GeneralizeIndianHealthServices(
            cls.project_id, cls.dataset_id, cls.sandbox_id)

        sandbox_table_names = cls.rule_instance.get_sandbox_tablenames()
        for table in sandbox_table_names:
            cls.fq_sandbox_table_names.append(
                f'{cls.project_id}.{cls.sandbox_id}.{table}')

        cls.fq_table_names = [
            f'{cls.project_id}.{cls.dataset_id}.{OBSERVATION}',
        ]

        super().setUpClass()

    def setUp(self):
        super().setUp()

        insert_test_data = self.jinja_env.from_string("""
        INSERT INTO `{{project}}.{{dataset}}.observation`
            (observation_id, person_id, observation_date, observation_type_concept_id,
            observation_concept_id, observation_source_concept_id, 
            value_source_concept_id, value_as_concept_id)
        VALUES
            -- Does not require generalization --
            (101, 1, '2020-01-01', 0, 9999, 9999, 9999, 9999),
            -- Need generalization. No duplicates. --
            (102, 1, '2020-01-01', 0, 40766241, 1384450, 1384516, 45883720),
            (103, 1, '2020-01-01', 0, 1585389, 1585389, 1585396, 45883720),
            (104, 1, '2020-01-01', 0, 43528428, 43528428, 43529111, 45883720),
            -- Does not require generalization --
            (201, 2, '2020-01-01', 0, 9999, 9999, 9999, 9999),
            -- 211 needs generalization. 211 and 212 become duplicates as a result. --
            -- 212 stays b/c its date is more recent. 211 gets dropped. --
            (211, 2, '2019-01-01', 0, 40766241, 1384450, 1384516, 45883720),
            (212, 2, '2020-01-01', 0, 40766241, 1384450, 1384595, 1384595),
            -- 221 needs generalization. 221 and 222 become duplicates as a result. --
            -- 221 stays b/c 221 and 222 have same dates and 221 has smaller observation_id. --
            (221, 2, '2020-01-01', 0, 1585389, 1585389, 1585396, 45883720),
            (222, 2, '2020-01-01', 0, 1585389, 1585389, 1585398, 45876762),
            -- 231 & 232's case is same as 221 & 222 but with different value_X_concept_ids. --
            (231, 2, '2020-01-01', 0, 43528428, 43528428, 43529111, 45883720),
            (232, 2, '2020-01-01', 0, 43528428, 43528428, 43528423, 43528423)
        """).render(project=self.project_id, dataset=self.dataset_id)

        self.load_test_data([insert_test_data])

    def test_generalize_indian_health_services(self):
        """
        Test cases for Indian Health Service generalization.
                
        NOTE string fields are not target of this cleaning rule.
        `StringFieldsSuppression` will take care of the string field suppression.
        """
        tables_and_counts = [{
            'fq_table_name':
                self.fq_table_names[0],
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[0],
            'loaded_ids': [
                101, 102, 103, 104, 201, 211, 212, 221, 222, 231, 232
            ],
            'sandboxed_ids': [102, 103, 104, 211, 221, 231],
            'fields': [
                'observation_id', 'person_id', 'observation_concept_id',
                'observation_source_concept_id', 'value_source_concept_id',
                'value_as_concept_id'
            ],
            'cleaned_values': [(101, 1, 9999, 9999, 9999, 9999),
                               (102, 1, 40766241, 1384450, 1384595, 1384595),
                               (103, 1, 1585389, 1585389, 1585398, 45876762),
                               (104, 1, 43528428, 43528428, 43528423, 43528423),
                               (201, 2, 9999, 9999, 9999, 9999),
                               (212, 2, 40766241, 1384450, 1384595, 1384595),
                               (221, 2, 1585389, 1585389, 1585398, 45876762),
                               (231, 2, 43528428, 43528428, 43528423, 43528423)]
        }]

        self.default_test(tables_and_counts)
