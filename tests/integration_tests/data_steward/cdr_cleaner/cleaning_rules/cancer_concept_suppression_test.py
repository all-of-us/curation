"""
Integration test for cancer_concept_suppression module

This rule sandboxes and suppresses reccords whose concept_codes end in 
'History_WhichConditions', 'History_AdditionalDiagnosis',
and 'OutsideTravel6MonthsWhere'.

Runs on the controlled tier.


Original Issue: DC-1381
"""

# Python Imports
import os

# Third party imports
from dateutil import parser

#Project imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.cancer_concept_suppression import CancerConceptSuppression, \
SUPPRESSION_RULE_CONCEPT_TABLE
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest
from common import CONCEPT, OBSERVATION


class CancerConceptSuppressionTest(BaseTest.CleaningRulesTestBase):

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
        dataset_id = os.environ.get('COMBINED_DATASET_ID')
        cls.dataset_id = dataset_id
        sandbox_id = dataset_id + '_sandbox'
        cls.sandbox_id = sandbox_id

        cls.rule_instance = CancerConceptSuppression(project_id, dataset_id,
                                                     sandbox_id)

        sb_table_names = cls.rule_instance.sandbox_table_for(OBSERVATION)

        cls.fq_sandbox_table_names.append(
            f'{cls.project_id}.{cls.sandbox_id}.{sb_table_names}')
        cls.fq_sandbox_table_names.append(
            f'{cls.project_id}.{cls.sandbox_id}.{SUPPRESSION_RULE_CONCEPT_TABLE}'
        )

        cls.fq_table_names = [
            f'{project_id}.{dataset_id}.{OBSERVATION}',
            f'{project_id}.{dataset_id}.{CONCEPT}',
        ]

        # call super to set up the client, create datasets, and create
        # empty test tables
        # NOTE:  does not create empty sandbox tables.
        super().setUpClass()

    def setUp(self):
        fq_dataset_name = self.fq_table_names[0].split('.')
        self.fq_dataset_name = '.'.join(fq_dataset_name[:-1])

        self.date = parser.parse('2020-05-05').date()

        super().setUp()

    def test_cancer_concept_suppression_cleaning(self):
        """
        Tests that the sepcifications for QUERYNAME perform as designed.

        Validates pre conditions, tests execution, and post conditions based on the load
        statements and the tables_and_counts variable.        
        """
        queries = []

        #Append some queries

        create_concepts_query_tmpl = self.jinja_env.from_string("""
            INSERT INTO `{{fq_dataset_name}}.concept`
                (concept_id, concept_name, domain_id, vocabulary_id, concept_class_id, concept_code, valid_start_date, valid_end_date)
            VALUES
                (43529626, "some text", "some text", "PPI", "some text", "OutsideTravel6Month_OutsideTravel6MonthWhereTravel", date('2020-05-05'), date('2020-05-05')),
                (43529099, "some text", "some text", "PPI", "some text", "OutsideTravel6Month_OutsideTravel6MonthWhere", date('2020-05-05'), date('2020-05-05')),
                (43529102, "some text", "some text", "PPI", "some text", "MotherDiagnosisHistory_WhichConditions", date('2020-05-05'), date('2020-05-05')),
                (43529627, "some text", "some text", "PPI", "some text", "CancerCondition_OtherCancer", date('2020-05-05'), date('2020-05-05')),
                (43529625, "some text", "some text", "PPI", "some text", "FatherCancerCondition_OtherCancers", date('2020-05-05'), date('2020-05-05')),
                (43529100, "some text", "some text", "PPI", "some text", "SonCancerCondition_History_AdditionalDiagnosis", date('2020-05-05'), date('2020-05-05')),
                (10821410, "some text", "some text", "PPI", "some text", "Sister_History_AdditionalDiagnoses", date('2020-05-05'), date('2020-05-05')),
                (42181902, "some text", "some text", "PPI", "some text", "Cancer", date('2020-05-05'), date('2020-05-05')),
                (24182910, "some text", "some text", "PPI", "some text", "", date('2020-05-05'), date('2020-05-05')),
                (43529098, "some text", "some text", "PPI", "some text", "FatherDiagnosisHistory_WhichConditions", date('2020-05-05'), date('2020-05-05'))
        """).render(fq_dataset_name=self.fq_dataset_name)

        drop_records_query_tmpl = self.jinja_env.from_string("""
            INSERT INTO `{{fq_dataset_name}}.observation`
                (observation_id, person_id, observation_concept_id, observation_date, 
                observation_type_concept_id)
            VALUES
                (1, 1, 43529626, date('2020-05-05'), 1),
                (2, 2, 43529099, date('2020-05-05'), 2),
                (3, 3, 43529102, date('2020-05-05'), 3),
                (4, 4, 43529627, date('2020-05-05'), 4),
                (5, 5, 43529625, date('2020-05-05'), 5),
                (6, 6, 43529100, date('2020-05-05'), 6),
                (7, 7, 43529098, date('2020-05-05'), 7),
                (8, 8, 10821410, date('2020-05-05'), 8),
                (9, 9, 42181902, date('2020-05-05'), 9),
                (10, 10, 24182910, date('2020-05-05'), 10)
            """).render(fq_dataset_name=self.fq_dataset_name)

        queries = [create_concepts_query_tmpl, drop_records_query_tmpl]

        self.load_test_data(queries)

        #Uncomment below and fill

        tables_and_counts = [{
            'fq_table_name':
                '.'.join([self.fq_dataset_name, 'observation']),
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[0],
            'loaded_ids': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            'sandboxed_ids': [1, 2, 3, 6, 7],
            'fields': [
                'observation_id', 'person_id', 'observation_concept_id',
                'observation_date', 'observation_type_concept_id'
            ],
            'cleaned_values': [(4, 4, 43529627, self.date, 4),
                               (5, 5, 43529625, self.date, 5),
                               (8, 8, 10821410, self.date, 8),
                               (9, 9, 42181902, self.date, 9),
                               (10, 10, 24182910, self.date, 10)]
        }]

        self.default_test(tables_and_counts)
