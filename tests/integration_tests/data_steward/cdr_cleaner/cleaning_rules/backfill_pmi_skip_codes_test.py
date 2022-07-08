"""

"""

# Python Imports
import os

# Third party imports
from dateutil import parser

# Project imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.backfill_pmi_skip_codes import BackfillPmiSkipCodes
from common import JINJA_ENV, OBSERVATION, PERSON
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest


class BackfillPmiSkipCodesTest(BaseTest.CleaningRulesTestBase):

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
        dataset_id = os.environ.get('RDR_DATASET_ID')
        cls.dataset_id = dataset_id
        sandbox_id = dataset_id + '_sandbox'
        cls.sandbox_id = sandbox_id

        cls.rule_instance = BackfillPmiSkipCodes(project_id, dataset_id,
                                                 sandbox_id)

        cls.fq_sandbox_table_names = []

        cls.fq_table_names = [
            f'{project_id}.{dataset_id}.{OBSERVATION}',
            f'{project_id}.{dataset_id}.{PERSON}',
        ]

        super().setUpClass()

    def setUp(self):
        self.date = parser.parse('2020-05-05').date()

        super().setUp()

    def test_backfill_pmi_skip_codes(self):
        """
        Tests that the sepcifications for QUERYNAME perform as designed.

        Validates pre conditions, tests execution, and post conditions based on the load
        statements and the tables_and_counts variable.        
        """
        queries = []

        #Append some queries

        insert_observation = JINJA_ENV.from_string("""
            INSERT INTO `{{project}}.{{dataset}}.observation`
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
        """)

        insert_person = JINJA_ENV.from_string("""
            INSERT INTO `{{project}}.{{dataset}}.person`
                (person_id, gender_concept_id, year_of_birth, race_concept_id, ethnicity_concept_id)
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
        """)

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