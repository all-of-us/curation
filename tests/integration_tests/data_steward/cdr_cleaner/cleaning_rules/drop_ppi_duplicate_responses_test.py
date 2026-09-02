"""
Integration test for clean_ppi_numeric_fields_using_parameters module

Removes the duplicate sets of responses to the same questions excluding COPE survey

Original Issues: DC-1051, DC-532

In PPI surveys, the purpose of questionnaire_response_id is to group all responses from the same survey together.
Some PPI questions allowed participants to provide multiple answers, which be will connected via the same
questionnaire_response_id. However, a participant may submit the responses multiple times for the same questions,
therefore creating duplicates. We need to use the combination of person_id, observation_source_concept_id,
observation_source_value, and questionnaire_response_id to identify multiple sets of responses. We only want to keep
the most recent set of responses and remove previous sets of responses. When identifying the most recent responses,
we can't use questionnaire_response_id alone because a larger questionnaire_response_id doesn't mean it's created at
a later time therefore we need to create ranks (lowest rank = most recent responses) based on observation_date_time.
However, we also need to add questionnaire_response_id for assigning unique ranks to different sets of responses
because there are cases where the multiple sets of responses for the same question were submitted at exactly the same
timestamp but with different answers. In addition, we also need to check whether one of the duplicate responses is
PMI_Skip because there are cases where the most recent response is a skip and the legitimate response was submitted
earlier, we want to keep the actual response instead of PMI_Skip regardless of the timestamps of those responses.
"""

# Python Imports
import os

# Project Imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.drop_ppi_duplicate_responses import DropPpiDuplicateResponses
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest


class DropPpiDuplicateResponsesTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        # Set the test project identifier
        project_id = os.environ.get(PROJECT_ID)
        cls.project_id = project_id

        # Set the expected test datasets
        dataset_id = os.environ.get('RDR_DATASET_ID')
        cls.dataset_id = dataset_id
        sandbox_id = dataset_id + '_sandbox'
        cls.sandbox_id = sandbox_id

        cls.rule_instance = DropPpiDuplicateResponses(project_id, dataset_id,
                                                      sandbox_id)

        sb_table_names = cls.rule_instance.get_sandbox_tablenames()
        for table_name in sb_table_names:
            cls.fq_sandbox_table_names.append(
                f'{project_id}.{sandbox_id}.{table_name}')

        cls.fq_table_names = [
            f'{project_id}.{dataset_id}.observation',
            f'{project_id}.{dataset_id}.cope_survey_semantic_version_map',
            # joined by the DL-2483 exclusion, which identifies a record emitted onto
            # a linked adult by its observation belonging to a different participant
            # than the survey that produced it
            f'{project_id}.{dataset_id}.survey_conduct'
        ]

        # call super to set up the client, create datasets, and create
        # empty test tables
        # NOTE:  does not create empty sandbox tables.
        super().setUpClass()

    def setUp(self):
        """
        Create common information for tests.

        Creates common expected parameter types from cleaned tables and a common
        fully qualified (fq) dataset name string to load the data.
        """
        fq_dataset_name = self.fq_table_names[0].split('.')
        self.fq_dataset_name = '.'.join(fq_dataset_name[:-1])

        super().setUp()

    def test_field_cleaning(self):
        """
        Tests that the specifications for the SANDBOX_QUERY and CLEAN_PPI_NUMERIC_FIELDS_QUERY
        perform as designed.

        Validates pre conditions, tests execution, and post conditions based on the load
        statements and the tables_and_counts variable.
        """

        tmpl = self.jinja_env.from_string("""
            INSERT INTO `{{fq_dataset_name}}.observation` (observation_id,
    person_id,
    observation_concept_id,
    observation_date,
    observation_datetime,
    observation_type_concept_id,
    observation_source_concept_id,
    observation_source_value,
    value_source_value,
    questionnaire_response_id)
VALUES
  (123, 1111111, 1384662, DATE('2015-09-15'), TIMESTAMP('2015-09-15'), 45905771, 1384662,
   'InfectiousDiseases_InfectiousDiseaseCondition', 'InfectiousDiseaseCondition_Chickenpox', 111111111),
  (234, 1111111, 1384662, DATE('2015-07-15'), TIMESTAMP('2015-07-15'),45905771, 1384662,
   'InfectiousDiseases_InfectiousDiseaseCondition', 'InfectiousDiseaseCondition_NoInfectiousDisease', 222222222),
  (345, 2222222, 3044964, DATE('2015-07-15'), TIMESTAMP('2015-07-15'), 45905771, 1333276,
   'phq_9_4', 'COPE_A_75', 333333333),
  (456, 2222222, 3044964, DATE('2015-08-15'), TIMESTAMP('2015-08-15'), 45905771, 1333276,
   'phq_9_4', 'COPE_A_161', 444444444),
  (567, 3333333, 4214956, DATE('2015-09-15'), TIMESTAMP('2015-09-15'), 45905771, 1384522,
   'Circulatory_CirculatoryConditions', 'CirculatoryConditions_Hypertension', 555555555),
  (678, 3333333, 4214956, DATE('2015-08-15'), TIMESTAMP('2015-08-15'), 45905771, 1384522,
   'Circulatory_CirculatoryConditions', 'PMI_Skip', 666666666),
  (789, 3333333, 4214956, DATE('2015-07-15'), TIMESTAMP('2015-07-15'), 45905771, 1384522,
   'Circulatory_CirculatoryConditions', 'PMI_Skip', 777777777),
  (890, 4444444, 1586213, DATE('2015-08-15'), TIMESTAMP('2015-08-15'), 45905771, 1586213,
   'Alcohol_6orMoreDrinksOccurence', '6orMoreDrinksOccurence_LessThanMonthly', 888888888),
  (901, 4444444, 1586213, DATE('2015-09-15'), TIMESTAMP('2015-09-15'), 45905771, 1586213,
   'Alcohol_6orMoreDrinksOccurence', '6orMoreDrinksOccurence_Weekly', 999999999)"""
                                         )

        tmp2 = self.jinja_env.from_string("""
        INSERT INTO `{{fq_dataset_name}}.cope_survey_semantic_version_map` (
              participant_id, questionnaire_response_id,
              semantic_version,
              cope_month)
        VALUES (2222222, 333333333, '4', 'jul' ),
              (2222222, 555555555, '4', 'jul' ),
              (2222222, 444444444, '6', 'aug' ),
              (2222222, 666666666, '14','dec' )
                """)

        query = tmpl.render(fq_dataset_name=self.fq_dataset_name)
        self.load_test_data([query])

        query = tmp2.render(fq_dataset_name=self.fq_dataset_name)
        self.load_test_data([query])

        # Expected results list
        tables_and_counts = [{
            'fq_table_name':
                '.'.join([self.fq_dataset_name, 'observation']),
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[0],
            'loaded_ids': [123, 234, 345, 456, 567, 678, 789, 890, 901],
            'sandboxed_ids': [234, 678, 789, 890],
            'fields': ['observation_id', 'questionnaire_response_id'],
            'cleaned_values': [(123, 111111111), (345, 333333333),
                               (456, 444444444), (567, 555555555),
                               (901, 999999999)]
        }]

        self.default_test(tables_and_counts)

    def test_emitted_pediatric_adult_record_is_excluded(self):
        """
        A record emitted onto a linked adult from a pediatric survey (DL-2483) must not
        take part in the duplicate ranking.

        Adult 500 answered EducationLevel_HighestGrade at enrollment in 2015, and a
        pediatric reassessment for their child lands on them in 2026. The reassessment
        is later, so without the exclusion it would rank first and the enrollment
        answer would be deleted. Both must survive.

        Income_AnnualIncome is the control: an ordinary duplicate on the same adult,
        both rows belonging to the adult's own surveys, is still deleted. Without it
        this test would also pass if the exclusion disabled the rule outright.
        """
        observation_tmpl = self.jinja_env.from_string("""
        INSERT INTO `{{fq_dataset_name}}.observation`
        (observation_id, person_id, observation_concept_id, observation_date,
         observation_datetime, observation_type_concept_id, observation_source_concept_id,
         observation_source_value, value_source_value, questionnaire_response_id)
        VALUES
        -- adult 500's own enrollment answer --
          (1001, 500, 40771091, DATE('2015-01-01'), TIMESTAMP('2015-01-01'), 45905771,
           1585940, 'EducationLevel_HighestGrade', 'HighestGrade_CollegeGraduate', 700),
        -- emitted onto adult 500 from child 501's pediatric survey, and later --
          (1002, 500, 40771091, DATE('2026-01-01'), TIMESTAMP('2026-01-01'), 45905771,
           1585940, 'EducationLevel_HighestGrade', 'HighestGrade_AdvancedDegree', 800),
        -- control: an ordinary duplicate on the adult's own surveys, still deleted --
          (1003, 500, 46235933, DATE('2015-01-01'), TIMESTAMP('2015-01-01'), 45905771,
           1585375, 'Income_AnnualIncome', 'AnnualIncome_10k25k', 700),
          (1004, 500, 46235933, DATE('2016-01-01'), TIMESTAMP('2016-01-01'), 45905771,
           1585375, 'Income_AnnualIncome', 'AnnualIncome_50k75k', 701)
        """)

        survey_conduct_tmpl = self.jinja_env.from_string("""
        INSERT INTO `{{fq_dataset_name}}.survey_conduct`
        (survey_conduct_id, person_id, survey_concept_id, survey_source_value)
        VALUES
          (700, 500, 1586134, 'TheBasics'),
          (701, 500, 1586134, 'TheBasics'),
        -- keyed to the child, which is what marks 1002 as emitted --
          (800, 501, 0, 'ped_basics')
        """)

        self.load_test_data([
            observation_tmpl.render(fq_dataset_name=self.fq_dataset_name),
            survey_conduct_tmpl.render(fq_dataset_name=self.fq_dataset_name)
        ])

        tables_and_counts = [{
            'fq_table_name': '.'.join([self.fq_dataset_name, 'observation']),
            'fq_sandbox_table_name': self.fq_sandbox_table_names[0],
            'loaded_ids': [1001, 1002, 1003, 1004],
            'sandboxed_ids': [1003],
            'fields': ['observation_id', 'questionnaire_response_id'],
            'cleaned_values': [(1001, 700), (1002, 800), (1004, 701)]
        }]

        self.default_test(tables_and_counts)
