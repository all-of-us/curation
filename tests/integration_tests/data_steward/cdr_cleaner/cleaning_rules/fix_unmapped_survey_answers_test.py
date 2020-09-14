"""
Integration test for fix_unmapped_survey_answers module

Original Issues: DC-1043, DC-1053

The intent is to map the unmapped survey answers (value_as_concept_ids=0) using 
value_source_concept_id through 'Maps to' relationship """

# Python Imports
import os

# Project Imports
from common import OBSERVATION, CONCEPT, CONCEPT_RELATIONSHIP
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.fix_unmapped_survey_answers import FixUnmappedSurveyAnswers
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import \
    BaseTest


class FixUnmappedSurveyAnswersTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        # Set the test project identifier
        cls.project_id = os.environ.get(PROJECT_ID)

        # Set the expected test datasets
        cls.dataset_id = os.environ.get('RDR_DATASET_ID')
        cls.sandbox_id = cls.dataset_id + '_sandbox'

        cls.query_class = FixUnmappedSurveyAnswers(cls.project_id,
                                                   cls.dataset_id,
                                                   cls.sandbox_id)

        # Generates list of fully qualified table names
        for table_name in [OBSERVATION, CONCEPT, CONCEPT_RELATIONSHIP]:
            cls.fq_table_names.append(
                f'{cls.project_id}.{cls.dataset_id}.{table_name}')

        sb_table_names = cls.query_class.get_sandbox_tablenames()
        for table_name in sb_table_names:
            cls.fq_sandbox_table_names.append(
                f'{cls.project_id}.{cls.sandbox_id}.{table_name}')

        # call super to set up the client, create datasets
        cls.up_class = super().setUpClass()

    def setUp(self):
        """
        Create empty tables for the rule to run on
        """
        # Create the observation, concept, and concept_relationship tables required for the test
        super().setUp()

    def test_fix_unmapped_survey_answers(self):
        """
        Question concept_id: 1384592 (Respiratory: Respiratory Conditions). 
        value_as_concept_id/value_source_concept_id: 1384615 (Respiratory Conditions: No Lung 
        Condition), which is a standard concept_id. This record doesn't not get sandboxed and 
        updated 
        
        Question concept_id: 1384592 (Respiratory: Respiratory Conditions). 
        value_source_concept_id: 1384615 (Respiratory Conditions: No Lung Condition), which is a 
        standard concept_id. Set value_as_concept_id to the value_source_concept_id 
        
        Question concept_id: 1332749 (Choose the answer that best describes how you felt in the 
        past month. In general, how happy are you?). value_source_concept_id : 1332776 (
        Moderately unhappy), which is non-standard but can be mapped to a standard concept_id 
        through Maps to. The domain of the corresponding concept_id is 'Answer'. Set 
        value_as_concept_id to the mapped concept_id (1333079) 
        
        Question concept_id: 43528660 (Health Advice: Spoken To General Doctor). 
        value_source_concept_id : 43530243 (Spoken To General Doctor: Yes), which is non-standard 
        but can be mapped to a standard concept_id through Maps to. The domain of the corresponding 
        concept_id is 'Clinical finding'. Set value_as_concept_id to the mapped concept_id (4088548)
        
        Question concept_id: 1384487 (Kidney: Kidney Conditions). value_source_concept_id : 
        1384515 (Kidney Conditions: Kidney Stones), which is non-standard but can be mapped to a 
        standard concept_id through Maps to. The domain of the corresponding concept_id is 
        'Context-dependent'. Set value_as_concept_id to the mapped concept_id (4179221) 
        
        Question concept_id: 1332756 (How long has it been since you last used an electronic 
        nicotine product?). value_source_concept_id : 1332724 (Months), which is non-standard but 
        can be mapped to a standard concept_id through Maps to. The domain of the corresponding 
        concept_id is 'Unit'. Set value_as_concept_id to the mapped concept_id (9580) 
        
        Question concept_id: 1586151 (Asian: Asian Specific). value_source_concept_id : 1585609 (
        Asian Specific: Asian Specific Indian), which is non-standard but can be mapped to a 
        standard concept_id through Maps to. However, the domain of the corresponding concept_id 
        is 'Question', so we don't include this mapping and set value_as_concept_id to 0. 
        
        Question concept_id: 0 (No matching concept). value_source_concept_id : 1585536 (Yes), 
        which is a deprecated concept_id and set value_as_concept_id to 0. 
        
        :return: 
        """

        concept_empl = self.jinja_env.from_string("""
         INSERT INTO `{{fq_dataset_name}}.concept`
            (concept_id,concept_name,domain_id,vocabulary_id,concept_class_id,standard_concept,concept_code,valid_start_date,valid_end_date,invalid_reason)
            VALUES
                (1585536,'Yes','Meas Value','PPI','Answer',null,'JacksonHindsAppointment_Yes','2017-07-26','2018-07-31','D'),
                (1384615,'Respiratory Conditions: No Lung Condition','Observation','PPI','Answer','S','RespiratoryConditions_NoLungCondition', '2018-10-10', '2099-12-31',null),
                (43530243,'Spoken To General Doctor: Yes','Observation','PPI','Answer',null,'SpokenToGeneralDoctor_Yes','2018-01-31','2099-12-31',null),
                (1332776,'Moderately unhappy','Observation','PPI','Answer',null,'cope_a_31','2020-05-07','2099-12-31',null),
                (1585609,'Asian Specific: Asian Specific Indian','Observation','PPI','Answer',null,'AsianSpecific_AsianSpecificIndian','2017-05-22','2099-12-31',null),
                (1384515,'Kidney Conditions: Kidney Stones','Observation','PPI','Answer',null,'KidneyConditions_KidneyStones','2018-10-09','2099-12-31',null),
                (4179221,'History of calculus of kidney','Observation','SNOMED','Context-dependent','S','429025008','2008-01-31','2099-12-31',null),
                (4088548,'Seen by general practitioner','Observation','SNOMED','Clinical Finding','S','185278000','1970-01-01','2099-12-31',null),
                (1586151,'Asian: Asian Specific','Observation','PPI','Question','S','Asian_AsianSpecific','2017-05-17','2099-12-31',null),
                (1333079,'Moderately unhappy','Observation','PPI','Answer','S','mhwb_a_40','2020-05-07','2099-12-31',null),
                (9580,'month','Unit','UCUM','Unit','S','mo','1970-01-01','2099-12-31',null),
                (1332724,'Months','Observation','PPI','Answer',null,'cope_a_198','2020-05-07','2099-12-31',null)
        """)

        insert_concept_query = concept_empl.render(
            fq_dataset_name=f'{self.project_id}.{self.dataset_id}')

        concept_relationship_empl = self.jinja_env.from_string("""
                 INSERT INTO `{{fq_dataset_name}}.concept_relationship`
                    (concept_id_1,concept_id_2,relationship_id,valid_start_date,valid_end_date,invalid_reason)
                    VALUES
                        (1332776,1333079,'Maps to','2020-05-07','2099-12-31',null),
                        (1384515,4179221,'Maps to','2019-04-21','2099-12-31',null),
                        (1384615,1384615,'Maps to','2019-04-21','2099-12-31',null),
                        (43530243,4088548,'Maps to','2019-04-21','2099-12-31',null),
                        (1585609,1586151,'Maps to','2019-04-21','2099-12-31',null),
                        (1332724,9580,'Maps to','2020-05-07','2099-12-31',null)
                """)

        insert_concept_relationship_query = concept_relationship_empl.render(
            fq_dataset_name=f'{self.project_id}.{self.dataset_id}')

        observation_tmpl = self.jinja_env.from_string("""
            INSERT INTO `{{fq_dataset_name}}.observation`
            (observation_id, person_id, observation_concept_id, observation_date, 
             observation_type_concept_id, value_as_concept_id, value_source_concept_id)
            VALUES
                (1, 111111, 1384592, '2015-07-15', 45905771, 1384615, 1384615),
                (2, 222222, 1384592, '2015-07-15', 45905771, 0, 1384615),
                (3, 333333, 1332749, '2015-07-15', 45905771, 0, 1332776),
                (4, 444444, 43528660, '2015-07-15', 45905771, 0, 43530243),
                (5, 555555, 1384487, '2015-07-15', 45905771, 0, 1384515), 
                (6, 666666, 1332756, '2015-07-15', 45905771, 0, 1332724),
                (7, 777777, 1586151, '2015-07-15', 45905771, 0, 1585609), 
                (8, 888888, 0, '2015-07-15', 45905771, 0, 1585536)
                """)
        insert_observation_query = observation_tmpl.render(
            fq_dataset_name=f'{self.project_id}.{self.dataset_id}')

        # Load test data
        self.load_test_data([
            f'''{insert_observation_query};
                {insert_concept_query};
                {insert_concept_relationship_query}'''
        ])

        # Expected results list
        tables_and_counts = [{
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.observation',
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[0],
            'loaded_ids': [1, 2, 3, 4, 5, 6, 7, 8],
            'sandboxed_ids': [2, 3, 4, 5, 6],
            'fields': [
                'observation_id', 'observation_concept_id',
                'value_source_concept_id', 'value_as_concept_id'
            ],
            'cleaned_values': [(1, 1384592, 1384615, 1384615),
                               (2, 1384592, 1384615, 1384615),
                               (3, 1332749, 1332776, 1333079),
                               (4, 43528660, 43530243, 4088548),
                               (5, 1384487, 1384515, 4179221),
                               (6, 1332756, 1332724, 9580),
                               (7, 1586151, 1585609, 0), (8, 0, 1585536, 0)]
        }]

        self.default_test(tables_and_counts)
