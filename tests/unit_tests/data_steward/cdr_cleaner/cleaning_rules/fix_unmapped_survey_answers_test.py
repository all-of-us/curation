"""Original Issues: DC-1043, DC-1053 

PPI answers are mapped to standard answer concepts in concept_relationship through 'Maps to value' (it has been this 
case historically and it is still the case now), however, there are a bunch of PPI answer concepts (619 in total in 
rdr20200807) missing such relationships in concept_relationship. Interestingly, the corresponding standard PPI 
answer concepts could be found through 'Maps to'.  This might have been a bug in the vocabulary and they 
probably should’ve used 'Maps to value' for mapping Answer concepts. 

There are 619 unique answers (value_source_concept_ids) in observation that are mapped to a '0' value_as_concept_id, 
out of which 368 are standard concepts, 202 are non-standard concepts that could be mapped to a standard concept 
through Maps to in concept_relationship, and 49 are deprecated concepts that do not map to anything.

For the 368 standard answers, we could just use it as-is for populating value_as_concept_id. However, among the 202 
non-standard concepts, not all of the mapped standard concepts are classified as 'Answer' and they could belong to 
other concept_classes. Below is a breakdown of the concept_class_id of the mapped concepts: 

95 Context-dependent
89 Answer
8 Clinical Finding
6 Question
3 Unit
1 Module

Question or Module concept classes don't make sense so will get excluded. In conclusion, 

1. For 368 standard concepts --> Set value_as_concept_id to the source_value_concept_id.
2. For 49 deprecated concepts --> Set value_as_concept_id to 0. Actually we don't need to do anything for this case 
because visit_as_concept_id is already 0.
3, For 202 non-standard concepts --> Set value_as_concept_id to standard concept ids mapped through 'Maps to' 
for the concept classes ('Answer', 'Context-dependent', 'Clinical Finding', 'Unit') only. 
"""

# Python imports
import unittest

# Project imports
from cdr_cleaner.cleaning_rules.fix_unmapped_survey_answers import FixUnmappedSurveyAnswers, \
    SANDBOX_FIX_UNMAPPED_SURVEY_ANSWERS_QUERY, UPDATE_FIX_UNMAPPED_SURVEY_ANSWERS_QUERY, OBSERVATION, JIRA_ISSUE_NUMBERS
from constants.bq_utils import WRITE_TRUNCATE
from constants.cdr_cleaner import clean_cdr as clean_consts
import constants.cdr_cleaner.clean_cdr as cdr_consts


class FixUnmappedSurveyAnswersTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'test_project'
        self.dataset_id = 'test_dataset'
        self.sandbox_id = 'test_sandbox'
        self.sandbox_table_name = '_'.join(
            JIRA_ISSUE_NUMBERS).lower() + '_' + OBSERVATION
        self.client = None

        self.query_class = FixUnmappedSurveyAnswers(self.project_id,
                                                    self.dataset_id,
                                                    self.sandbox_id)

        self.assertEqual(self.query_class.project_id, self.project_id)
        self.assertEqual(self.query_class.dataset_id, self.dataset_id)
        self.assertEqual(self.query_class.sandbox_dataset_id, self.sandbox_id)

    def test_setup_rule(self):
        # Test
        self.query_class.setup_rule(self.client)

    def test_get_sandbox_tablenames(self):
        self.assertListEqual(self.query_class.get_sandbox_tablenames(),
                             [self.sandbox_table_name])

    def test_get_query_specs(self):
        # Pre conditions
        self.assertEqual(self.query_class.affected_datasets, [clean_consts.RDR])

        # Test
        results_list = self.query_class.get_query_specs()

        sandbox_query_dict = {
            cdr_consts.QUERY:
                SANDBOX_FIX_UNMAPPED_SURVEY_ANSWERS_QUERY.render(
                    project=self.project_id,
                    sandbox_dataset=self.sandbox_id,
                    sandbox_table=self.sandbox_table_name,
                    dataset=self.dataset_id)
        }

        update_query_dict = {
            cdr_consts.QUERY:
                UPDATE_FIX_UNMAPPED_SURVEY_ANSWERS_QUERY.render(
                    project=self.query_class.project_id,
                    sandbox_dataset=self.sandbox_id,
                    sandbox_table=self.sandbox_table_name,
                    dataset=self.dataset_id),
            cdr_consts.DESTINATION_TABLE:
                OBSERVATION,
            cdr_consts.DESTINATION_DATASET:
                self.dataset_id,
            cdr_consts.DISPOSITION:
                WRITE_TRUNCATE
        }

        self.assertEqual(results_list, [sandbox_query_dict, update_query_dict])
