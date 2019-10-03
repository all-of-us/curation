import unittest
import cdr_cleaner.cleaning_rules.generalize_concept_ids as generalize_concept_ids
from mock import mock
from cdr_cleaner.cleaning_rules.generalize_concept_ids import WOMAN_CONCEPT_ID
from cdr_cleaner.cleaning_rules.generalize_concept_ids import MAN_CONCEPT_ID
from cdr_cleaner.cleaning_rules.generalize_concept_ids import SEX_AT_BIRTH_MALE_CONCEPT_ID
from cdr_cleaner.cleaning_rules.generalize_concept_ids import SEX_AT_BIRTH_FEMALE_CONCEPT_ID
from cdr_cleaner.cleaning_rules.generalize_concept_ids import GENERALIZE_GENDER_CONCEPT_ID
import constants.cdr_cleaner.clean_cdr as cdr_consts


class GeneralizeConceptIdsTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'project_id'
        self.dataset_id = 'dataset_id'
        self.query_man_to_generalized = 'UPDATE gender identity man the generalized concept id'
        self.query_woman_to_generalized = 'UPDATE gender identity woman the generalized concept id'

    def test_parse_query_for_updating_woman_to_generalized_concept_id(self):
        generalize_birth_male_gender_identity_woman_query = generalize_concept_ids.parse_query_for_updating_woman_to_generalized_concept_id(
            self.project_id, self.dataset_id)

        expected = generalize_concept_ids.GENERALIZED_CONCEPT_ID_QUERY_TEMPLATE.format(project_id=self.project_id,
                                                                                       dataset_id=self.dataset_id,
                                                                                       gender_value_source_concept_id=WOMAN_CONCEPT_ID,
                                                                                       biological_sex_birth_concept_id=SEX_AT_BIRTH_MALE_CONCEPT_ID,
                                                                                       generalized_gender_concept_id=GENERALIZE_GENDER_CONCEPT_ID)

        self.assertItemsEqual(expected, generalize_birth_male_gender_identity_woman_query)

    def test_parse_query_for_updating_man_to_generalized_concept_id(self):
        generalize_birth_female_gender_identity_man_query = generalize_concept_ids.parse_query_for_updating_man_to_generalized_concept_id(
            self.project_id, self.dataset_id)

        expected = generalize_concept_ids.GENERALIZED_CONCEPT_ID_QUERY_TEMPLATE.format(project_id=self.project_id,
                                                                                       dataset_id=self.dataset_id,
                                                                                       gender_value_source_concept_id=MAN_CONCEPT_ID,
                                                                                       biological_sex_birth_concept_id=SEX_AT_BIRTH_FEMALE_CONCEPT_ID,
                                                                                       generalized_gender_concept_id=GENERALIZE_GENDER_CONCEPT_ID)

        self.assertItemsEqual(expected, generalize_birth_female_gender_identity_man_query)

    @mock.patch(
        'cdr_cleaner.cleaning_rules.generalize_concept_ids.parse_query_for_updating_man_to_generalized_concept_id')
    @mock.patch(
        'cdr_cleaner.cleaning_rules.generalize_concept_ids.parse_query_for_updating_woman_to_generalized_concept_id')
    def test_get_generalized_concept_id_queries(self, mock_parse_query_for_updating_woman_to_generalized_concept_id,
                                                mock_parse_query_for_updating_man_to_generalized_concept_id):
        mock_parse_query_for_updating_woman_to_generalized_concept_id.side_effect = [self.query_woman_to_generalized]
        mock_parse_query_for_updating_man_to_generalized_concept_id.side_effect = [self.query_man_to_generalized]

        actual = generalize_concept_ids.get_generalized_concept_id_queries(self.project_id, self.dataset_id)

        expected = []
        query_woman = dict()
        query_woman[cdr_consts.QUERY] = self.query_woman_to_generalized
        query_woman[cdr_consts.DESTINATION_DATASET] = self.dataset_id
        query_woman[cdr_consts.BATCH] = True

        query_man = dict()
        query_man[cdr_consts.QUERY] = self.query_man_to_generalized
        query_man[cdr_consts.DESTINATION_DATASET] = self.dataset_id
        query_man[cdr_consts.BATCH] = True

        expected.append(query_woman)
        expected.append(query_man)

        self.assertItemsEqual(expected, actual)
