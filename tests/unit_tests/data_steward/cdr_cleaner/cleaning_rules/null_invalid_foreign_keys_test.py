"""
Unit test for cleaning_rules.null_invalid_foreign_keys.py module

Original Issues: DC-807, DC-388

The intent of this unit test is to ensure that any invalid foreign keys are nulled out while
the remaining rows of the table are unchanged. A valid foreign key means that an existing
foreign key already exists in the table it references. An invalid foreign key means that there
is NOT an existing foreign key in the table it references
"""

# Python imports
import unittest
from mock import patch

# Project imports
import resources
import common
from constants import bq_utils as bq_consts
from constants.cdr_cleaner import clean_cdr as cdr_consts
import cdr_cleaner.cleaning_rules.null_invalid_foreign_keys as nifk


class NullInvalidForeignKeys(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'foo_project'
        self.dataset_id = 'bar_dataset'
        self.sandbox_id = 'baz_sandbox'
        self.client = None

        self.person_foreign_keys = [
            'location_id', 'provider_id', 'care_site_id'
        ]
        self.person_col_expression = "person_id, gender_concept_id, year_of_birth, month_of_birth, " \
                                     "day_of_birth, birth_datetime, race_concept_id, ethnicity_concept_id, " \
                                     "loc.location_id, pro.provider_id, car.care_site_id, " \
                                     "person_source_value, gender_source_value, gender_source_concept_id, " \
                                     "race_source_value, race_source_concept_id, ethnicity_source_value, " \
                                     "ethnicity_source_concept_id"

        self.join_expression = """
            LEFT JOIN `bar_dataset._mapping_location` loc
            ON t.location_id = loc.location_id
            LEFT JOIN `bar_dataset._mapping_care_site` car
            ON t.care_site_id = car.care_site_id
            LEFT JOIN `bar_dataset._mapping_provider` pro
            ON t.provider_id = pro.provider_id
            """
        self.sandbox_expression = """
            (location_id NOT IN (
            SELECT location_id
            FROM `bar_dataset._mapping_location` AS loc)
            AND location_id IS NOT NULL) OR
            (provider_id NOT IN (
            SELECT provider_id
            FROM `bar_dataset._mapping_provider` AS pro)
            AND provider_id IS NOT NULL) OR
            (care_site_id NOT IN (
            SELECT care_site_id
            FROM `bar_dataset._mapping_care_site` AS car)
            AND care_site_id IS NOT NULL)
        """

        self.rule_instance = nifk.NullInvalidForeignKeys(
            self.project_id, self.dataset_id, self.sandbox_id)

        self.assertEqual(self.rule_instance.project_id, self.project_id)
        self.assertEqual(self.rule_instance.dataset_id, self.dataset_id)
        self.assertEqual(self.rule_instance.sandbox_dataset_id, self.sandbox_id)

    def test_setup_rule(self):
        # Test
        self.rule_instance.setup_rule(self.client)

        # No errors are raised, nothing will happen

    def test_get_field_names(self):
        # Pre conditions
        person_fields = [
            'person_id', 'gender_concept_id', 'year_of_birth', 'month_of_birth',
            'day_of_birth', 'birth_datetime', 'race_concept_id',
            'ethnicity_concept_id', 'location_id', 'provider_id',
            'care_site_id', 'person_source_value', 'gender_source_value',
            'gender_source_concept_id', 'race_source_value',
            'race_source_concept_id', 'ethnicity_source_value',
            'ethnicity_source_concept_id'
        ]

        # Post conditions
        self.assertEqual(self.rule_instance.get_field_names(common.PERSON),
                         person_fields)

    def test_get_foreign_keys(self):
        # Post conditions
        self.assertEqual(self.rule_instance.get_foreign_keys(common.PERSON),
                         self.person_foreign_keys)

    def test_has_foreign_keys(self):
        self.assertFalse(self.rule_instance.has_foreign_key(common.CONCEPT))
        self.assertTrue(self.rule_instance.has_foreign_key(common.PERSON))

    def test_get_col_expression(self):
        # Pre conditions
        expected_list = self.person_col_expression

        # Post conditions
        self.assertEqual(self.rule_instance.get_col_expression(common.PERSON),
                         expected_list)

    def test_get_join_expression(self):
        # Pre conditions
        join_expression = []

        for key in self.person_foreign_keys:
            table_alias = self.rule_instance.get_mapping_table(
                '{x}'.format(x=key)[:-3])
            join_expression.append(
                nifk.LEFT_JOIN.render(dataset_id=self.dataset_id,
                                      prefix=key[:3],
                                      field=key,
                                      table=table_alias))

        expected_query = ' '.join(join_expression)

        # Test
        actual_query = self.rule_instance.get_join_expression(common.PERSON)

        # Post conditions
        self.assertEqual(actual_query, expected_query)

    def test_get_sandbox_expression(self):
        # Pre conditions
        sandbox_expression = []

        for key in self.person_foreign_keys:
            table_alias = self.rule_instance.get_mapping_table(
                '{x}'.format(x=key)[:-3])
            sandbox_expression.append(
                nifk.SANDBOX_EXPRESSION.render(field=key,
                                               dataset_id=self.dataset_id,
                                               table=table_alias,
                                               prefix=key[:3]))
        expected_query = ' OR '.join(sandbox_expression)

        # Test
        actual_query = self.rule_instance.get_sandbox_expression(common.PERSON)

        # Post conditions
        self.assertEqual(actual_query, expected_query)

    @patch.object(nifk.NullInvalidForeignKeys, 'get_sandbox_expression')
    @patch.object(nifk.NullInvalidForeignKeys, 'get_join_expression')
    @patch.object(nifk.NullInvalidForeignKeys, 'get_col_expression')
    @patch.object(nifk.NullInvalidForeignKeys, 'has_foreign_key')
    @patch.object(nifk.NullInvalidForeignKeys, 'get_affected_tables')
    def test_get_query_specs(self, mock_get_affected_tables,
                             mock_has_foreign_keys, mock_get_col_expression,
                             mock_get_join_expression,
                             mock_get_sandbox_expression):
        # Pre conditions
        mock_has_foreign_keys.return_value = True
        mock_get_col_expression.return_value = self.person_col_expression
        mock_get_join_expression.return_value = self.join_expression
        mock_get_sandbox_expression.return_value = self.sandbox_expression
        mock_get_affected_tables.return_value = [common.PERSON]

        table = common.PERSON

        sandbox_query = {
            cdr_consts.QUERY:
                nifk.SANDBOX_QUERY.render(
                    project_id=self.project_id,
                    sandbox_dataset_id=self.sandbox_id,
                    intermediary_table=self.rule_instance.sandbox_table_for(
                        table),
                    dataset_id=self.dataset_id,
                    table_name=table,
                    sandbox_expr=self.sandbox_expression),
        }

        invalid_foreign_key_query = {
            cdr_consts.QUERY:
                nifk.INVALID_FOREIGN_KEY_QUERY.render(
                    cols=self.person_col_expression,
                    table_name=table,
                    dataset_id=self.dataset_id,
                    project_id=self.project_id,
                    join_expr=self.join_expression),
            cdr_consts.DESTINATION_TABLE:
                table,
            cdr_consts.DESTINATION_DATASET:
                self.dataset_id,
            cdr_consts.DISPOSITION:
                bq_consts.WRITE_TRUNCATE
        }

        expected_list = [sandbox_query] + [invalid_foreign_key_query]

        # Test
        results_list = self.rule_instance.get_query_specs()

        # Post conditions
        self.assertEqual(self.rule_instance.affected_tables,
                         resources.CDM_TABLES + [common.AOU_DEATH])
        self.assertEqual(results_list, expected_list)
