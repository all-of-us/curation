"""
Unit test for cleaning_rules.null_invalid_foreign_keys.py module

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
        # Pre conditions
        person_foreign_keys = ['location_id', 'provider_id', 'care_site_id']

        # Post conditions
        self.assertEqual(self.rule_instance.get_foreign_keys(common.PERSON),
                         person_foreign_keys)

    def test_has_foreign_keys(self):
        self.assertFalse(self.rule_instance.has_foreign_key(common.CONCEPT))
        self.assertTrue(self.rule_instance.has_foreign_key(common.PERSON))

    def test_get_col_expression(self):
        # Pre conditions
        person_col_expression = [
            'person_id', 'gender_concept_id', 'year_of_birth', 'month_of_birth',
            'day_of_birth', 'birth_datetime', 'race_concept_id',
            'ethnicity_concept_id', 'loc.location_id', 'pro.provider_id',
            'car.care_site_id', 'person_source_value', 'gender_source_value',
            'gender_source_concept_id', 'race_source_value',
            'race_source_concept_id', 'ethnicity_source_value',
            'ethnicity_source_concept_id'
        ]

        expected_list = ', '.join(person_col_expression)

        # Post conditions
        self.assertEqual(self.rule_instance.get_col_expression(common.PERSON),
                         expected_list)

    def test_get_join_expression(self):
        # Pre conditions
        join_expression = []
        foreign_keys = ['location_id', 'care_site_id', 'provider_id']

        for key in foreign_keys:
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

    @patch.object(nifk.NullInvalidForeignKeys, 'get_affected_tables')
    def test_get_query_specs(self, mock_get_affected_tables):
        # Pre conditions
        cols = self.rule_instance.get_col_expression(common.PERSON)
        join_expression = self.rule_instance.get_join_expression(common.PERSON)

        mock_get_affected_tables.return_value = [common.PERSON]
        table = common.PERSON

        self.assertEqual(self.rule_instance.affected_tables,
                         resources.CDM_TABLES)

        invalid_foreign_key_query = {
            cdr_consts.QUERY:
                nifk.INVALID_FOREIGN_KEY_QUERY.render(
                    cols=cols,
                    table_name=table,
                    dataset_id=self.dataset_id,
                    project_id=self.project_id,
                    join_expr=join_expression),
            cdr_consts.DESTINATION_TABLE:
                table,
            cdr_consts.DESTINATION_DATASET:
                self.dataset_id,
            cdr_consts.DISPOSITION:
                bq_consts.WRITE_TRUNCATE
        }

        sandbox_query = {
            cdr_consts.QUERY:
                nifk.SANDBOX_QUERY.render(project_id=self.project_id,
                                          sandbox_dataset_id=self.sandbox_id,
                                          intermediary_table=self.rule_instance.
                                          get_sandbox_tablenames(),
                                          cols=cols,
                                          dataset_id=self.dataset_id,
                                          table_name=table,
                                          join_expr=join_expression),
            cdr_consts.DESTINATION_DATASET:
                self.dataset_id,
            cdr_consts.DESTINATION_TABLE:
                table,
            cdr_consts.DISPOSITION:
                bq_consts.WRITE_TRUNCATE
        }

        expected_list = [invalid_foreign_key_query] + [sandbox_query]

        # Test
        results_list = self.rule_instance.get_query_specs()

        # Post conditions
        self.assertEqual(results_list, expected_list)
