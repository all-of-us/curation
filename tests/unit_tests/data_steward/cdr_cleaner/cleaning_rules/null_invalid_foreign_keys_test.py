"""
Unit test for cleaning_rules.null_invalid_foreign_keys.py module

The intent of this unit test is to ensure that any invalid foreign keys are nulled out while
the remaining rows of the table are unchanged. A valid foreign key means that an existing
foreign key already exists in the table it references. An invalid foreign key means that there
is NOT an existing foreign key in the table it references
"""

# Python imports
import unittest

# Project imports
import resources
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

    def test_get_query_specs(self):
        # Pre conditions
        self.assertEqual(self.rule_instance.affected_tables,
                         resources.CDM_TABLES)

        queries_list = []
        sandbox_queries_list = []

        for table in self.rule_instance.affected_tables:
            field_names = [
                field['name'] for field in resources.fields_for(table)
            ]
            foreign_keys_flags = []
            fields_to_join = []

            for field_name in field_names:
                if field_name in nifk.FOREIGN_KEYS_FIELDS and field_name != table + '_id':
                    fields_to_join.append(field_name)
                    foreign_keys_flags.append(field_name)

            if fields_to_join:
                col_exprs = []
                for field in field_names:
                    if field in fields_to_join:
                        if field in foreign_keys_flags:
                            col_expr = '{x}.'.format(x=field[:3]) + field
                    else:
                        col_expr = field
                    col_exprs.append(col_expr)
                cols = ', '.join(col_exprs)

                join_expression = []
                for key in nifk.FOREIGN_KEYS_FIELDS:
                    if key in foreign_keys_flags:
                        if key == 'person_id':
                            table_alias = cdr_consts.PERSON_TABLE_NAME
                        else:
                            table_alias = self.rule_instance.get_mapping_tables(
                                '{x}'.format(x=key)[:-3])
                        join_expression.append(
                            nifk.LEFT_JOIN.render(dataset_id=self.dataset_id,
                                                  prefix=key[:3],
                                                  field=key,
                                                  table=table_alias))

                full_join_expression = " ".join(join_expression)

                invalid_foreign_key_query = {
                    cdr_consts.QUERY:
                        nifk.INVALID_FOREIGN_KEY_QUERY.render(
                            cols=cols,
                            table_name=table,
                            dataset_id=self.dataset_id,
                            project_id=self.project_id,
                            join_expr=full_join_expression),
                    cdr_consts.DESTINATION_TABLE:
                        table,
                    cdr_consts.DESTINATION_DATASET:
                        self.dataset_id,
                    cdr_consts.DISPOSITION:
                        bq_consts.WRITE_TRUNCATE
                }

                queries_list.append(invalid_foreign_key_query)

                sandbox_query = {
                    cdr_consts.QUERY:
                        nifk.SANDBOX_QUERY.render(
                            project_id=self.project_id,
                            sandbox_dataset_id=self.sandbox_id,
                            intermediary_table=self.rule_instance.
                            get_sandbox_tablenames(),
                            cols=cols,
                            dataset_id=self.dataset_id,
                            table_name=table,
                            join_expr=full_join_expression),
                    cdr_consts.DESTINATION_TABLE:
                        table,
                    cdr_consts.DESTINATION_DATASET:
                        self.dataset_id,
                    cdr_consts.DISPOSITION:
                        bq_consts.WRITE_TRUNCATE
                }

                sandbox_queries_list.append(sandbox_query)

        expected_list = queries_list + sandbox_queries_list

        # Test
        results_list = self.rule_instance.get_query_specs()

        # Post conditions
        self.assertEqual(results_list, expected_list)
