"""
Unit test for negative_ages.py

Age should not be negative for the person at any dates/start dates.
Using rule 20, 21 in Achilles Heel for reference.
Also ensure ages are not beyond 150.

Original Issues: DC-393, DC-811

"""

# Python imports
import unittest

# Project imports
import common
from constants import bq_utils as bq_consts
from constants.cdr_cleaner import clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.negative_ages import NegativeAges, date_fields, person, NEGATIVE_AGE_DEATH_QUERY, \
    NEGATIVE_AGES_QUERY, MAX_AGE, MAX_AGE_QUERY, SANDBOX_NEGATIVE_AND_MAX_AGE_QUERY, SANDBOX_NEGATIVE_AGE_DEATH_QUERY


class NegativeAgesTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'foo_project'
        self.dataset_id = 'foo_dataset'
        self.sandbox_dataset_id = 'foo_sandbox'
        self.client = None

        self.rule_instance = NegativeAges(self.project_id, self.dataset_id,
                                          self.sandbox_dataset_id)

        self.assertEqual(self.rule_instance.project_id, self.project_id)
        self.assertEqual(self.rule_instance.dataset_id, self.dataset_id)
        self.assertEqual(self.rule_instance.sandbox_dataset_id,
                         self.sandbox_dataset_id)

    def test_setup_rule(self):
        # Test
        self.rule_instance.setup_rule(self.client)

    def test_get_query_spec(self):
        # Pre conditions
        self.assertEqual(self.rule_instance.affected_datasets,
                         [cdr_consts.COMBINED])

        # Test
        result_list = self.rule_instance.get_query_specs()

        # Post conditions
        expected_sandbox_query_list = []
        expected_queries = []
        for table in date_fields:
            sandbox_query = dict()
            query_na = dict()
            query_ma = dict()
            person_table = person
            sandbox_query[
                cdr_consts.QUERY] = SANDBOX_NEGATIVE_AND_MAX_AGE_QUERY.render(
                    project_id=self.project_id,
                    dataset_id=self.dataset_id,
                    sandbox_id=self.sandbox_dataset_id,
                    intermediary_table=self.rule_instance.sandbox_table_for(
                        table),
                    table=table,
                    person_table=person_table,
                    table_date=date_fields[table],
                    MAX_AGE=MAX_AGE)
            expected_sandbox_query_list.append(sandbox_query)
            query_na[cdr_consts.QUERY] = NEGATIVE_AGES_QUERY.render(
                project_id=self.project_id,
                dataset_id=self.dataset_id,
                table=table,
                person_table=person_table,
                table_date=date_fields[table])
            query_na[cdr_consts.DESTINATION_TABLE] = table
            query_na[cdr_consts.DISPOSITION] = bq_consts.WRITE_TRUNCATE
            query_na[cdr_consts.DESTINATION_DATASET] = self.dataset_id
            query_ma[cdr_consts.QUERY] = MAX_AGE_QUERY.render(
                project_id=self.project_id,
                dataset_id=self.dataset_id,
                table=table,
                person_table=person_table,
                table_date=date_fields[table],
                MAX_AGE=MAX_AGE)
            query_ma[cdr_consts.DESTINATION_TABLE] = table
            query_ma[cdr_consts.DISPOSITION] = bq_consts.WRITE_TRUNCATE
            query_ma[cdr_consts.DESTINATION_DATASET] = self.dataset_id
            expected_queries.extend([query_na, query_ma])

        # query for death before birthdate
        death = common.DEATH
        sandbox_query = dict()
        query = dict()
        person_table = person
        sandbox_query[
            cdr_consts.QUERY] = SANDBOX_NEGATIVE_AGE_DEATH_QUERY.render(
                project_id=self.project_id,
                dataset_id=self.dataset_id,
                sandbox_id=self.sandbox_dataset_id,
                intermediary_tatble=self.rule_instance.sandbox_table_for(death),
                table=death,
                person_table=person_table)
        expected_sandbox_query_list.append(sandbox_query)
        query[cdr_consts.QUERY] = NEGATIVE_AGE_DEATH_QUERY.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            table=death,
            person_table=person_table)
        query[cdr_consts.DESTINATION_TABLE] = death
        query[cdr_consts.DISPOSITION] = bq_consts.WRITE_TRUNCATE
        query[cdr_consts.DESTINATION_DATASET] = self.dataset_id
        expected_queries.append(query)

        expected_list = [expected_sandbox_query_list, expected_queries]

        self.assertEqual(result_list, expected_list)
