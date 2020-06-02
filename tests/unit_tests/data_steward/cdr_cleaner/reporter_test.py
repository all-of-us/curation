import mock
import unittest
from cdr_cleaner import reporter
import constants.cdr_cleaner.reporter as consts


class CleanRulesReporterTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):

        self.query_dict = {
            #            cdr_consts.QUERY: 'query',
            #            cdr_consts.DESTINATION_DATASET: 'foo',
            #            cdr_consts.DESTINATION_TABLE: 'bar',
        }

    def test_check_field_list_validity(self):
        # preconditions
        fields = [k for k in consts.FIELDS_PROPERTIES_MAP.keys()]
        list_dictionary = [{'name': 'foo', 'module': 'bar'}]

        # test
        actual = reporter.check_field_list_validity(fields, list_dictionary)

        # post conditions
        expected = fields + ['name', 'module']
        self.assertEqual(set(actual), set(expected))

    def test_separate_sql_statements(self):
        # preconditions
        statement_dicts = [{
            'name':
                'foo',
            'sql': [{
                'query': 'query one'
            }, {
                'query': 'query two'
            }, {
                'query': 'query three'
            }]
        }]

        # test
        actual = reporter.separate_sql_statements(statement_dicts)

        # post conditions
        expected = [
            {
                'name': 'foo',
                'sql': 'query one'
            },
            {
                'name': 'foo',
                'sql': 'query two'
            },
            {
                'name': 'foo',
                'sql': 'query three'
            },
        ]
        self.assertEqual(actual, expected)

    def test_format_values(self):
        # preconditions
        statement_dicts = [{'name': 'foo', 'jira-issues': ['DC-000', 'DC-0']}]

        # test
        actual = reporter.format_values(statement_dicts)

        # post conditions
        expected = [{'name': 'foo', 'jira-issues': 'DC-000, DC-0'}]
        self.assertEqual(actual, expected)

    def test_main(self):
        pass

    def test_write_csv_report(self):
        pass

    def test_get_stage_elements(self):
        pass

    def test_parse_args(self):
        pass
