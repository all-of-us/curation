"""Integration test for data violation in lower environment.
"""
import os

from admin.prod_pid_detection import check_violation, HEADER_CHECK_COMPLETED, HEADER_ID_VIOLATION_FOUND, HEADER_NO_ID_VIOLATION_FOUND, HEADER_SCHEDULED_QUERY_FAILED
from app_identity import PROJECT_ID
from common import (PERSON, JINJA_ENV)
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest

CREATE_LOOKUP_TMPL = JINJA_ENV.from_string("""
CREATE TABLE `{{project}}.{{dataset}}.id_violations_in_lower_envs`
(project_id STRING, dataset_id STRING, table_id STRING, num_violation INT64, violation_type STRING, monitor_date DATE)
""")

INSERT_LOOKUP_TMPL = JINJA_ENV.from_string("""
INSERT INTO `{{project}}.{{dataset}}.id_violations_in_lower_envs`
VALUES
('No violation found', 'No violation found', 'No violation found', 0, '-', DATE_ADD(CURRENT_DATE(), INTERVAL -2 DAY)),
('test-project', 'dataset_a', 'table_a', 100, 'person_id', DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY)),
{% for row in rows %}
('{{row['project_id']}}', '{{row['dataset_id']}}', '{{row['table_id']}}', {{row['num_violation']}}, 
 '{{row['violation_type']}}', {{row['monitor_date']}}){% if not loop.last -%}, {% endif %}
{% endfor %}
""")

VALUES_NO_VIOLATION = [{
    'project_id': 'No violation found',
    'dataset_id': 'No violation found',
    'table_id': 'No violation found',
    'num_violation': 0,
    'violation_type': '-',
    'monitor_date': 'CURRENT_DATE()'
}]

VALUES_NO_VIOLATION_DUPLICATE = [{
    'project_id': 'No violation found',
    'dataset_id': 'No violation found',
    'table_id': 'No violation found',
    'num_violation': 0,
    'violation_type': '-',
    'monitor_date': 'CURRENT_DATE()'
}, {
    'project_id': 'No violation found',
    'dataset_id': 'No violation found',
    'table_id': 'No violation found',
    'num_violation': 0,
    'violation_type': '-',
    'monitor_date': 'CURRENT_DATE()'
}]

VALUES_NO_CHECK_RUN = [{
    'project_id': 'No violation found',
    'dataset_id': 'No violation found',
    'table_id': 'No violation found',
    'num_violation': 0,
    'violation_type': '-',
    'monitor_date': 'DATE_ADD(CURRENT_DATE(), INTERVAL -3 DAY)'
}]

VALUES_VIOLATION_FOUND = [{
    'project_id': 'project_test_a',
    'dataset_id': 'dataset_a',
    'table_id': 'table_a',
    'num_violation': 10,
    'violation_type': 'person_id',
    'monitor_date': 'CURRENT_DATE()'
}]

VALUES_MULTIPLE_VIOLATION_FOUND = [{
    'project_id': 'project_test_a',
    'dataset_id': 'dataset_a',
    'table_id': 'table_a',
    'num_violation': 10,
    'violation_type': 'person_id',
    'monitor_date': 'CURRENT_DATE()'
}, {
    'project_id': 'project_test_b',
    'dataset_id': 'dataset_b',
    'table_id': 'table_b',
    'num_violation': 10,
    'violation_type': 'research_id',
    'monitor_date': 'CURRENT_DATE()'
}, {
    'project_id': 'No violation found',
    'dataset_id': 'No violation found',
    'table_id': 'No violation found',
    'num_violation': 0,
    'violation_type': '-',
    'monitor_date': 'CURRENT_DATE()'
}]


class ProdPidDetectionTest(BaseTest.BigQueryTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()
        cls.project_id = os.environ.get(PROJECT_ID)
        cls.sandbox_id = os.environ.get('BIGQUERY_DATASET_ID')
        cls.fq_sandbox_table_names = [
            f'{cls.project_id}.{cls.sandbox_id}.id_violations_in_lower_envs'
        ]
        # fq_table_names cannot be empty, so putting person table as a placeholder.
        cls.fq_table_names = [f'{cls.project_id}.{cls.sandbox_id}.{PERSON}']

        super().setUpClass()

    def setUp(self):
        super().setUp()
        create_lookup_table = CREATE_LOOKUP_TMPL.render(project=self.project_id,
                                                        dataset=self.sandbox_id)
        self.load_test_data([create_lookup_table])

    def test_no_violation(self):
        """When id_violations_in_lower_envs has only 'No violation found' record,
        it does not throw an error message.
        """
        insert_lookup_table = INSERT_LOOKUP_TMPL.render(
            project=self.project_id,
            dataset=self.sandbox_id,
            rows=VALUES_NO_VIOLATION)
        self.load_test_data([insert_lookup_table])

        with self.assertLogs(level='INFO') as cm:
            check_violation(self.project_id, self.sandbox_id)

        self.assertTrue(
            any(HEADER_NO_ID_VIOLATION_FOUND in msg for msg in cm.output))
        self.assertTrue(any(HEADER_CHECK_COMPLETED in msg for msg in cm.output))

    def test_no_violation_duplicate(self):
        """When id_violations_in_lower_envs multiple 'No violation found' records,
        it does not throw an error message.
        """
        insert_lookup_table = INSERT_LOOKUP_TMPL.render(
            project=self.project_id,
            dataset=self.sandbox_id,
            rows=VALUES_NO_VIOLATION_DUPLICATE)
        self.load_test_data([insert_lookup_table])

        with self.assertLogs(level='INFO') as cm:
            check_violation(self.project_id, self.sandbox_id)

        self.assertTrue(
            any(HEADER_NO_ID_VIOLATION_FOUND in msg for msg in cm.output))
        self.assertTrue(any(HEADER_CHECK_COMPLETED in msg for msg in cm.output))

    def test_no_check_run(self):
        """When id_violations_in_lower_envs does not have a record for today,
        it throws an error message.
        """
        insert_lookup_table = INSERT_LOOKUP_TMPL.render(
            project=self.project_id,
            dataset=self.sandbox_id,
            rows=VALUES_NO_CHECK_RUN)
        self.load_test_data([insert_lookup_table])

        with self.assertLogs() as cm:
            check_violation(self.project_id, self.sandbox_id)

        self.assertTrue(
            any(HEADER_SCHEDULED_QUERY_FAILED in msg for msg in cm.output))
        self.assertTrue(any(HEADER_CHECK_COMPLETED in msg for msg in cm.output))

    def test_violation_found(self):
        """When id_violations_in_lower_envs have a violation record for today,
        it throws an error message.
        """
        insert_lookup_table = INSERT_LOOKUP_TMPL.render(
            project=self.project_id,
            dataset=self.sandbox_id,
            rows=VALUES_VIOLATION_FOUND)
        self.load_test_data([insert_lookup_table])

        with self.assertLogs() as cm:
            check_violation(self.project_id, self.sandbox_id)

        self.assertTrue(
            any(HEADER_ID_VIOLATION_FOUND in msg for msg in cm.output))
        self.assertTrue(any(HEADER_CHECK_COMPLETED in msg for msg in cm.output))

    def test_multiple_violation_found(self):
        """When id_violations_in_lower_envs have multiple violation records for today,
        it throws an error message, even when one of them is 'No violation found'.
        """
        insert_lookup_table = INSERT_LOOKUP_TMPL.render(
            project=self.project_id,
            dataset=self.sandbox_id,
            rows=VALUES_MULTIPLE_VIOLATION_FOUND)
        self.load_test_data([insert_lookup_table])

        with self.assertLogs() as cm:
            check_violation(self.project_id, self.sandbox_id)

        self.assertTrue(
            any(HEADER_ID_VIOLATION_FOUND in msg for msg in cm.output))
        self.assertTrue(any(HEADER_CHECK_COMPLETED in msg for msg in cm.output))
