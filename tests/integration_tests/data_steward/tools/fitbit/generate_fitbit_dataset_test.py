from decimal import Decimal
from unittest import TestCase
from unittest.mock import patch
import os
from datetime import datetime

import app_identity
import tools.fitbit.generate_fitbit_dataset as gfd
from common import JINJA_ENV
from gcloud.bq import BigQueryClient

table_query = JINJA_ENV.from_string("""
CREATE TABLE {{project_id}}.{{dataset_id}}.{{table_id}}
 (person_id INT64, datetime DATETIME, steps NUMERIC, src_id STRING) AS
SELECT 1 as person_id, NULL as datetime, 10 as steps, 'ce' as src_id
UNION ALL
SELECT 2 as person_id, NULL as datetime, 20 as steps, 'ptsc' as src_id
UNION ALL
SELECT 3 as person_id, DATETIME('2021-01-01') as datetime, 30 as steps, 'ce' as src_id
UNION ALL
SELECT 4 as person_id, NULL as datetime, 40 as steps, 'ptsc' as src_id
""")

view_query = JINJA_ENV.from_string("""
CREATE VIEW {{project_id}}.{{dataset_id}}.{{view_id}} AS
SELECT *
FROM {{project_id}}.{{dataset_id}}.{{table_id}}
WHERE person_id > 2
""")

content_query = JINJA_ENV.from_string("""
SELECT *
FROM {{project_id}}.{{dataset_id}}.{{table_id}}
""")


class GenerateFitbitDatasetTest(TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = app_identity.get_application_id()
        self.dataset = os.environ.get('UNIONED_DATASET_ID')
        self.bq_client = BigQueryClient(self.project_id)
        self.table_id = 'fake'
        self.final_table = 'steps_intraday'
        self.view_id = f'view_{self.final_table}'
        self.test_tables = [self.table_id, self.view_id, self.final_table]

    @patch('tools.fitbit.generate_fitbit_dataset.FITBIT_TABLES',
           ['steps_intraday'])
    def test_copy_fitbit_tables(self):
        expected = [{
            'person_id': 3,
            'datetime': datetime.fromisoformat('2021-01-01'),
            'steps': Decimal(30),
            'src_id': 'ce',
        }, {
            'person_id': 4,
            'datetime': None,
            'steps': Decimal(40),
            'src_id': 'ptsc',
        }]
        create_table = table_query.render(project_id=self.project_id,
                                          dataset_id=self.dataset,
                                          table_id=self.table_id)
        table_job = self.bq_client.query(create_table)
        table_job.result()
        create_view = view_query.render(project_id=self.project_id,
                                        dataset_id=self.dataset,
                                        view_id=self.view_id,
                                        table_id=self.table_id)
        view_job = self.bq_client.query(create_view)
        view_job.result()
        gfd.copy_fitbit_tables_from_views(self.bq_client, self.dataset,
                                          self.dataset, 'view_')

        query_contents = content_query.render(project_id=self.project_id,
                                              dataset_id=self.dataset,
                                              table_id=self.final_table)
        content_job = self.bq_client.query(query_contents)
        contents = list(content_job.result())
        actual = [dict(row.items()) for row in contents]
        self.assertCountEqual(actual, expected)

    def tearDown(self):
        for table in self.test_tables:
            self.bq_client.delete_table(
                f'{self.project_id}.{self.dataset}.{table}', not_found_ok=True)
