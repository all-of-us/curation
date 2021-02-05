import datetime
import time
from typing import Any, Optional, Union, Tuple, Set, Dict

from google.cloud import bigquery
from google.cloud.bigquery import Table, TimePartitioning

import app_identity
import bq_utils
from cdr_cleaner.cleaning_rules import ppi_branching
from cdr_cleaner.cleaning_rules.ppi_branching import PpiBranching
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import \
    BaseTest
from utils import bq, sandbox

TEST_DATA_FIELDS = ('observation_id', 'person_id', 'observation_source_value',
                    'value_as_number', 'value_source_value', 'value_as_string',
                    'questionnaire_response_id')
"""The columns associated with `TEST_DATA_ROWS`"""

TEST_DATA_ROWS = {
    (2000, 2000, 'Race_WhatRaceEthnicity', None,
     'WhatRaceEthnicity_RaceEthnicityNoneOfThese', None, 1),
    (2001, 2000, 'RaceEthnicityNoneOfThese_RaceEthnicityFreeTextBox', None,
     None, 'Mexican and Filipino', 1),
    (3000, 3000, 'Race_WhatRaceEthnicity', None, 'WhatRaceEthnicity_White',
     None, 2),
    (3001, 3000, 'RaceEthnicityNoneOfThese_RaceEthnicityFreeTextBox', None,
     'PMI_Skip', None, 2),
    (4000, 4000, 'OverallHealth_OrganTransplant', None, 'OrganTransplant_Yes',
     None, 3),
    (4001, 4000, 'OrganTransplant_OrganTransplantDescription', None, None,
     'Cornea', 3),
    (5000, 5000, 'OverallHealth_OrganTransplant', None, 'OrganTransplant_No',
     None, 4),
    (5001, 5000, 'OrganTransplant_OrganTransplantDescription', None, 'PMI_Skip',
     None, 4), (6000, 6000, 'eds_9', None, 'COPE_A_120', None, 5),
    (6001, 6000, 'eds_6', None, 'COPE_A_62', None, 5),
    (6002, 6000, 'eds_follow_up_1', None, 'PMI_Skip', None, 5),
    (7000, 7000, 'basics_xx', 2, None, None, 6),
    (7001, 7000, 'basics_xx20', None, 'PMI_Skip', None, 6),
    (8000, 8000, 'basics_xx', None, 'PMI_Skip', None, 7),
    (8001, 8000, 'basics_xx20', None, 'PMI_Skip', None, 7),
    (9000, 9000, 'basics_xx', 0.1, 'PMI_Skip', None, 7),
    (9001, 9000, 'basics_xx20', None, 'PMI_Skip', None, 7),
    (10000, 10000, 'basics_xx', 0.0, 'PMI_Skip', None, 8),
    (10001, 10000, 'basics_xx20', None, 'PMI_Skip', None, 8)
}
"""Set of tuples used to create rows in the observation table"""

TEST_DATA_DROP = {
    r for r in TEST_DATA_ROWS if r[0] in (3001, 5001, 8001, 10001)
}
"""Set of tuples in TEST_DATA_ROWS that should be removed after rule is run"""

TEST_DATA_KEEP = set(TEST_DATA_ROWS) - set(TEST_DATA_DROP)
"""Set of tuples in TEST_DATA_ROWS that should remain after rule is run"""


def _default_value_for(field: bigquery.SchemaField) -> Optional[Any]:
    """
    Get a default dummy value for a field. Used to create test observation rows more easily.

    :param field: the field
    :return: a value
    """
    if field.name.endswith('concept_id'):
        return 0
    if field.mode == 'required':
        if field.field_type == 'integer':
            return 0
        elif field.field_type == 'date':
            return datetime.datetime.today().strftime('%Y-%m-%d')
        elif field.field_type == 'timestamp':
            return time.time()
    return None


class Observation(object):
    """
    Helper class to initialize test observation rows
    """

    SCHEMA = bq.get_table_schema('observation')
    """List of schema fields for observation table"""

    _FIELD_DEFAULTS = dict(
        (field.name, _default_value_for(field)) for field in SCHEMA)
    """Maps field names to default values"""

    def __init__(self, **kwargs):
        # only permit observation fields as args
        for prop, val in kwargs.items():
            if prop not in Observation._FIELD_DEFAULTS.keys():
                raise ValueError(
                    f'Supplied key {prop} is not a field in the observation table'
                )
            self.__setattr__(prop, val)
        # unset args are set to a (dummy) default value
        for field_name, default_val in Observation._FIELD_DEFAULTS.items():
            if field_name not in kwargs.keys():
                self.__setattr__(field_name, default_val)


def _fq_table_name(table: Table) -> str:
    """
    Get fully qualified name of a table

    :param table: the table to get the name of
    :return: table name in the form `project.dataset.table_id`
    """
    return f'{table.project}.{table.dataset_id}.{table.table_id}'


def row_iter_to_set(row_iter: bigquery.table.RowIterator) -> Set[Tuple]:
    """
    Convert a row iterator to a set of tuples

    :param row_iter: open, unread results from a query
    :return: the results as a set of tuples
    """
    return {tuple(row[f] for f in TEST_DATA_FIELDS) for row in row_iter}


class PPiBranchingTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()
        project_id = app_identity.get_application_id()
        dataset_id = bq_utils.get_rdr_dataset_id()
        sandbox_dataset_id = sandbox.get_sandbox_dataset_id(dataset_id)
        rule = PpiBranching(project_id, dataset_id, sandbox_dataset_id)
        cls.dataset_id = dataset_id
        cls.sandbox_dataset_id = sandbox_dataset_id
        cls.project_id = project_id
        cls.rule_instance = rule
        cls.fq_sandbox_table_names = [
            _fq_table_name(table)
            for table in (rule.lookup_table, rule.backup_table)
        ]
        cls.fq_table_names = [_fq_table_name(rule.observation_table)]
        super().setUpClass()

    def setUp(self):
        self.data = [
            Observation(**dict(zip(TEST_DATA_FIELDS, row))).__dict__
            for row in TEST_DATA_ROWS
        ]
        self.client.delete_table(f'{self.dataset_id}.observation',
                                 not_found_ok=True)
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        # TODO figure out how to handle if clustering does NOT exist
        #      CREATE OR REPLACE fails if partitioning specs differ
        job_config.clustering_fields = ['person_id']
        job_config.time_partitioning = TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY)
        job_config.schema = Observation.SCHEMA
        job_config.create_disposition = bigquery.CreateDisposition.CREATE_IF_NEEDED
        self.client.load_table_from_json(
            self.data,
            destination=f'{self.dataset_id}.{ppi_branching.OBSERVATION}',
            job_config=job_config).result()

    def load_observation_table(self):
        """
        Drop existing and create observation table loaded with test data
        :return:
        """
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        job_config.schema = Observation.SCHEMA
        self.client.load_table_from_json(
            self.data,
            destination=f'{self.dataset_id}.{ppi_branching.OBSERVATION}',
            job_config=job_config).result()

    def _query(self, q: str) -> Tuple[Set[Tuple], bigquery.QueryJob]:
        """
        Execute query and return results

        :param q: the query
        :return: (rows, job) where results is a set of tuples
                 and job is the completed job
        """
        query_job = self.client.query(q)
        row_iter = query_job.result()
        return row_iter_to_set(row_iter), query_job

    def assert_job_success(self, job: Union[bigquery.QueryJob,
                                            bigquery.LoadJob]):
        """
        Check that job is done and does not have errors

        :param job: the job to check
        :return: None
        """
        self.assertEqual(job.state, 'DONE')
        self.assertIsNone(job.error_result)
        self.assertIsNone(job.errors)

    def get_dataset_table_map(self) -> Dict[str, Set[str]]:
        """
        Get set of tables currently in this test's datasets 

        :return: a mapping from dataset_id -> table_ids
        """
        dataset_cols_query = bq.dataset_columns_query(self.project_id,
                                                      self.dataset_id)
        sandbox_cols_query = bq.dataset_columns_query(self.project_id,
                                                      self.sandbox_dataset_id)
        cols_query = f"""
                {dataset_cols_query}
                UNION ALL
                {sandbox_cols_query}
                """
        cols = list(self.client.query(cols_query).result())
        dataset_tables = {
            dataset_id: set(col.table_name
                            for col in cols
                            if col.table_schema == dataset_id)
            for dataset_id in (self.dataset_id, self.sandbox_dataset_id)
        }
        return dataset_tables

    def post_execution_table_checks(self):
        """
        Check that tables were created or dropped from associated their 
        associated datasets as expected 
        """
        rule = self.rule_instance

        dataset_tables = self.get_dataset_table_map()

        # lookup, observation and backup tables should exist
        created_tables = [
            rule.lookup_table, rule.observation_table, rule.backup_table
        ]
        # staged table should have been removed
        dropped_tables = [rule.stage_table]

        for table in created_tables:
            self.assertIn(table.table_id, dataset_tables[table.dataset_id])

        for table in dropped_tables:
            self.assertNotIn(table.table_id, dataset_tables[table.dataset_id])

    def test(self):
        rule = self.rule_instance  # var just to reduce line lengths

        # setup_rule creates lookup
        rules_df = rule.create_rules_dataframe()
        rule.setup_rule(client=self.client)
        q = f'SELECT * FROM {rule.lookup_table.dataset_id}.{rule.lookup_table.table_id}'
        row_iter = self.client.query(q).result()
        self.assertEqual(len(rules_df.index), row_iter.total_rows)
        # if lookup exists it gets overwritten successfully
        lookup_job = rule.load_rules_lookup(client=self.client)
        self.assert_job_success(lookup_job)
        row_iter = self.client.query(q).result()
        self.assertEqual(len(rules_df.index), row_iter.total_rows)

        # subsequent tests rely on observation test data
        self.load_observation_table()

        # clean rows
        clean_table_script = rule.cleaning_script()
        _, drop_job = self._query(clean_table_script)
        self.assertIsInstance(drop_job, bigquery.QueryJob)
        self.assert_job_success(drop_job)

        self.post_execution_table_checks()

        # deleted rows are backed up
        q = f'''SELECT * FROM {_fq_table_name(rule.backup_table)}
                        ORDER BY observation_id'''
        rows, _ = self._query(q)
        self.assertSetEqual(TEST_DATA_DROP, rows)

        # source table is cleaned
        q = f'''SELECT * FROM {_fq_table_name(rule.observation_table)} 
                ORDER BY observation_id'''
        rows, _ = self._query(q)
        self.assertSetEqual(TEST_DATA_KEEP, rows)

        # repeated cleaning yields same output (no rows are backed up)
        _, drop_job = self._query(clean_table_script)
        self.assert_job_success(drop_job)

        self.post_execution_table_checks()

        # no rows are backed up this time
        q = f'''SELECT * FROM {_fq_table_name(rule.backup_table)}
                                ORDER BY observation_id'''
        rows, _ = self._query(q)
        self.assertEqual(0, len(rows))

        # source table is cleaned
        q = f'''SELECT * FROM {_fq_table_name(rule.observation_table)} 
                        ORDER BY observation_id'''
        rows, _ = self._query(q)
        self.assertSetEqual(TEST_DATA_KEEP, rows)
