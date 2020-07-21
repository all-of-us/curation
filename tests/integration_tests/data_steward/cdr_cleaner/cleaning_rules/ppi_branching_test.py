import datetime
import time
from typing import Any, Optional, Union, Tuple, Set

from google.cloud import bigquery
from google.cloud.bigquery import Table, TimePartitioning

import app_identity
import bq_utils
import sandbox
from cdr_cleaner.cleaning_rules.ppi_branching import PpiBranching
from cdr_cleaner.cleaning_rules import ppi_branching
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import \
    BaseTest
from utils import bq

TEST_DATA_FIELDS = ('observation_id', 'person_id', 'observation_source_value',
                    'value_source_value', 'value_as_string')
"""The columns associated with `TEST_DATA_ROWS`"""

TEST_DATA_ROWS = {
    (2000, 2000, 'Race_WhatRaceEthnicity',
     'WhatRaceEthnicity_RaceEthnicityNoneOfThese', None),
    (2001, 2000, 'RaceEthnicityNoneOfThese_RaceEthnicityFreeTextBox', None,
     'Mexican and Filipino'),
    (3000, 3000, 'Race_WhatRaceEthnicity', 'WhatRaceEthnicity_White', None),
    (3001, 3000, 'RaceEthnicityNoneOfThese_RaceEthnicityFreeTextBox',
     'PMI_Skip', None),
    (4000, 4000, 'OverallHealth_OrganTransplant', 'OrganTransplant_Yes', None),
    (4001, 4000, 'OrganTransplant_OrganTransplantDescription', None, 'Cornea'),
    (5000, 5000, 'OverallHealth_OrganTransplant', 'OrganTransplant_No', None),
    (5001, 5000, 'OrganTransplant_OrganTransplantDescription', 'PMI_Skip', None)
}
"""Set of tuples used to create rows in the observation table"""

TEST_DATA_DROP = {r for r in TEST_DATA_ROWS if r[0] in (3001, 5001)}
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
        cls.project_id = project_id
        cls.query_class = rule
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

    def test(self):
        rule = self.query_class  # var just to reduce line lengths

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

        # stage created
        q = f'''SELECT * FROM {_fq_table_name(rule.stage_table)}
                                ORDER BY observation_id'''
        rows, _ = self._query(q)
        self.assertSetEqual(TEST_DATA_KEEP, rows)

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

        # stage created
        q = f'''SELECT * FROM {_fq_table_name(rule.stage_table)}
                                        ORDER BY observation_id'''
        rows, _ = self._query(q)
        self.assertSetEqual(TEST_DATA_KEEP, rows)

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
