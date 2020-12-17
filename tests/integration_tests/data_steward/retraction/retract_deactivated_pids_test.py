# Python imports
import os
import unittest
import logging
from io import open

# Third party imports
import pandas as pd
import mock
import google.cloud.bigquery as gbq

# Project Imports
import app_identity
import bq_utils
from utils import bq
import gcs_utils
from tests import test_util
from retraction import retract_deactivated_pids as rdp
from constants.cdr_cleaner import clean_cdr as clean_consts
import sandbox as sb
from common import JINJA_ENV

TABLE_ROWS_QUERY = 'SELECT * FROM {dataset_id}.__TABLES__ '

EXPECTED_DROPPED_ROWS_QUERY = ('SELECT * '
                               'FROM {dataset_id}.{table_id} '
                               'WHERE person_id = {pid} '
                               'AND {date_column} >= (SELECT deactivated_date '
                               'FROM {dataset_id}.{pid_table_id} '
                               'WHERE person_id = {pid})')
EXPECTED_KEPT_ROWS_QUERY = ('SELECT * '
                            'FROM {dataset_id}.{table_id} '
                            'WHERE person_id != {pid} '
                            'OR(  person_id = {pid} '
                            'AND {date_column} < (SELECT deactivated_date '
                            'FROM {dataset_id}.{pid_table_id} '
                            'WHERE person_id = {pid}))')
EXPECTED_DROPPED_ROWS_QUERY_END_DATE = (
    'SELECT * '
    'FROM {dataset_id}.{table_id} '
    'WHERE person_id = {pid} '
    'AND (CASE WHEN {end_date_column} IS NOT NULL THEN {end_date_column} >= '
    '(SELECT deactivated_date '
    'FROM `{dataset_id}.{pid_table_id}` '
    'WHERE person_id = {pid} ) ELSE CASE WHEN {end_date_column} IS NULL THEN '
    '{start_date_column} >= ( '
    'SELECT deactivated_date '
    'FROM `{dataset_id}.{pid_table_id}` '
    'WHERE person_id = {pid}) END END)')
EXPECTED_KEPT_ROWS_QUERY_END_DATE = (
    'SELECT * '
    'FROM {dataset_id}.{table_id} '
    'WHERE person_id != {pid} '
    'OR (person_id = {pid} '
    'AND (CASE WHEN {end_date_column} IS NOT NULL THEN {end_date_column} < '
    '(SELECT deactivated_date '
    'FROM `{dataset_id}.{pid_table_id}` '
    'WHERE person_id = {pid} ) ELSE CASE WHEN {end_date_column} IS NULL THEN '
    '{start_date_column} < ( '
    'SELECT deactivated_date '
    'FROM `{dataset_id}.{pid_table_id}` '
    'WHERE person_id = {pid}) END END))')
INSERT_PID_TABLE = ('INSERT INTO {dataset_id}.{pid_table_id} '
                    '(person_id, deactivated_date) '
                    'VALUES{person_research_ids}')

DEACTIVATED_PIDS_SCHEMA = [{
    "type": "integer",
    "name": "person_id",
    "mode": "required",
    "description": ""
}, {
    "type": "date",
    "name": "deactivated_date",
    "mode": "nullable",
    "description": ""
}]

DEACTIVATED_PIDS = JINJA_ENV.from_string("""
INSERT INTO `{{deact_table.project}}.{{deact_table.dataset_id}}.{{deact_table.table_id}}` 
(person_id, deactivated_date) 
VALUES
(1,'2009-07-25'),
(2,'2009-03-14'),
(3,'2009-11-18'),
(4,'2009-11-25'),
(5,'2009-09-20')
""")

TABLE_ROWS = {
    'person':
        JINJA_ENV.from_string("""
INSERT INTO `{{table.project}}.{{table.dataset_id}}.{{table.table_id}}`
(person_id, gender_concept_id, year_of_birth, birth_datetime, race_concept_id, ethnicity_concept_id)
VALUES
(1,8507,1989,'1989-07-25 01:00:00 UTC', 8527, 38003563),
(2,8507,1975,'1975-03-14 02:00:00 UTC', 8527, 38003564),
(3,8507,1981,'1981-11-18 05:00:00 UTC', 8527, 38003564),
(4,8507,1991,'1991-11-25 08:00:00 UTC', 8527, 38003564),
(5,8507,2001,'2001-09-20 11:00:00 UTC', 8527, 38003564)
"""),
    'observation':
        JINJA_ENV.from_string("""
INSERT INTO `{{table.project}}.{{table.dataset_id}}.{{table.table_id}}`
(observation_id, person_id, observation_concept_id, observation_date, observation_datetime, observation_type_concept_id)
VALUES
(1001,1,0,'2008-07-25','2008-07-25 01:00:00 UTC',45905771),
(1005,2,0,'2008-03-14','2008-03-14 02:00:00 UTC',45905771),
(1002,3,0,'2009-11-18','2009-11-18 05:00:00 UTC',45905771),
(1004,4,0,'2009-11-25','2009-11-25 08:00:00 UTC',45905771),
(1003,5,0,'2010-09-20','2010-09-20 11:00:00 UTC',45905771)
"""),
    'death':
        JINJA_ENV.from_string("""
INSERT INTO `{{table.project}}.{{table.dataset_id}}.{{table.table_id}}`
(person_id, death_date, death_datetime, death_type_concept_id)
VALUES
(2,'2008-03-12','2008-03-12 05:00:00 UTC',8),
(3,'2011-01-18','2011-01-18 05:00:00 UTC',6)
"""),
    'drug_exposure':
        JINJA_ENV.from_string("""
INSERT INTO `{{table.project}}.{{table.dataset_id}}.{{table.table_id}}`
(drug_exposure_id, person_id, drug_concept_id, drug_exposure_start_date, drug_exposure_start_datetime,
drug_exposure_end_date, drug_exposure_end_datetime, verbatim_end_date, drug_type_concept_id)
VALUES
(2002,1,50,'2008-06-05','2008-06-05 01:00:00 UTC','2010-07-05','2008-06-05 01:00:00 UTC','2011-04-11',87),
(2003,2,21,'2008-11-22','2008-11-22 02:00:00 UTC',null,null,'2010-06-18',51),
(2004,3,5241,'2009-08-03','2009-08-03 05:00:00 UTC',null,null,'2009-12-26',2754),
(2005,4,76536,'2010-02-17','2010-02-17 08:00:00 UTC',null,null,'2008-03-04',24),
(2006,5,274,'2009-04-19','2009-04-19 11:00:00 UTC',null,'2010-11-19 01:00:00 UTC','2011-10-22',436)
""")
}

MAPPING_TABLE_ROWS = {
    '_mapping_observation':
        JINJA_ENV.from_string("""
INSERT INTO `{{table.project}}.{{table.dataset_id}}.{{table.table_id}}`
(observation_id, src_hpo_id)
VALUES
(1001,'hpo_1'),
(1002,'hpo_2'),
(1003,'hpo_3'),
(1004,'PPI/PM'),
(1005,'hpo_4'),
(1006,'hpo_4')
""")
}

EXT_TABLE_ROWS = {
    'drug_exposure_ext':
        JINJA_ENV.from_string("""
INSERT INTO `{{table.project}}.{{table.dataset_id}}.{{table.table_id}}`
(drug_exposure_id, src_id)
VALUES
(2002,'PPI/PM'),
(2003,'EHR Site 50'),
(2004,'EHR Site 22'),
(2005,'EHR Site 9'),
(2006,'EHR Site 17')
""")
}


class RetractDeactivatedEHRDataBqTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.hpo_id = 'fake'
        self.project_id = app_identity.get_application_id()
        if 'test' not in self.project_id:
            raise RuntimeError(
                f"Make sure the project_id is set to test. Project_id is {self.project_id}"
            )
        self.bq_dataset_id = os.environ.get('UNIONED_DATASET_ID')
        self.dataset_id = os.environ.get('COMBINED_DATASET_ID')
        self.client = bq.get_client(self.project_id)
        self.bq_sandbox_dataset_id = sb.get_sandbox_dataset_id(
            self.bq_dataset_id)
        self.ticket_number = 'DCXXX'
        self.pid_table_id = 'pid_table'
        self.pid_table_id_list = [
            self.project_id + '.' + self.bq_dataset_id + '.' + 'pid_table'
        ]
        self.deactivated_ehr_participants = [(1, '2010-01-01'),
                                             (2, '2010-01-01'),
                                             (5, '2010-01-01')]
        test_util.delete_all_tables(self.bq_dataset_id)
        test_util.delete_all_tables(self.dataset_id)
        # test_util.delete_all_tables(self.bq_sandbox_dataset_id)
        self.setup_data()

    def setup_data(self):
        # setup deactivated participants table
        self.deact_table = f'{self.project_id}.{self.dataset_id}.deactivated_participants'
        deact_table_ref = gbq.TableReference.from_string(self.deact_table)
        bq.create_tables(self.client,
                         self.project_id, [self.deact_table],
                         exists_ok=True,
                         fields=[DEACTIVATED_PIDS_SCHEMA])
        job_config = gbq.QueryJobConfig()
        job = self.client.query(
            DEACTIVATED_PIDS.render(deact_table=deact_table_ref), job_config)
        job.result()

        # create omop tables and mapping/ext tables
        tables = {**TABLE_ROWS, **MAPPING_TABLE_ROWS, **EXT_TABLE_ROWS}
        for table in tables:
            fq_table = f'{self.project_id}.{self.bq_dataset_id}.{table}'
            bq.create_tables(self.client,
                             self.project_id, [fq_table],
                             exists_ok=True)
            table_ref = gbq.TableReference.from_string(fq_table)
            job_config = gbq.QueryJobConfig()
            job = self.client.query(tables[table].render(table=table_ref),
                                    job_config)
            job.result()

    @mock.patch(
        'retraction.retract_deactivated_pids.get_date_info_for_pids_tables')
    def test_integration_queries_to_retract_from_fake_dataset(
        self, mock_retraction_info):
        d = {
            'project_id': [
                self.project_id, self.project_id, self.project_id,
                self.project_id, self.project_id, self.project_id
            ],
            'dataset_id': [
                self.bq_dataset_id, self.bq_dataset_id, self.bq_dataset_id,
                self.bq_dataset_id, self.bq_dataset_id, self.bq_dataset_id
            ],
            'table': [
                'fake_condition_occurrence', 'fake_drug_exposure',
                'fake_measurement', 'fake_observation',
                'fake_procedure_occurrence', 'fake_visit_occurrence'
            ],
            'date_column': [
                None, None, 'measurement_date', 'observation_date',
                'procedure_date', None
            ],
            'start_date_column': [
                'condition_start_date', 'drug_exposure_start_date', None, None,
                None, 'visit_start_date'
            ],
            'end_date_column': [
                'condition_end_date', 'drug_exposure_end_date', None, None,
                None, 'visit_end_date'
            ]
        }
        retraction_info = pd.DataFrame(data=d)
        mock_retraction_info.return_value = retraction_info

        # Create and load person_ids and deactivated_date to pid table
        bq.create_tables(self.client,
                         self.project_id,
                         self.pid_table_id_list,
                         exists_ok=False,
                         fields=rdp.PID_TABLE_FIELDS)
        bq_formatted_insert_values = ', '.join([
            '(%s, "%s")' % (person_id, deactivated_date)
            for (person_id,
                 deactivated_date) in self.deactivated_ehr_participants
        ])
        q = INSERT_PID_TABLE.format(
            dataset_id=self.bq_dataset_id,
            pid_table_id=self.pid_table_id,
            person_research_ids=bq_formatted_insert_values)
        self.client.query(q)

        job_ids = []
        dropped_row_count_queries = []
        kept_row_count_queries = []
        hpo_table_list = []

        # Load the cdm files into dataset
        for cdm_file in test_util.NYC_FIVE_PERSONS_FILES:
            cdm_file_name = os.path.basename(cdm_file)
            cdm_table = cdm_file_name.split('.')[0]
            hpo_table = bq_utils.get_table_id(self.hpo_id, cdm_table)
            # Do not process if person table
            if hpo_table == 'fake_person':
                continue
            hpo_table_list.append(hpo_table)
            logging.info(
                f'Preparing to load table {self.bq_dataset_id}.{hpo_table}')
            with open(cdm_file, 'rb') as f:
                gcs_utils.upload_object(gcs_utils.get_hpo_bucket(self.hpo_id),
                                        cdm_file_name, f)
            result = bq_utils.load_cdm_csv(self.hpo_id,
                                           cdm_table,
                                           dataset_id=self.bq_dataset_id)
            logging.info(f'Loading table {self.bq_dataset_id}.{hpo_table}')
            job_id = result['jobReference']['jobId']
            job_ids.append(job_id)

        incomplete_jobs = bq_utils.wait_on_jobs(job_ids)
        self.assertEqual(len(incomplete_jobs), 0,
                         'NYC five person load job did not complete')
        logging.info('All tables loaded successfully')

        # Store query for checking number of rows to delete
        for ehr in self.deactivated_ehr_participants:
            pid = ehr[0]
            for row in retraction_info.itertuples(index=False):
                if row.date_column is None:
                    dropped_query = EXPECTED_DROPPED_ROWS_QUERY_END_DATE.format(
                        dataset_id=self.bq_dataset_id,
                        table_id=row.table,
                        pid_table_id=self.pid_table_id,
                        pid=pid,
                        start_date_column=row.start_date_column,
                        end_date_column=row.end_date_column)
                    kept_query = EXPECTED_KEPT_ROWS_QUERY_END_DATE.format(
                        dataset_id=self.bq_dataset_id,
                        table_id=row.table,
                        pid_table_id=self.pid_table_id,
                        pid=pid,
                        start_date_column=row.start_date_column,
                        end_date_column=row.end_date_column)
                else:
                    dropped_query = EXPECTED_DROPPED_ROWS_QUERY.format(
                        dataset_id=self.bq_dataset_id,
                        table_id=row.table,
                        pid_table_id=self.pid_table_id,
                        pid=pid,
                        date_column=row.date_column)
                    kept_query = EXPECTED_KEPT_ROWS_QUERY.format(
                        dataset_id=self.bq_dataset_id,
                        table_id=row.table,
                        pid_table_id=self.pid_table_id,
                        pid=pid,
                        date_column=row.date_column)
                dropped_row_count_queries.append({
                    clean_consts.QUERY: dropped_query,
                    clean_consts.DESTINATION_DATASET: self.bq_dataset_id,
                    clean_consts.DESTINATION_TABLE: row.table
                })
                kept_row_count_queries.append({
                    clean_consts.QUERY: kept_query,
                    clean_consts.DESTINATION_DATASET: self.bq_dataset_id,
                    clean_consts.DESTINATION_TABLE: row.table
                })

        # Use query results to count number of expected dropped row deletions
        expected_dropped_row_count = {}
        for query_dict in dropped_row_count_queries:
            response = self.client.query(query_dict['query'])
            result = response.result()
            if query_dict['destination_table_id'] in expected_dropped_row_count:
                expected_dropped_row_count[
                    query_dict['destination_table_id']] += result.total_rows
            else:
                expected_dropped_row_count[
                    query_dict['destination_table_id']] = result.total_rows

        # Separate check to find number of actual deleted rows
        q = TABLE_ROWS_QUERY.format(dataset_id=self.bq_dataset_id)
        q_result = self.client.query(q)
        row_count_before_retraction = {}
        for row in q_result:
            row_count_before_retraction[row['table_id']] = row['row_count']

        # Use query results to count number of expected dropped row deletions
        expected_kept_row_count = {}
        for query_dict in kept_row_count_queries:
            response = self.client.query(query_dict['query'])
            result = response.result()
            if query_dict['destination_table_id'] in expected_kept_row_count:
                expected_kept_row_count[query_dict['destination_table_id']] -= (
                    (row_count_before_retraction[
                        query_dict['destination_table_id']] -
                     result.total_rows))
            else:
                expected_kept_row_count[query_dict['destination_table_id']] = (
                    row_count_before_retraction[
                        query_dict['destination_table_id']] -
                    (row_count_before_retraction[
                        query_dict['destination_table_id']] -
                     result.total_rows))

        # Perform retraction
        query_list = rdp.create_queries(self.project_id, self.ticket_number,
                                        self.project_id, self.bq_dataset_id,
                                        self.pid_table_id)
        rdp.run_queries(query_list, self.client)

        # Find actual deleted rows
        q_result = self.client.query(q)
        results = q_result.result()
        row_count_after_retraction = {}
        for row in results:
            row_count_after_retraction[row['table_id']] = row['row_count']

        for table in expected_dropped_row_count:
            self.assertEqual(
                expected_dropped_row_count[table],
                row_count_before_retraction[table] -
                row_count_after_retraction[table])

        for table in expected_kept_row_count:
            self.assertEqual(expected_kept_row_count[table],
                             row_count_after_retraction[table])

    def test_queries_to_retract_from_fake_dataset(self):
        rdp.run_deactivation(self.client, self.project_id, [self.bq_dataset_id],
                             self.deact_table)
        person_cols = [
            'person_id', 'gender_concept_id', 'year_of_birth', 'birth_datetime',
            'race_concept_id', 'ethnicity_concept_id'
        ]
        person_data = [
            (1, 8507, 1989, '1989-07-25 01:00:00 UTC', 8527, 38003563),
            (2, 8507, 1975, '1975-03-14 02:00:00 UTC', 8527, 38003564),
            (3, 8507, 1981, '1981-11-18 05:00:00 UTC', 8527, 38003564),
            (4, 8507, 1991, '1991-11-25 08:00:00 UTC', 8527, 38003564),
            (5, 8507, 2001, '2001-09-20 11:00:00 UTC', 8527, 38003564)
        ]
        person_df = pd.DataFrame.from_records(person_data, columns=person_cols)
        observation_cols = [
            'observation_id', 'person_id', 'observation_concept_id',
            'observation_date', 'observation_datetime',
            'observation_type_concept_id'
        ]
        observation_data = [
            (1001, 1, 0, '2008-07-25', '2008-07-25 01:00:00 UTC', 45905771),
            (1005, 2, 0, '2008-03-14', '2008-03-14 02:00:00 UTC', 45905771),
            (1004, 4, 0, '2009-11-25', '2009-11-25 08:00:00 UTC', 45905771),
        ]
        observation_df = pd.DataFrame.from_records(observation_data,
                                                   columns=observation_cols)
        drug_exposure_data = [
            (2002, 1, 50, '2008-06-05', '2008-06-05 01:00:00 UTC', '2010-07-05',
             '2008-06-05 01:00:00 UTC', '2011-04-11', 87),
            (2003, 2, 21, '2008-11-22', '2008-11-22 02:00:00 UTC', 'None',
             'NaT', '2010-06-18', 51),
            (2004, 3, 5241, '2009-08-03', '2009-08-03 05:00:00 UTC', 'None',
             'NaT', '2009-12-26', 2754)
        ]
        drug_exposure_cols = [
            'drug_exposure_id', 'person_id', 'drug_concept_id',
            'drug_exposure_start_date', 'drug_exposure_start_datetime',
            'drug_exposure_end_date', 'drug_exposure_end_datetime',
            'verbatim_end_date', 'drug_type_concept_id'
        ]
        drug_exposure_df = pd.DataFrame.from_records(drug_exposure_data,
                                                     columns=drug_exposure_cols)
        death_data = [(2, '2008-03-12', '2008-03-12 05:00:00 UTC', 8)]
        death_cols = [
            'person_id', 'death_date', 'death_datetime', 'death_type_concept_id'
        ]
        death_df = pd.DataFrame.from_records(death_data, columns=death_cols)
        expected_dict = {
            'person': person_df,
            'observation': observation_df,
            'drug_exposure': drug_exposure_df,
            'death': death_df
        }
        for table in TABLE_ROWS:
            query = f"SELECT * FROM `{self.project_id}.{self.bq_dataset_id}.{table}`"
            job_config = gbq.QueryJobConfig()
            job = self.client.query(query, job_config)
            result_df = job.result().to_dataframe()
            result_df = result_df.dropna(how='all', axis=1)
            date_cols = [col for col in result_df.columns if 'date' in col]
            for date_col in date_cols:
                if 'datetime' in date_col:
                    result_df[date_col] = result_df[date_col].dt.strftime(
                        '%Y-%m-%d %H:%M:%S %Z')
                else:
                    result_df[date_col] = result_df[date_col].astype(str)
            actual = result_df.sort_values('person_id').set_index('person_id')
            expected = expected_dict[table].sort_values('person_id').set_index(
                'person_id')
            pd.testing.assert_frame_equal(actual, expected)

    def tearDown(self):
        test_util.delete_all_tables(self.bq_dataset_id)
        test_util.delete_all_tables(self.bq_sandbox_dataset_id)
        self.client.delete_dataset(self.bq_sandbox_dataset_id)
