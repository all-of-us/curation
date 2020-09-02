"""
Integration test for the remove_ehr_data_past_deactivation_date module

Original Issue: DC-686

The intent is to sandbox and drop records dated after the date of deactivation for participants
who have deactivated from the Program.
"""

# Python imports
import unittest
import mock

# Third party imports
import pandas
from jinja2 import Environment

# Project imports
import app_identity
import bq_utils
import utils.participant_summary_requests as psr
import retraction.retract_deactivated_pids as rdp
import tests.integration_tests.data_steward.retraction.retract_deactivated_pids_test as rdpt
import cdr_cleaner.cleaning_rules.remove_ehr_data_past_deactivation_date as red
from sandbox import check_and_create_sandbox_dataset
from constants.cdr_cleaner import clean_cdr as clean_consts
from utils import bq
from tests import test_util

TABLES = [
    'measurement', 'observation', 'procedure_occurrence',
    'condition_occurrence', 'drug_exposure', 'visit_occurrence'
]

jinja_env = Environment(
    # help protect against cross-site scripting vulnerabilities
    autoescape=True,
    # block tags on their own lines
    # will not cause extra white space
    trim_blocks=True,
    lstrip_blocks=True,
    # syntax highlighting should be better
    # with these comment delimiters
    comment_start_string='--',
    comment_end_string=' --')


class RemoveEhrDataPastDeactivationDateTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = app_identity.get_application_id()
        self.dataset_id = bq_utils.get_dataset_id()
        self.sandbox_id = check_and_create_sandbox_dataset(
            self.project_id, self.dataset_id)
        self.tablename = '_deactivated_participants'
        self.ticket_number = 'DC12345'

        self.columns = ['participantId', 'suspensionStatus', 'suspensionTime']

        self.deactivated_participants = [(1, 'NO_CONTACT', '2018-12-07'),
                                         (2, 'NO_CONTACT', '2019-12-07'),
                                         (3, 'NO_CONTACT', '2017-12-07')]

        self.json_response_entry = {
            'entry': [{
                'fullUrl':
                    'https//foo_project.appspot.com/rdr/v1/Participant/P1/Summary',
                'resource': {
                    'participantId': 'P1',
                    'suspensionStatus': 'NO_CONTACT',
                    'suspensionTime': '2018-12-07T08:21:14'
                }
            }, {
                'fullUrl':
                    'https//foo_project.appspot.com/rdr/v1/Participant/P2/Summary',
                'resource': {
                    'participantId': 'P2',
                    'suspensionStatus': 'NO_CONTACT',
                    'suspensionTime': '2019-12-07T08:21:14'
                }
            }, {
                'fullUrl':
                    'https//foo_project.appspot.com/rdr/v1/Participant/P3/Summary',
                'resource': {
                    'participantId': 'P3',
                    'suspensionStatus': 'NO_CONTACT',
                    'suspensionTime': '2017-12-07T08:21:14'
                }
            }]
        }

        self.client = bq.get_client(self.project_id)

    @mock.patch('utils.participant_summary_requests.requests.get')
    @mock.patch(
        'retraction.retract_deactivated_pids.get_date_info_for_pids_tables')
    def test_remove_ehr_data_past_deactivation_date(self, mock_retraction_info,
                                                    mock_get):
        # pre conditions for participant summary API module
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = self.json_response_entry

        # Ensure deactivated participants table is created and or updated
        psr.get_deactivated_participants(self.project_id, self.dataset_id,
                                         self.tablename, self.columns)

        # pre conditions for retraction module
        d = {
            'project_id': [
                self.project_id, self.project_id, self.project_id,
                self.project_id, self.project_id, self.project_id
            ],
            'dataset_id': [
                self.dataset_id, self.dataset_id, self.dataset_id,
                self.dataset_id, self.dataset_id, self.dataset_id
            ],
            'table': [
                'condition_occurrence', 'drug_exposure', 'measurement',
                'observation', 'procedure_occurrence', 'visit_occurrence'
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
        retraction_info = pandas.DataFrame(data=d)
        mock_retraction_info.return_value = retraction_info

        load_data_queries = []
        dropped_row_count_queries = []
        kept_row_count_queries = []
        sandbox_row_count_queries = []

        # Queries to load the dummy data into the tables
        measurement_query = jinja_env.from_string("""
        INSERT INTO `{{project}}.{{dataset}}.{{measurement}}`
        (measurement_id, person_id, measurement_concept_id, measurement_date,
        measurement_type_concept_id)
        VALUES
            (1234, 1, 0, date('2017-12-07'), 0),
            (5678, 2, 0, date('2017-12-07'), 0),
            (2345, 3, 0, date('2018-12-07'), 0)""").render(
            project=self.project_id,
            dataset=self.dataset_id,
            measurement=TABLES[0])
        load_data_queries.append(measurement_query)

        observation_query = jinja_env.from_string("""
        INSERT INTO `{{project}}.{{dataset}}.{{observation}}`
        (observation_id, person_id, observation_concept_id, observation_date,
        observation_type_concept_id)
        VALUES
            (1234, 1, 0, date('2017-12-07'), 0),
            (5678, 2, 0, date('2017-12-07'), 0),
            (2345, 3, 0, date('2018-12-07'), 0)""").render(
            project=self.project_id,
            dataset=self.dataset_id,
            observation=TABLES[1])
        load_data_queries.append(observation_query)

        procedure_occ_query = jinja_env.from_string("""
        INSERT INTO `{{project}}.{{dataset}}.{{procedure}}`
        (procedure_occurrence_id, person_id, procedure_concept_id, procedure_date,
        procedure_datetime, procedure_type_concept_id)
        VALUES
            (1234, 1, 0, date('2017-12-07'), timestamp('2017-12-07T08:21:14'), 0),
            (5678, 2, 0, date('2017-12-07'), timestamp('2017-12-07T08:21:14'), 0), 
            (2345, 3, 0, date('2018-12-07'), timestamp('2018-12-07T08:21:14'), 0)"""
                                                   ).render(
                                                       project=self.project_id,
                                                       dataset=self.dataset_id,
                                                       procedure=TABLES[2])
        load_data_queries.append(procedure_occ_query)

        condition_occ_query = jinja_env.from_string("""
        INSERT INTO `{{project}}.{{dataset}}.{{condition}}`
        (condition_occurrence_id, person_id, condition_concept_id, condition_start_date,
        condition_start_datetime, condition_end_date, condition_type_concept_id)
        VALUES
            (1234, 1, 0, date('2017-12-07'), timestamp('2017-12-07T08:21:14'), date('2017-12-08'), 0),
            (5678, 2, 0, date('2017-12-07'), timestamp('2017-12-07T08:21:14'), date('2017-12-08'), 0), 
            (2345, 3, 0, date('2018-12-07'), timestamp('2018-12-07T08:21:14'), date('2018-12-08'), 0)"""
                                                   ).render(
                                                       project=self.project_id,
                                                       dataset=self.dataset_id,
                                                       condition=TABLES[3])
        load_data_queries.append(condition_occ_query)

        drug_query = jinja_env.from_string("""
        INSERT INTO `{{project}}.{{dataset}}.{{drug}}`
        (drug_exposure_id, person_id, drug_concept_id, drug_exposure_start_date, 
        drug_exposure_start_datetime, drug_exposure_end_date, drug_type_concept_id)
        VALUES
            (1234, 1, 0, date('2017-12-07'), timestamp('2017-12-07T08:21:14'), date('2017-12-08'), 0),
            (5678, 2, 0, date('2017-12-07'), timestamp('2017-12-07T08:21:14'), date('2017-12-08'), 0), 
            (2345, 3, 0, date('2018-12-07'), timestamp('2018-12-07T08:21:14'), date('2018-12-08'), 0)"""
                                          ).render(project=self.project_id,
                                                   dataset=self.dataset_id,
                                                   drug=TABLES[4])
        load_data_queries.append(drug_query)

        visit_query = jinja_env.from_string("""
        INSERT INTO `{{project}}.{{dataset}}.{{visit}}`
        (visit_occurrence_id, person_id, visit_concept_id, visit_start_date, visit_start_datetime,
        visit_end_date, visit_type_concept_id)
        VALUES
            (1234, 1, 0, date('2017-12-07'), timestamp('2017-12-07T08:21:14'), date('2017-12-08'), 0),
            (5678, 2, 0, date('2017-12-07'), timestamp('2017-12-07T08:21:14'), date('2017-12-08'), 0), 
            (2345, 3, 0, date('2018-12-07'), timestamp('2018-12-07T08:21:14'), date('2018-12-08'), 0)"""
                                           ).render(project=self.project_id,
                                                    dataset=self.dataset_id,
                                                    visit=TABLES[5])
        load_data_queries.append(visit_query)

        # Create tables
        fq_table_names = []
        for table_name in TABLES:
            fq_table_names.append(
                f'{self.project_id}.{self.dataset_id}.{table_name}')
        bq.create_tables(self.client,
                         self.project_id,
                         fq_table_names,
                         exists_ok=True)

        # Load queries
        for query in load_data_queries:
            response = self.client.query(query)
            self.assertIsNotNone(response.result())
            self.assertIsNone(response.exception())

        # Store query for checking number of rows to delete
        for ehr in self.deactivated_participants:
            pid = ehr[0]
            for row in retraction_info.itertuples(index=False):
                if row.date_column is None:
                    dropped_query = rdpt.EXPECTED_DROPPED_ROWS_QUERY_END_DATE.format(
                        dataset_id=self.dataset_id,
                        table_id=row.table,
                        pid_table_id=self.tablename,
                        pid=pid,
                        start_date_column=row.start_date_column,
                        end_date_column=row.end_date_column)
                    kept_query = rdpt.EXPECTED_KEPT_ROWS_QUERY_END_DATE.format(
                        dataset_id=self.dataset_id,
                        table_id=row.table,
                        pid_table_id=self.tablename,
                        pid=pid,
                        start_date_column=row.start_date_column,
                        end_date_column=row.end_date_column)
                    sandbox_query = rdp.SANDBOX_QUERY_END_DATE.render(
                        project=self.project_id,
                        dataset=self.dataset_id,
                        table=row.table,
                        pid=pid,
                        end_date_column=row.end_date_column,
                        start_date_column=row.start_date_column,
                        deactivated_pids_project=self.project_id,
                        deactivated_pids_dataset=self.dataset_id,
                        deactivated_pids_table=self.tablename)
                else:
                    dropped_query = rdpt.EXPECTED_DROPPED_ROWS_QUERY.format(
                        dataset_id=self.dataset_id,
                        table_id=row.table,
                        pid_table_id=self.tablename,
                        pid=pid,
                        date_column=row.date_column)
                    kept_query = rdpt.EXPECTED_KEPT_ROWS_QUERY.format(
                        dataset_id=self.dataset_id,
                        table_id=row.table,
                        pid_table_id=self.tablename,
                        pid=pid,
                        date_column=row.date_column)
                    sandbox_query = rdp.SANDBOX_QUERY_DATE.render(
                        project=self.project_id,
                        dataset=self.dataset_id,
                        table=row.table,
                        pid=pid,
                        date_column=row.date_column,
                        deactivated_pids_project=self.project_id,
                        deactivated_pids_dataset=self.dataset_id,
                        deactivated_pids_table=self.tablename)
                dropped_row_count_queries.append({
                    clean_consts.QUERY: dropped_query,
                    clean_consts.DESTINATION_DATASET: self.dataset_id,
                    clean_consts.DESTINATION_TABLE: row.table
                })
                kept_row_count_queries.append({
                    clean_consts.QUERY: kept_query,
                    clean_consts.DESTINATION_DATASET: self.dataset_id,
                    clean_consts.DESTINATION_TABLE: row.table
                })
                sandbox_row_count_queries.append({
                    clean_consts.QUERY: sandbox_query,
                    clean_consts.DESTINATION_DATASET: self.sandbox_id,
                    clean_consts.DESTINATION_TABLE: self.tablename
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
        q = rdpt.TABLE_ROWS_QUERY.format(dataset_id=self.dataset_id)
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
        query_list = red.remove_ehr_data_queries(self.project_id,
                                                 self.ticket_number,
                                                 self.project_id,
                                                 self.dataset_id,
                                                 self.tablename)
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

    def tearDown(self):
        test_util.delete_all_tables(self.dataset_id)
        test_util.delete_all_tables(self.sandbox_id)
