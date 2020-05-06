# Python imports
import unittest
import re

# Third party imports
import mock
import pandas as pd
from pandas.util.testing import assert_frame_equal

# Project imports
from retraction import retract_deactivated_pids
from utils import bq
from cdr_cleaner.cleaning_rules import sandbox_and_remove_pids
from constants.cdr_cleaner import clean_cdr as clean_consts
import constants.cdr_cleaner.clean_cdr as cdr_consts
from sandbox import get_sandbox_dataset_id
from constants import bq_utils as bq_consts
from retraction.retract_utils import DEID_REGEX


class RetractDataBqTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'fake_project_id'
        self.ticket_number = 'DC_fake'
        self.pids_project_id = 'fake_pids_project_id'
        self.pids_dataset_id = 'fake_pids_dataset_id'
        self.pids_table = 'fake_pids_table'
        self.pids_dataset_list = [
            'fake_dataset_1', 'fake_dataset_2', 'fake_dataset_3'
        ]
        self.pids_table_list = ['fake_table_1', 'fake_table_2', 'fake_table_3']

    @mock.patch('utils.bq.list_datasets')
    @mock.patch('utils.bq.query')
    def test_get_pids_datasets_and_tables(self, mock_query, mock_datasets_list):
        dataset_1 = mock.Mock(spec=['dataset_1'], dataset_id='dataset_id_1')
        dataset_2 = mock.Mock(spec=['dataset_2'], dataset_id='dataset_id_2')
        expected_dataset_list = mock_datasets_list.return_value = [
            dataset_1, dataset_2
        ]

        table_list = ['table_1', 'table_2', 'table_3']
        df = pd.DataFrame(data={'table_name': table_list})
        mock_query.return_value = df

        expected_pids_table_list = []
        expected_pids_dataset_list = []
        for dataset in expected_dataset_list:
            pid_tables = sandbox_and_remove_pids.get_tables_with_person_id(
                self.project_id, dataset)
            if pid_tables:
                expected_pids_table_list.extend(pid_tables)
                expected_pids_dataset_list.append(dataset.dataset_id)

        returned_pids_table_list, returned_pids_dataset_list = retract_deactivated_pids.get_pids_datasets_and_tables(
            self.project_id)

        self.assertListEqual(returned_pids_dataset_list,
                             expected_pids_dataset_list)
        self.assertListEqual(returned_pids_table_list, expected_pids_table_list)

    @mock.patch(
        'retraction.retract_deactivated_pids.get_pids_datasets_and_tables')
    @mock.patch('utils.bq.get_table_info_for_dataset')
    def test_get_date_info_for_pids_tables(self, mock_get_table_info,
                                           mock_get_pids_datasets_and_tables):
        mock_get_pids_datasets_and_tables.return_value = self.pids_dataset_list, self.pids_table_list
        mock_get_table_info.return_value = pd.DataFrame(columns=[
            'table_catalog', 'table_schema', 'table_name', 'column_name',
            'ordinal_position', 'is_nullable', 'data_type', 'is_generated',
            'generation_expression', 'is_stored', 'is_hidden', 'is_updatable',
            'is_system_defined', 'is_partitioning_column',
            'clustering_ordinal_position'
        ])
        retraction_info_df = pd.DataFrame()

        for dataset in self.pids_dataset_list:
            table_info_df = bq.get_table_info_for_dataset(
                self.project_id, dataset)
            pids_tables_df = table_info_df[table_info_df['table_name'].isin(
                self.pids_table_list)]

            # Keep only records with datatype of 'DATE'
            date_fields_df = pids_tables_df[pids_tables_df['data_type'] ==
                                            'DATE']

            # Create df to append to, keeping only one record per table
            df_to_append = pd.DataFrame(columns=[
                'project_id', 'dataset_id', 'table', 'date_column',
                'start_date_column', 'end_date_column'
            ])
            df_to_append['project_id'] = date_fields_df['table_catalog']
            df_to_append['dataset_id'] = date_fields_df['table_schema']
            df_to_append['table'] = date_fields_df['table_name']
            df_to_append = df_to_append.drop_duplicates()

            # Create new df to loop through date time fields
            df_to_iterate = pd.DataFrame(
                columns=['project_id', 'dataset_id', 'table', 'column'])
            df_to_iterate['project_id'] = date_fields_df['table_catalog']
            df_to_iterate['dataset_id'] = date_fields_df['table_schema']
            df_to_iterate['table'] = date_fields_df['table_name']
            df_to_iterate['column'] = date_fields_df['column_name']

            # Remove person table and death table
            df_to_append = df_to_append[~df_to_append.table.str.
                                        contains('death', 'person')]
            df_to_iterate = df_to_iterate[~df_to_iterate.table.str.
                                          contains('death', 'person')]

            # Filter through date columns and append to the appropriate column
            for i, row in df_to_iterate.iterrows():
                column = getattr(row, 'column')
                table = getattr(row, 'table')
                if 'start_date' in column:
                    df_to_append.loc[df_to_append.table == table,
                                     'start_date_column'] = column
                elif 'end_date' in column:
                    df_to_append.loc[df_to_append.table == table,
                                     'end_date_column'] = column
                else:
                    df_to_append.loc[df_to_append.table == table,
                                     'date_column'] = column

        date_fields_info_df = retraction_info_df.append(df_to_append)
        returned_df = retract_deactivated_pids.get_date_info_for_pids_tables(
            self.project_id)

        assert_frame_equal(date_fields_info_df, returned_df)

    @mock.patch('utils.bq.query')
    @mock.patch(
        'retraction.retract_deactivated_pids.get_date_info_for_pids_tables')
    def test_create_queries(self, mock_retraction_info, mock_query):
        table_list = ['table_1', 'table_2', 'table_3']
        df = pd.DataFrame(data={'table_name': table_list})
        mock_query.return_value = df

        returned = retract_deactivated_pids.create_queries(
            self.project_id, self.ticket_number, self.pids_project_id,
            self.pids_dataset_id, self.pids_table)

        expected_queries_list = []
        d = {
            'person_id': [1, 2],
            'research_id': [3, 4],
            'deactivated_date': ['2019-06-01', '2020-01-01']
        }
        deactivated_ehr_pids_df = pd.DataFrame(
            columns=['person_id', 'research_id', 'deactivated_date'], data=d)

        mock_retraction_info.return_value = pd.DataFrame(columns=[
            'project_id', 'dataset_id', 'table', 'date_column',
            'start_date_column', 'end_date_column'
        ])
        retraction_info_df = retract_deactivated_pids.get_date_info_for_pids_tables(
            self.project_id)

        for ehr_row in deactivated_ehr_pids_df.itertuples(index=False):
            for retraction_row in retraction_info_df.itertuples(index=False):
                if get_sandbox_dataset_id(
                        retraction_row.dataset_id) in bq.list_datasets(
                            self.project_id):
                    sandbox_dataset = get_sandbox_dataset_id(
                        retraction_row.dataset_id)
                else:
                    sandbox_dataset = get_sandbox_dataset_id(
                        retraction_row.dataset_id)

                if re.match(DEID_REGEX, retraction_row.dataset_id):
                    pid = ehr_row.research_id
                    identifier = 'research_id'
                else:
                    pid = ehr_row.person_id
                    identifier = 'person_id'

                if not pd.isnull(retraction_row.date_column):
                    sandbox_queries = dict()
                    sandbox_queries[
                        cdr_consts.
                        QUERY] = retract_deactivated_pids.SANDBOX_QUERY_DATE.format(
                            project=retraction_row.project_id,
                            sandbox_dataset=sandbox_dataset,
                            intermediary_table=self.ticket_number + '_' +
                            retraction_row.table,
                            dataset=retraction_row.dataset_id,
                            table=retraction_row.table,
                            identifier=identifier,
                            pid=pid,
                            deactivated_pids_project=self.pids_project_id,
                            deactivated_pids_dataset=self.pids_dataset_id,
                            deactivated_pids_table=self.pids_table,
                            date_column=retraction_row.date_column)
                    expected_queries_list.append(sandbox_queries)
                    clean_query = retract_deactivated_pids.CLEAN_QUERY_DATE.format(
                        project=retraction_row.project_id,
                        dataset=retraction_row.dataset_id,
                        table=retraction_row.table,
                        identifier=identifier,
                        pid=pid,
                        deactivated_pids_project=self.pids_project_id,
                        deactivated_pids_dataset=self.pids_dataset_id,
                        deactivated_pids_table=self.pids_table,
                        date_column=retraction_row.date_column)
                    expected_queries_list.append({
                        clean_consts.QUERY:
                            clean_query,
                        clean_consts.DESTINATION_DATASET:
                            retraction_row.dataset_id,
                        clean_consts.DESTINATION_TABLE:
                            retraction_row.table,
                        clean_consts.DISPOSITION:
                            bq_consts.WRITE_TRUNCATE
                    })
                else:
                    sandbox_queries = dict()
                    sandbox_queries[
                        cdr_consts.
                        QUERY] = retract_deactivated_pids.SANDBOX_QUERY_END_DATE.format(
                            project=retraction_row.project_id,
                            sandbox_dataset=sandbox_dataset,
                            intermediary_table=self.ticket_number + '_' +
                            retraction_row.table,
                            dataset=retraction_row.dataset_id,
                            table=retraction_row.table,
                            identifier=identifier,
                            pid=pid,
                            deactivated_pids_project=self.pids_project_id,
                            deactivated_pids_dataset=self.pids_dataset_id,
                            deactivated_pids_table=self.pids_table,
                            end_date_column=retraction_row.end_date_column,
                            start_date_column=retraction_row.start_date_column)
                    expected_queries_list.append(sandbox_queries)
                    clean_query = retract_deactivated_pids.CLEAN_QUERY_END_DATE.format(
                        project=retraction_row.project_id,
                        dataset=retraction_row.dataset_id,
                        table=retraction_row.table,
                        identifier=identifier,
                        pid=pid,
                        deactivated_pids_project=self.pids_project_id,
                        deactivated_pids_dataset=self.pids_dataset_id,
                        deactivated_pids_table=self.pids_table,
                        end_date_column=retraction_row.end_date_column,
                        start_date_column=retraction_row.start_date_column)
                    expected_queries_list.append({
                        clean_consts.QUERY:
                            clean_query,
                        clean_consts.DESTINATION_DATASET:
                            retraction_row.dataset_id,
                        clean_consts.DESTINATION_TABLE:
                            retraction_row.table,
                        clean_consts.DISPOSITION:
                            bq_consts.WRITE_TRUNCATE
                    })

        self.assertEquals(returned, expected_queries_list)
