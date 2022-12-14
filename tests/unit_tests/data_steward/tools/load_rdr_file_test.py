from argparse import Namespace
from datetime import datetime
import unittest
from mock import ANY, call, patch, MagicMock, mock_open

from google.cloud.bigquery import LoadJobConfig

from tools import load_rdr_file as lrf


class LoadRDRFileTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.bucket_file = 'gs://filepath.csv'
        self.dest_table = 'abcd.efg.lmnop'
        self.schema_filepath = 'foo.json'
        self.email = 'foo@bar.com'
        self.project_id = 'test'

    def test_parse_args(self):
        # bad arguments, abbreviations used
        self.assertRaises(SystemExit, lrf.parse_args, [
            '--bucket', self.bucket_file, '--run_as', self.email,
            '--curation_proj', self.project_id, '-l', '--destination_tab',
            self.dest_table, '--schema_file', self.schema_filepath
        ])

        args = [
            '--bucket_filepath', self.bucket_file, '--run_as', self.email,
            '--curation_project', self.project_id, '-l', '--destination_table',
            self.dest_table, '--schema_filepath', self.schema_filepath
        ]
        actual = Namespace(bucket_filepath=self.bucket_file,
                           run_as_email=self.email,
                           curation_project_id=self.project_id,
                           console_log=True,
                           fq_dest_table=self.dest_table,
                           schema_filepath=self.schema_filepath)
        # good arguments
        with patch('os.path.isfile', return_value=True):
            self.assertEqual(lrf.parse_args(args), actual)

    def test_load_rdr_file(self):

        # test setup
        mo = mock_open(
            read_data=
            '[{"name": "person_id", "type": "int", "mode": "required", "description": "none"}]'
        )

        # running the test
        # mock opening a json file
        with patch("tools.load_rdr_file.open", mo) as mock_file:
            # mock creating a BigQueryClient object
            with patch('tools.load_rdr_file.BigQueryClient',
                       return_value=MagicMock()) as client:
                lrf.load_rdr_table(client, self.bucket_file, self.dest_table,
                                   self.schema_filepath)

        # post condition checks
        mock_file.assert_called_with(self.schema_filepath, 'r')
        calls = [
            call.load_table_from_uri(self.bucket_file,
                                     self.dest_table,
                                     job_config=ANY,
                                     job_id_prefix=ANY),
            call.load_table_from_uri().result(),
            call.get_table(self.dest_table)
        ]
        client.assert_has_calls(calls)

    def test_main(self):

        # test setup
        mo = mock_open(
            read_data=
            '[{"name": "person_id", "type": "int", "mode": "required", "description": "none"}]'
        )
        args = [
            '--bucket_filepath', self.bucket_file, '--run_as', self.email,
            '--curation_project', self.project_id, '-l', '--destination_table',
            self.dest_table, '--schema_filepath', self.schema_filepath
        ]

        # running the test
        # mock opening a json file
        with patch("tools.load_rdr_file.open", mo) as mock_file:
            # mock creating a BigQueryClient object
            with patch('tools.load_rdr_file.BigQueryClient',
                       return_value=MagicMock()) as client:
                with patch('os.path.isfile', return_value=True):

                    lrf.main(args)

        # post condition checks
        mock_file.assert_called_with(self.schema_filepath, 'r')
        calls = [call(self.project_id, credentials=ANY)]
        client.assert_has_calls(calls)
