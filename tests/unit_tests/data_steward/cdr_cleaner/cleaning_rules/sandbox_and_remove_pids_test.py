# Python imports
import unittest

# Third party imports
import mock
import pandas as pd

# Project imports
from cdr_cleaner.cleaning_rules import sandbox_and_remove_pids
from constants.cdr_cleaner import clean_cdr as clean_consts
import constants.cdr_cleaner.clean_cdr as cdr_consts
from constants import bq_utils as bq_consts


class SandboxAndRemovePidsTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'project_id'
        self.dataset_id = 'dataset_id'
        self.sandbox_dataset = 'sandbox_dataset'
        self.ticket_number = 'DC_XXX'
        self.pids = [
            324264993, 307753491, 335484227, 338965846, 354812933, 324983298,
            366423185, 352721597, 352775367, 314281264, 319123185, 325306942,
            324518105, 320577401, 339641873, 329210551, 364674103, 339564778,
            309381334, 352068257, 353001073, 319604059, 336744297, 357830316,
            352653514, 349988031, 349731310, 359249014, 361359486, 315083772,
            358741126, 312045923, 313427389, 341366267, 305170199, 308597253,
            348834424, 325536292, 360363123
        ]
        self.person_table_list = [
            'observation', 'drug_era', 'observation_period', '_deid_map',
            'visit_occurrence', 'condition_era', 'measurement', 'person',
            'procedure_occurrence', 'specimen', 'death', 'device_exposure',
            'payer_plan_period', '_ehr_consent', 'condition_occurrence',
            'drug_exposure', 'note', 'dose_era'
        ]

        self.mapping_table_list = [
            '_mapping_measurement', '_mapping_condition_occurrence',
            '_mapping_procedure_occurrence'
        ]

    @mock.patch('cdr_cleaner.cleaning_rules.sandbox_and_remove_pids.bq.query')
    def test_get_tables_with_pids(self, mock_query):
        mock_query.return_value = pd.DataFrame(
            self.person_table_list + self.mapping_table_list,
            columns=[sandbox_and_remove_pids.TABLE_NAME_COLUMN])
        actual = sandbox_and_remove_pids.get_tables_with_person_id(
            self.project_id, self.dataset_id)
        self.assertListEqual(self.person_table_list, actual)

    @mock.patch(
        'cdr_cleaner.cleaning_rules.sandbox_and_remove_pids.get_tables_with_person_id'
    )
    @mock.patch(
        'cdr_cleaner.cleaning_rules.sandbox_and_remove_pids.get_sandbox_dataset_id'
    )
    def test_sandbox_query_generation(self, mock_get_sandbox_dataset_id,
                                      mock_get_tables_with_person_id):

        mock_get_tables_with_person_id.return_value = self.person_table_list
        mock_get_sandbox_dataset_id.return_value = self.sandbox_dataset

        result = sandbox_and_remove_pids.get_sandbox_queries(
            self.project_id, self.dataset_id, self.pids, self.ticket_number)
        expected = list()

        for table in self.person_table_list:
            expected.append({
                cdr_consts.QUERY:
                    sandbox_and_remove_pids.SANDBOX_QUERY.format(
                        dataset=self.dataset_id,
                        project=self.project_id,
                        table=table,
                        sandbox_dataset=self.sandbox_dataset,
                        intermediary_table=table + '_' + self.ticket_number,
                        pids=','.join([str(i) for i in self.pids]))
            })
        self.assertEquals(result, expected)

    @mock.patch(
        'cdr_cleaner.cleaning_rules.sandbox_and_remove_pids.get_tables_with_person_id'
    )
    def test_remove_pids_query_generation(self, mock_get_tables_with_person_id):

        mock_get_tables_with_person_id.return_value = self.person_table_list

        result = sandbox_and_remove_pids.get_remove_pids_queries(
            self.project_id, self.dataset_id, self.pids)
        expected = list()

        for table in self.person_table_list:
            expected.append({
                cdr_consts.QUERY:
                    sandbox_and_remove_pids.CLEAN_QUERY.format(
                        project=self.project_id,
                        dataset=self.dataset_id,
                        table=table,
                        pids=','.join([str(i) for i in self.pids])),
                clean_consts.DESTINATION_TABLE:
                    table,
                clean_consts.DESTINATION_DATASET:
                    self.dataset_id,
                clean_consts.DISPOSITION:
                    bq_consts.WRITE_TRUNCATE
            })
        self.assertEquals(result, expected)
