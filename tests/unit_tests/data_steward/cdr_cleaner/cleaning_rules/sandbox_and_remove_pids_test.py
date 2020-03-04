import unittest

import mock

from cdr_cleaner.cleaning_rules import sandbox_and_remove_pids
import constants.cdr_cleaner.clean_cdr as cdr_consts


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

    @mock.patch(
        'cdr_cleaner.cleaning_rules.sandbox_and_remove_pids.get_tables_with_person_id'
    )
    def test_get_tables_with_pids(self, mock_get_tables_with_person_id):

        result = []
        mock_get_tables_with_person_id.return_value = (result,
                                                       self.person_table_list)

    @mock.patch(
        'cdr_cleaner.cleaning_rules.sandbox_and_remove_pids.get_tables_with_person_id'
    )
    def test_sandbox_query_generation(self, mock_get_tables_with_person_id):
        result = sandbox_and_remove_pids.get_sandbox_queries(
            self.project_id, self.dataset_id, self.pids, self.ticket_number)
        expected = list()
        person_table_list = mock_get_tables_with_person_id(
            self.project_id, self.dataset_id)

        for table in person_table_list:
            expected.append({
                cdr_consts.QUERY:
                    sandbox_and_remove_pids.SANDBOX_QUERY.format(
                        dataset=self.dataset_id,
                        project=self.project_id,
                        table=table,
                        sandbox_dataset=self.sandbox_dataset,
                        intermediary_table=table + '_' + self.ticket_number,
                        pids=self.pids)
            })
        self.assertEquals(result, expected)

    @mock.patch(
        'cdr_cleaner.cleaning_rules.sandbox_and_remove_pids.get_tables_with_person_id'
    )
    def test_remove_pids_query_generation(self, mock_get_tables_with_person_id):
        result = sandbox_and_remove_pids.get_remove_pids_queries(
            self.project_id, self.dataset_id, self.pids)
        expected = list()
        person_table_list = mock_get_tables_with_person_id(
            self.project_id, self.dataset_id)

        for table in person_table_list:
            expected.append({
                cdr_consts.QUERY:
                    sandbox_and_remove_pids.CLEAN_QUERY.format(
                        project=self.project_id,
                        dataset=self.dataset_id,
                        table=table,
                        pids=self.pids)
            })
        self.assertEquals(result, expected)
