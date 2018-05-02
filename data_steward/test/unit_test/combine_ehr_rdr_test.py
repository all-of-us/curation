import unittest
import os
import common
import resources
import bq_utils
import test_util

from tools.combine_ehr_rdr import copy_rdr_person, consented_person, CONSENTED_PERSON_TABLE_ID
from google.appengine.api import app_identity
from google.appengine.ext import testbed


class CombineEhrRdrTest(unittest.TestCase):
    EHR_DATASET_ID = bq_utils.get_dataset_id()
    RDR_DATASET_ID = bq_utils.get_rdr_dataset_id()
    COMBINED_DATASET_ID = bq_utils.get_ehr_rdr_dataset_id()
    APP_ID = app_identity.get_application_id()

    @classmethod
    def _list_files_in(cls, path):
        return [f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]

    @classmethod
    def _must_load_datasets(cls):
        table_list = bq_utils.list_tables(cls.EHR_DATASET_ID)
        curr_tables = [t['tableReference']['tableId'] for t in table_list['tables']]
        req_tables = [f.split(os.sep)[-1].split('.')[0] for f in cls._list_files_in(test_util.NYC_FIVE_PERSONS_PATH)]
        if not set(curr_tables).issuperset(set(req_tables)):
            return True
        table_list = bq_utils.list_tables(cls.RDR_DATASET_ID)
        curr_tables = [t['tableReference']['tableId'] for t in table_list['tables']]
        req_tables = [f.split(os.sep)[-1].split('.')[0] for f in cls._list_files_in(test_util.RDR_PATH)]
        if not set(curr_tables).issuperset(set(req_tables)):
            return True
        return False

    @classmethod
    def setUpClass(cls):
        # Ensure test EHR and RDR datasets
        if cls._must_load_datasets():
            cls._load_ehr_and_rdr_datasets()

    @classmethod
    def _load_csv(cls, dataset_id, table_id, local_path, schema):
        cmd_fmt = "bq load --replace --source_format=CSV --allow_jagged_rows --skip_leading_rows=1 %s.%s %s %s"
        cmd = cmd_fmt % (dataset_id, table_id, local_path, schema)
        result = test_util.command(cmd)
        return result

    @classmethod
    def _load_dataset_from_files(cls, dataset_id, path):
        for cdm_table in common.CDM_TABLES:
            cdm_file_name = os.path.join(path, cdm_table + '.csv')
            schema = os.path.join(resources.fields_path, cdm_table + '.json')
            if os.path.exists(cdm_file_name):
                cls._load_csv(dataset_id, cdm_table, cdm_file_name, schema)

    @classmethod
    def _load_ehr_and_rdr_datasets(cls):
        cls._load_dataset_from_files(CombineEhrRdrTest.EHR_DATASET_ID, test_util.NYC_FIVE_PERSONS_PATH)
        cls._load_dataset_from_files(CombineEhrRdrTest.RDR_DATASET_ID, test_util.RDR_PATH)

    def drop_combined_tables(self):
        cmd_fmt = "for i in $(bq ls %s | awk '{print $1}'); do bq rm -ft %s.$i; done;"
        cmd = cmd_fmt % (self.COMBINED_DATASET_ID, self.COMBINED_DATASET_ID)
        result = test_util.bash(cmd)
        return result

    def setUp(self):
        super(CombineEhrRdrTest, self).setUp()
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_app_identity_stub()
        self.testbed.init_memcache_stub()
        self.testbed.init_urlfetch_stub()
        self.testbed.init_blobstore_stub()
        self.testbed.init_datastore_v3_stub()
        self.drop_combined_tables()

    def test_consented_person_id(self):
        """
        Test observation data has seven (7) persons with consent records as described below
         1: No
         2: Yes
         3: NULL
         4: No  followed by Yes
         5: Yes followed by No
         6: Yes followed by NULL
         7: NULL and Yes with same date/time
        """
        # sanity check
        self.assertFalse(bq_utils.table_exists(CONSENTED_PERSON_TABLE_ID, self.COMBINED_DATASET_ID))

        consented_person()
        self.assertTrue(bq_utils.table_exists(CONSENTED_PERSON_TABLE_ID, self.COMBINED_DATASET_ID),
                        'Table {dataset}.{table} created by consented_person'.format(dataset=self.COMBINED_DATASET_ID,
                                                                                     table=CONSENTED_PERSON_TABLE_ID))
        response = bq_utils.query('SELECT * FROM {dataset}.{table}'.format(dataset=self.COMBINED_DATASET_ID,
                                                                           table=CONSENTED_PERSON_TABLE_ID))
        rows = test_util.response2rows(response)
        expected = {2, 4}
        actual = set(row['person_id'] for row in rows)
        self.assertSetEqual(expected,
                            actual,
                            'Records in {dataset}.{table}'.format(dataset=self.COMBINED_DATASET_ID,
                                                                  table=CONSENTED_PERSON_TABLE_ID))

    def test_copy_rdr_person(self):
        consented_person()
        # person records from rdr with consent
        self.assertFalse(bq_utils.table_exists('person', self.COMBINED_DATASET_ID))  # sanity check
        copy_rdr_person()
        self.assertTrue(bq_utils.table_exists('person', self.COMBINED_DATASET_ID))

    def test_ehr_only_records_excluded(self):
        # any ehr records whose person_id is missing from rdr are excluded
        pass

    def test_rdr_only_records_included(self):
        # all rdr records are included whether or not there is corresponding ehr
        pass

    def test_ehr_person_to_observation(self):
        # ehr person table converts to observation records
        pass

    def tearDown(self):
        self.drop_combined_tables()
        self.testbed.deactivate()

    @classmethod
    def tearDownClass(cls):
        test_util.delete_all_tables(CombineEhrRdrTest.EHR_DATASET_ID)
        test_util.delete_all_tables(CombineEhrRdrTest.RDR_DATASET_ID)
