import StringIO
import unittest
import os

from google.appengine.ext import testbed

import common
import gcs_utils
from validation import export
from validation import achilles
from test_util import FAKE_HPO_ID
import test_util
import bq_utils


class ExportTest(unittest.TestCase):
    def setUp(self):
        super(ExportTest, self).setUp()
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_app_identity_stub()
        self.testbed.init_memcache_stub()
        self.testbed.init_urlfetch_stub()
        self.testbed.init_blobstore_stub()
        self.testbed.init_datastore_v3_stub()
        self.hpo_bucket = gcs_utils.get_hpo_bucket(test_util.FAKE_HPO_ID)

    def _write_cloud_str(self, bucket, name, contents_str):
        fp = StringIO.StringIO(contents_str)
        return self._write_cloud_fp(bucket, name, fp)

    def _write_cloud_file(self, bucket, f):
        name = os.path.basename(f)
        with open(f, 'r') as fp:
            return self._write_cloud_fp(bucket, name, fp)

    def _write_cloud_fp(self, bucket, name, fp):
        return gcs_utils.upload_object(bucket, name, fp)

    def _populate_achilles(self):
        for cdm_table in common.CDM_TABLES:
            cdm_file_name = os.path.join(test_util.FIVE_PERSONS_PATH, cdm_table + '.csv')
            if os.path.exists(cdm_file_name):
                self._write_cloud_file(self.hpo_bucket, cdm_file_name)
            else:
                self._write_cloud_str(self.hpo_bucket, cdm_table + '.csv', 'dummy\n')
            bq_utils.load_table_from_bucket(FAKE_HPO_ID, cdm_table)
        achilles.create_tables(FAKE_HPO_ID, True)
        achilles.load_analyses(FAKE_HPO_ID)
        achilles.run_analyses(hpo_id=FAKE_HPO_ID)

    def test_export_from_path(self):
        self._populate_achilles()
        p = os.path.join(export.EXPORT_PATH, 'datadensity')
        r = export.export_from_path(p, FAKE_HPO_ID)
        print r

    def tearDown(self):
        self.testbed.deactivate()
