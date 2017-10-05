import StringIO
import unittest
import os

from google.appengine.ext import testbed

import gcs_utils
import resources
from validation import export
from validation import achilles
from test_util import FAKE_HPO_ID
import test_util
import time
import bq_utils

BQ_TIMEOUT_SECONDS = 5


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
        from google.appengine.api import app_identity

        app_id = app_identity.get_application_id()
        for table_name in achilles.ACHILLES_TABLES:
            schema_file_name = table_name + '.json'
            schema_path = os.path.join(resources.fields_path, schema_file_name)
            test_file_name = table_name + '.csv'
            test_file_path = os.path.join(test_util.TEST_DATA_PATH, table_name + '.csv')
            self._write_cloud_file(self.hpo_bucket, test_file_path)
            gcs_path = 'gs://' + self.hpo_bucket + '/' + test_file_name
            dataset_id = bq_utils.get_dataset_id()
            table_id = bq_utils.get_table_id(FAKE_HPO_ID, table_name)
            bq_utils.load_csv(schema_path, gcs_path, app_id, dataset_id, table_id)
        time.sleep(BQ_TIMEOUT_SECONDS)

    def _export_from_path(p, hpo_id):
        """Utility to create response test payloads"""
        for f in export.list_files_only(p):
            abs_path = os.path.join(p, f)
            with open(abs_path, 'r') as fp:
                sql = fp.read()
                sql = export.render(sql, hpo_id, results_schema=bq_utils.get_dataset_id(), vocab_schema='synpuf_100')
                query_result = bq_utils.query(sql)
                with open(f + '.json', 'w') as fp:
                    data = dict()
                    if 'rows' in query_result:
                        data['rows'] = query_result['rows']
                    if 'schema' in query_result:
                        data['schema'] = query_result['schema']
                    import json
                    json.dump(data, fp, sort_keys=True, indent=4, separators=(',', ': '))

    def test_export_from_path(self):
        self._populate_achilles()
        p = os.path.join(export.EXPORT_PATH, 'datadensity')
        r = export.export_from_path(p, FAKE_HPO_ID)
        print r

    def tearDown(self):
        self.testbed.deactivate()
