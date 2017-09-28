import unittest

from google.appengine.ext import testbed
from validation import achilles
from test_util import FAKE_HPO_ID

# This may change if we strip out unused analyses
ACHILLES_ANALYSIS_COUNT = 215


class AchillesTest(unittest.TestCase):
    def setUp(self):
        super(AchillesTest, self).setUp()
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_app_identity_stub()
        self.testbed.init_memcache_stub()
        self.testbed.init_urlfetch_stub()
        self.testbed.init_blobstore_stub()
        self.testbed.init_datastore_v3_stub()

    def test_detect_commented_block(self):
        commented_command = """
--1300- ORGANIZATION

--NOT APPLICABLE IN CDMV5
--insert into fake_ACHILLES_analysis (analysis_id, analysis_name, stratum_1_name)
--	values (1300, 'Number of organizations by place of service', 'place_of_service_concept_id')"""
        self.assertFalse(achilles.is_active_command(commented_command))

    def test_get_load_analysis_commands(self):
        cmd_iter = achilles._get_load_analysis_commands(FAKE_HPO_ID)
        commands = list(cmd_iter)
        self.assertEqual(len(commands), ACHILLES_ANALYSIS_COUNT)

    def test_load_analyses(self):
        # Long-running test
        achilles.create_tables(FAKE_HPO_ID, True)
        achilles.load_analyses(FAKE_HPO_ID)

    def tearDown(self):
        self.testbed.deactivate()
