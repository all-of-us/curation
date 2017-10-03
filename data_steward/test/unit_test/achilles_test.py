import unittest

import os
import StringIO
from google.appengine.ext import testbed

import bq_utils
import common
from validation import achilles
from test_util import FAKE_HPO_ID
import gcs_utils
import test_util

# This may change if we strip out unused analyses
ACHILLES_LOOKUP_COUNT = 215
ACHILLES_ANALYSIS_COUNT = 367
SOURCE_NAME_QUERY = """insert into synpuf_100.achilles_analysis (analysis_id, analysis_name) 
  values (0, 'Source name')"""
TEMP_QUERY_1 = """INTO temp.tempresults
   WITH rawdata  as ( select  p.person_id as person_id, min(EXTRACT(YEAR from observation_period_start_date)) - p.year_of_birth  as age_value   from  synpuf_100.person p
  join synpuf_100.observation_period op on p.person_id = op.person_id
   group by  p.person_id, p.year_of_birth
 ), overallstats  as (select  cast(avg(1.0 * age_value)  as float64)  as avg_value, cast(STDDEV(age_value)  as float64)  as stdev_value, min(age_value)  as min_value, max(age_value)  as max_value, COUNT(*)  as total  from  rawdata
), agestats  as ( select  age_value as age_value, COUNT(*)  as total, row_number() over (order by age_value)  as rn   from  rawdata
   group by  1 ), agestatsprior  as ( select  s.age_value as age_value, s.total as total, sum(p.total)  as accumulated   from  agestats s
  join agestats p on p.rn <= s.rn
   group by  s.age_value, s.total, s.rn
 )
 select  103 as analysis_id, o.total as count_value, o.min_value, o.max_value, o.avg_value, o.stdev_value, min(case when p.accumulated >= .50 * o.total then age_value end) as median_value, min(case when p.accumulated >= .10 * o.total then age_value end) as p10_value, min(case when p.accumulated >= .25 * o.total then age_value end) as p25_value, min(case when p.accumulated >= .75 * o.total then age_value end) as p75_value, min(case when p.accumulated >= .90 * o.total then age_value end) as p90_value
  FROM  agestatsprior p
cross join overallstats o
 group by  o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value"""
TEMP_QUERY_2 = """INTO temp.rawdata_1006
  SELECT ce.condition_concept_id as subject_id, p1.gender_concept_id, ce.condition_start_year - p1.year_of_birth as count_value
  FROM  synpuf_100.person p1
inner join
(
   select  person_id, condition_concept_id, min(EXTRACT(YEAR from condition_era_start_date)) as condition_start_year
    from  synpuf_100.condition_era
   group by  1, 2 ) ce on p1.person_id = ce.person_id"""


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

    def _load_dataset(self):
        for cdm_table in common.CDM_TABLES:
            cdm_file_name = os.path.join(test_util.FIVE_PERSONS_PATH, cdm_table + '.csv')
            if os.path.exists(cdm_file_name):
                self._write_cloud_file(self.hpo_bucket, cdm_file_name)
            else:
                self._write_cloud_str(self.hpo_bucket, cdm_table + '.csv', 'dummy\n')
            bq_utils.load_table_from_bucket(FAKE_HPO_ID, cdm_table)

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
        self.assertEqual(len(commands), ACHILLES_LOOKUP_COUNT)

    def _test_load_analyses(self):
        # Long-running test
        achilles.create_tables(FAKE_HPO_ID, True)
        achilles.load_analyses(FAKE_HPO_ID)

    def test_get_run_analysis_commands(self):
        cmd_iter = achilles._get_run_analysis_commands(FAKE_HPO_ID)
        commands = list(cmd_iter)
        self.assertEqual(len(commands), ACHILLES_ANALYSIS_COUNT)

    def test_temp_table(self):
        self.assertTrue(achilles.is_to_temp_table(TEMP_QUERY_1))
        self.assertTrue(achilles.is_to_temp_table(TEMP_QUERY_2))
        self.assertFalse(achilles.is_to_temp_table(SOURCE_NAME_QUERY))
        self.assertEqual(achilles.get_temp_table_name(TEMP_QUERY_1), 'temp.tempresults')
        self.assertEqual(achilles.get_temp_table_name(TEMP_QUERY_2), 'temp.rawdata_1006')
        self.assertTrue(achilles.get_temp_table_query(TEMP_QUERY_1).startswith('WITH rawdata'))
        self.assertTrue(achilles.get_temp_table_query(TEMP_QUERY_2).startswith('SELECT ce.condition_concept_id'))

    def test_run_analyses(self):
        # Long-running test
        self._load_dataset()
        # achilles.create_tables(FAKE_HPO_ID, True)
        # achilles.load_analyses(FAKE_HPO_ID)
        achilles.run_analyses(hpo_id=FAKE_HPO_ID)
        result = bq_utils.query('SELECT * FROM %sachilles_results WHERE analysis_id = 0' % achilles.PREFIX_PLACEHOLDER)
        result = bq_utils.query('SELECT * FROM %sachilles_results WHERE analysis_id = 2003' % achilles.PREFIX_PLACEHOLDER)

    def tearDown(self):
        self.testbed.deactivate()
