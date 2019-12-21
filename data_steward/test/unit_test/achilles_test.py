import datetime
import os
import re
import unittest

import mock

import bq_utils
import gcs_utils
import resources
from test.unit_test import test_util
from validation import sql_wrangle
from test.unit_test.test_util import FAKE_HPO_ID
from validation import achilles

# This may change if we strip out unused analyses
ACHILLES_LOOKUP_COUNT = 215
ACHILLES_ANALYSIS_COUNT = 134
ACHILLES_RESULTS_COUNT = 2497
SOURCE_NAME_QUERY = """insert into synpuf_100.achilles_analysis (analysis_id, analysis_name) 
  values (0, 'Source name')"""
TEMP_QUERY_1 = """INTO temp.tempresults
   WITH rawdata  as ( select  p.person_id as person_id, min(EXTRACT(YEAR from observation_period_start_date)) - 
   p.year_of_birth  as age_value   from  synpuf_100.person p
  join synpuf_100.observation_period op on p.person_id = op.person_id
   group by  p.person_id, p.year_of_birth
 ), overallstats  as (select  cast(avg(1.0 * age_value)  as float64)  as avg_value, cast(STDDEV(age_value)  as 
 float64)  as stdev_value, min(age_value)  as min_value, max(age_value)  as max_value, COUNT(*)  as total  from  rawdata
), agestats  as ( select  age_value as age_value, COUNT(*)  as total, row_number() over (order by age_value)  as rn   
from  rawdata
   group by  1 ), agestatsprior  as ( select  s.age_value as age_value, s.total as total, sum(p.total)  as 
   accumulated   from  agestats s
  join agestats p on p.rn <= s.rn
   group by  s.age_value, s.total, s.rn
 )
 select  103 as analysis_id, o.total as count_value, o.min_value, o.max_value, o.avg_value, o.stdev_value, 
 min(case when p.accumulated >= .50 * o.total then age_value end) as median_value, min(case when p.accumulated >= .10 
 * o.total then age_value end) as p10_value, min(case when p.accumulated >= .25 * o.total then age_value end) as 
 p25_value, min(case when p.accumulated >= .75 * o.total then age_value end) as p75_value, min(case when 
 p.accumulated >= .90 * o.total then age_value end) as p90_value
  FROM  agestatsprior p
cross join overallstats o
 group by  o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value"""
TEMP_QUERY_2 = """INTO temp.rawdata_1006
  SELECT ce.condition_concept_id as subject_id, p1.gender_concept_id, ce.condition_start_year - p1.year_of_birth as 
  count_value
  FROM  synpuf_100.person p1
inner join
(
   select  person_id, condition_concept_id, min(EXTRACT(YEAR from condition_era_start_date)) as condition_start_year
    from  synpuf_100.condition_era
   group by  1, 2 ) ce on p1.person_id = ce.person_id"""


@unittest.skipIf(os.getenv('ALL_TESTS') == 'False', 'Skipping AchillesTest cases')
class AchillesTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.hpo_bucket = gcs_utils.get_hpo_bucket(test_util.FAKE_HPO_ID)
        test_util.empty_bucket(self.hpo_bucket)
        test_util.delete_all_tables(bq_utils.get_dataset_id())

    @staticmethod
    def _get_timestamp_nums():
        timestamp = str(datetime.datetime.now().time()).split('.')[0]
        time_nums = re.sub('\D', '', timestamp)
        return time_nums

    def _load_dataset(self, hpo_id):
        for cdm_table in resources.CDM_TABLES:
            cdm_file_name = os.path.join(test_util.FIVE_PERSONS_PATH, cdm_table + '.csv')
            if os.path.exists(cdm_file_name):
                test_util.write_cloud_file(self.hpo_bucket, cdm_file_name)
            else:
                test_util.write_cloud_str(self.hpo_bucket, cdm_table + '.csv', 'dummy\n')
            bq_utils.load_cdm_csv(hpo_id, cdm_table)

    def test_detect_commented_block(self):
        commented_command = """
--1300- ORGANIZATION

--NOT APPLICABLE IN CDMV5
--insert into fake_ACHILLES_analysis (analysis_id, analysis_name, stratum_1_name)
--	values (1300, 'Number of organizations by place of service', 'place_of_service_concept_id')"""
        self.assertFalse(sql_wrangle.is_active_command(commented_command))

    def test_load_analyses(self):
        achilles.create_tables(FAKE_HPO_ID, True)
        achilles.load_analyses(FAKE_HPO_ID)
        cmd = sql_wrangle.qualify_tables(
            'SELECT DISTINCT(analysis_id) FROM %sachilles_analysis' % sql_wrangle.PREFIX_PLACEHOLDER,
            FAKE_HPO_ID)
        result = bq_utils.query(cmd)
        self.assertEqual(ACHILLES_LOOKUP_COUNT, int(result['totalRows']))

    def test_get_run_analysis_commands(self):
        cmd_iter = achilles._get_run_analysis_commands(FAKE_HPO_ID)
        commands = [achilles.convert_insert_to_append(command)[0] for command in cmd_iter]
        self.assertEqual(len(commands), ACHILLES_ANALYSIS_COUNT)

    def test_temp_table(self):
        self.assertTrue(sql_wrangle.is_to_temp_table(TEMP_QUERY_1))
        self.assertTrue(sql_wrangle.is_to_temp_table(TEMP_QUERY_2))
        self.assertFalse(sql_wrangle.is_to_temp_table(SOURCE_NAME_QUERY))
        self.assertEqual(sql_wrangle.get_temp_table_name(TEMP_QUERY_1), 'temp.tempresults')
        self.assertEqual(sql_wrangle.get_temp_table_name(TEMP_QUERY_2), 'temp.rawdata_1006')
        self.assertTrue(sql_wrangle.get_temp_table_query(TEMP_QUERY_1).startswith('WITH rawdata'))
        self.assertTrue(
            sql_wrangle.get_temp_table_query(TEMP_QUERY_2).startswith('SELECT ce.condition_concept_id'))

    @staticmethod
    def get_mock_hpo_bucket():
        bucket_env = 'BUCKET_NAME_' + FAKE_HPO_ID.upper()
        hpo_bucket_name = os.getenv(bucket_env)
        if hpo_bucket_name is None:
            raise EnvironmentError()
        return hpo_bucket_name

    @mock.patch('gcs_utils.get_hpo_bucket')
    def test_run_analyses(self, mock_hpo_bucket):
        # Long-running test
        mock_hpo_bucket.return_value = self.get_mock_hpo_bucket()
        timestamp_nums = self._get_timestamp_nums()
        timestamped_hpo_id = FAKE_HPO_ID + '_' + timestamp_nums
        self._load_dataset(timestamped_hpo_id)
        achilles.create_tables(timestamped_hpo_id, True)
        achilles.load_analyses(timestamped_hpo_id)
        achilles.run_analyses(hpo_id=timestamped_hpo_id)
        achilles_results_table = timestamped_hpo_id + '_' + achilles.ACHILLES_RESULTS
        cmd = sql_wrangle.qualify_tables(
            'SELECT COUNT(1) as num_rows FROM %s' % achilles_results_table)
        response = bq_utils.query(cmd)
        rows = bq_utils.response2rows(response)
        self.assertEqual(rows[0]['num_rows'], ACHILLES_RESULTS_COUNT)

    def test_parse_temp(self):
        commands = achilles._get_run_analysis_commands(FAKE_HPO_ID)
        for command in commands:
            is_temp = sql_wrangle.is_to_temp_table(command)
            self.assertFalse(is_temp)

    def tearDown(self):
        test_util.delete_all_tables(bq_utils.get_dataset_id())
        test_util.empty_bucket(self.hpo_bucket)
