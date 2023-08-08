# Python imports
import os
import unittest

# Third party imports

# Project imports
from validation import sql_wrangle

# This may change if we strip out unused analyses


class SQLWrangleTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print(
            '\n**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.source_name_query = (
            'insert into synpuf_100.achilles_analysis (analysis_id, analysis_name) '
            'values (0, \'Source name\')')
        self.query_1 = (
            'INTO temp.tempresults '
            'WITH rawdata as ( '
            'select p.person_id as person_id, min(EXTRACT(YEAR from observation_period_start_date)) - '
            'p.year_of_birth as age_value '
            'from  synpuf_100.person p '
            'join synpuf_100.observation_period op '
            'on p.person_id = op.person_id '
            'group by  p.person_id, p.year_of_birth '
            '), '
            'overallstats as ('
            'select cast(avg(1.0 * age_value) as float64) as avg_value, '
            'cast(STDDEV(age_value) as float64) as stdev_value, '
            'min(age_value) as min_value, '
            'max(age_value) as max_value, '
            'COUNT(*) as total from rawdata '
            '), '
            'agestats as ('
            'select age_value as age_value, '
            'COUNT(*) as total, '
            'row_number() over (order by age_value) as rn '
            'from  rawdata '
            'group by  1 '
            '), '
            'agestatsprior as ( '
            'select s.age_value as age_value, '
            's.total as total, '
            'sum(p.total)  as accumulated '
            'from agestats s '
            'join agestats p '
            'on p.rn <= s.rn '
            'group by s.age_value, s.total, s.rn '
            ') '
            'select 103 as analysis_id, '
            'o.total as count_value, '
            'o.min_value, '
            'o.max_value, '
            'o.avg_value, '
            'o.stdev_value, '
            'min('
            'case '
            'when p.accumulated >= .50 * o.total then age_value end'
            ') as median_value, '
            'min('
            'case '
            'when p.accumulated >= .10 * o.total then age_value end'
            ') as p10_value, '
            'min('
            'case '
            'when p.accumulated >= .25 * o.total then age_value end'
            ') as p25_value, '
            'min('
            'case '
            'when p.accumulated >= .75 * o.total then age_value end'
            ') as p75_value, '
            'min('
            'case '
            'when p.accumulated >= .90 * o.total then age_value end'
            ') as p90_value '
            'FROM  agestatsprior p '
            'cross join overallstats o '
            'group by  o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value'
        )
        self.query_2 = (
            'INTO temp.rawdata_1006 '
            'SELECT '
            'ce.condition_concept_id as subject_id, '
            'p1.gender_concept_id, '
            'ce.condition_start_year - p1.year_of_birth as count_value '
            'FROM synpuf_100.person p1'
            'inner join '
            '('
            'select '
            'person_id, '
            'condition_concept_id, '
            'min(EXTRACT(YEAR from condition_era_start_date)) as condition_start_year '
            'from synpuf_100.condition_era'
            'group by  1, 2 ) AS ce '
            'on p1.person_id = ce.person_id')

    def test_detect_commented_block_by_block_comments(self):
        # pre conditions
        command = [
            '/*', '1300- ORGANIZATION', '', 'NOT APPLICABLE IN CDMV5',
            'insert into fake_ACHILLES_analysis (analysis_id, analysis_name, stratum_1_name)',
            'values (1300, \'Number of organizations by place of service\', \'place_of_service_concept_id\')',
            '*/'
        ]
        commented_command = '\n'.join(command)

        uncommented_command = command
        uncommented_command[0] = '  --  /*'
        uncommented_command = '\n'.join(uncommented_command)

        # test
        self.assertTrue(sql_wrangle.is_commented_block(commented_command))
        self.assertFalse(sql_wrangle.is_commented_block(uncommented_command))

    def test_detect_commented_block_by_line_comments(self):
        # pre conditions
        commented_command = [
            '--1300- ORGANIZATION', '', '--NOT APPLICABLE IN CDMV5',
            '--insert into fake_ACHILLES_analysis (analysis_id, analysis_name, stratum_1_name)',
            "--	values (1300, 'Number of organizations by place of service', 'place_of_service_concept_id')"
        ]
        commented_command = '\n'.join(commented_command)

        # test
        self.assertTrue(sql_wrangle.is_commented_block(commented_command))

    def test_is_to_temp_table_uppercase(self):
        # preconditions
        query_1 = self.query_1.upper()
        query_2 = self.query_2.upper()
        source_query = self.source_name_query.upper()

        # test
        self.assertTrue(
            sql_wrangle.is_to_temp_table(query_1),
            "failed with upper case.  function is not case insensitive")
        self.assertTrue(
            sql_wrangle.is_to_temp_table(query_2),
            "failed with upper case.  function is not case insensitive")
        self.assertFalse(
            sql_wrangle.is_to_temp_table(source_query),
            "failed with upper case.  function is not case insensitive")

    def test_is_to_temp_table_lowercase(self):
        # preconditions
        query_1 = self.query_1.lower()
        query_2 = self.query_2.lower()
        source_query = self.source_name_query.lower()

        # test
        self.assertTrue(
            sql_wrangle.is_to_temp_table(query_1),
            "failed with lower case.  function is not case insensitive")
        self.assertTrue(
            sql_wrangle.is_to_temp_table(query_2),
            "failed with lower case.  function is not case insensitive")
        self.assertFalse(
            sql_wrangle.is_to_temp_table(source_query),
            "failed with lower case.  function is not case insensitive")

    def test_get_temp_table_name(self):
        self.assertEqual(sql_wrangle.get_temp_table_name(self.query_1),
                         'temp.tempresults')
        self.assertEqual(sql_wrangle.get_temp_table_name(self.query_2),
                         'temp.rawdata_1006')

    def test_get_temp_table_query(self):
        self.assertTrue(
            sql_wrangle.get_temp_table_query(
                self.query_1).startswith('WITH rawdata'))
        self.assertTrue(
            sql_wrangle.get_temp_table_query(
                self.query_2).startswith('SELECT ce.condition_concept_id'))

    def test_qualify_tables(self):
        r = sql_wrangle.qualify_tables('temp.some_table', hpo_id='fake')
        self.assertEqual(r, 'fake_temp_some_table')

        r = sql_wrangle.qualify_tables('synpuf_100.achilles_results',
                                       hpo_id='fake')
        self.assertEqual(r, 'fake_achilles_results')

        r = sql_wrangle.qualify_tables('temp.some_table', hpo_id='pitt_temple')
        self.assertEqual(r, 'pitt_temple_temp_some_table')

        r = sql_wrangle.qualify_tables('synpuf_100.achilles_results',
                                       hpo_id='pitt_temple')
        self.assertEqual(r, 'pitt_temple_achilles_results')

        # For HPO sites in EHR dataset, death table exists.
        r = sql_wrangle.qualify_tables(
            'SELECT * FROM synpuf_100.death WHERE 1=1', hpo_id='fake')
        self.assertEqual(r, 'SELECT * FROM fake_death WHERE 1=1')

        # For unioned_ehr in EHR dataset, death table does not exist.
        # Use aou_death instead.
        r = sql_wrangle.qualify_tables(
            'SELECT * FROM synpuf_100.death WHERE 1=1', hpo_id='unioned_ehr')
        self.assertEqual(r, 'SELECT * FROM unioned_ehr_aou_death WHERE 1=1')

        # For non-EHR, death table exists but is empty.
        # Use aou_death instead.
        r = sql_wrangle.qualify_tables(
            'SELECT * FROM synpuf_100.death WHERE 1=1')
        self.assertEqual(r, 'SELECT * FROM aou_death WHERE 1=1')

    def tearDown(self):
        pass
