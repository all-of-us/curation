import unittest
from unittest import mock

import bq_utils
from validation import ehr_union as eu


class EhrUnionTest(unittest.TestCase):
    FAKE_SITE_1 = 'fake_site_1'
    FAKE_SITE_2 = 'fake_site_2'

    @classmethod
    def setUpClass(cls):
        print(
            '\n**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.hpo_ids = [self.FAKE_SITE_1, self.FAKE_SITE_2]

    @mock.patch('bq_utils.list_all_table_ids')
    def test_mapping_subqueries(self, mock_list_all_table_ids):
        """
        Verify the query for loading mapping tables. A constant value should be added to
        destination key fields in all tables except for person where the values in 
        the src_person_id and person_id fields should be equal.
        
        :param mock_list_all_table_ids: simulate tables being returned
        """
        # patch list_all_table_ids so that
        # for FAKE_SITE_1 and FAKE_SITE_2
        #   it returns both of their person, visit_occurrence and pii_name tables
        # for FAKE_SITE_1 only
        #   it returns the condition_occurrence table
        tables = ['person', 'visit_occurrence', 'pii_name']
        fake_table_ids = [
            bq_utils.get_table_id(hpo_id, table)
            for hpo_id in self.hpo_ids
            for table in tables
        ]
        fake_table_ids.append(
            bq_utils.get_table_id(self.FAKE_SITE_1, 'condition_occurrence'))
        mock_list_all_table_ids.return_value = fake_table_ids
        hpo_count = len(self.hpo_ids)

        # offset is added to the visit_occurrence destination key field
        actual = eu._mapping_subqueries('visit_occurrence', self.hpo_ids,
                                        'fake_dataset', 'fake_project')
        hpo_unique_identifiers = eu.get_hpo_offsets(self.hpo_ids)
        self.assertEqual(hpo_count, len(actual))
        for i in range(0, hpo_count):
            hpo_id = self.hpo_ids[i]
            subquery = actual[i]
            hpo_table = bq_utils.get_table_id(hpo_id, 'visit_occurrence')
            hpo_offset = hpo_unique_identifiers[hpo_id]
            self.assertIn(f"'{hpo_table}' AS src_table_id", subquery)
            self.assertIn('visit_occurrence_id AS src_visit_occurrence_id',
                          subquery)
            self.assertIn(
                f'visit_occurrence_id + {hpo_offset} AS visit_occurrence_id',
                subquery)

        # src_person_id and person_id fields both use participant ID value
        # (offset is NOT added to the value)
        actual = eu._mapping_subqueries('person', self.hpo_ids, 'fake_dataset',
                                        'fake_project')
        self.assertEqual(hpo_count, len(actual))
        for i in range(0, hpo_count):
            hpo_id = self.hpo_ids[i]
            subquery = actual[i]
            hpo_table = bq_utils.get_table_id(hpo_id, 'person')
            self.assertIn(f"'{hpo_table}' AS src_table_id", subquery)
            self.assertIn('person_id AS src_person_id', subquery)
            self.assertIn('person_id AS person_id', subquery)

        # only return queries for tables that exist
        actual = eu._mapping_subqueries('condition_occurrence', self.hpo_ids,
                                        'fake_dataset', 'fake_project')
        self.assertEqual(1, len(actual))
        subquery = actual[0]
        hpo_table = bq_utils.get_table_id(self.FAKE_SITE_1,
                                          'condition_occurrence')
        hpo_offset = hpo_unique_identifiers[self.FAKE_SITE_1]
        self.assertIn(f"'{hpo_table}' AS src_table_id", subquery)
        self.assertIn('condition_occurrence_id AS src_condition_occurrence_id',
                      subquery)
        self.assertIn(
            f'condition_occurrence_id + {hpo_offset} AS condition_occurrence_id',
            subquery)

    @mock.patch('bq_utils.list_all_table_ids')
    def test_mapping_query(self, mock_list_all_table_ids):
        mock_list_all_table_ids.return_value = [
            f'{self.FAKE_SITE_1}_measurement', f'{self.FAKE_SITE_2}_measurement'
        ]
        dataset_id = 'fake_dataset'
        project_id = 'fake_project'
        table = 'measurement'
        query = eu.mapping_query(table, self.hpo_ids, dataset_id, project_id)
        # testing the query string
        expected_query = f'''
            WITH all_measurement AS (
    
    (SELECT '{self.FAKE_SITE_1}_measurement' AS src_table_id,
      measurement_id AS src_measurement_id,
      measurement_id + 3000000000000000 AS measurement_id
      FROM `{project_id}.{dataset_id}.{self.FAKE_SITE_1}_measurement`)
    

        UNION ALL


    (SELECT '{self.FAKE_SITE_2}_measurement' AS src_table_id,
      measurement_id AS src_measurement_id,
      measurement_id + 4000000000000000 AS measurement_id
      FROM `{project_id}.{dataset_id}.{self.FAKE_SITE_2}_measurement`)
    
    )
    SELECT DISTINCT
        src_table_id,
        src_measurement_id,
        measurement_id,
        SUBSTR(src_table_id, 1, STRPOS(src_table_id, "_measurement")-1) AS src_hpo_id,
        '{dataset_id}' as src_dataset_id
    FROM all_measurement
    '''.format(dataset_id=dataset_id, project_id=project_id)
        self.assertEqual(
            expected_query.strip(), query.strip(),
            "Mapping query for \n {q} \n to is not as expected".format(q=query))

    def tearDown(self):
        pass
