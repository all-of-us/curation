import unittest
from unittest import mock

import bq_utils
from validation import ehr_union as eu
from constants.validation import ehr_union as eu_constants

MOVE_PER_OBS_QRY = '''
        SELECT
            CASE observation_concept_id
                WHEN {gender_concept_id} THEN pto.person_id + {pto_offset} + {gender_offset} + {hpo_offset} * hpo.Display_Order
                WHEN {race_concept_id} THEN pto.person_id + {pto_offset} + {race_offset} + {hpo_offset} * hpo.Display_Order
                WHEN {dob_concept_id} THEN pto.person_id + {pto_offset} + {dob_offset} + {hpo_offset} * hpo.Display_Order 
                WHEN {ethnicity_concept_id} THEN pto.person_id + {pto_offset} + {ethnicity_offset} + {hpo_offset} * hpo.Display_Order
            END AS observation_id,
            pto.person_id,
            observation_concept_id,
            EXTRACT(DATE FROM observation_datetime) as observation_date,
            observation_type_concept_id,
            observation_datetime,
            CAST(NULL AS FLOAT64) as value_as_number,
            value_as_concept_id,
            CAST(value_as_string AS STRING) as value_as_string,
            observation_source_value,
            observation_source_concept_id,
            NULL as qualifier_concept_id,
            NULL as unit_concept_id,
            NULL as provider_id,
            NULL as visit_occurrence_id,
            CAST(NULL AS STRING) as unit_source_value,
            CAST(NULL AS STRING) as qualifier_source_value,
            NULL as value_source_concept_id,
            CAST(NULL AS STRING) as value_source_value,
            NULL as questionnaire_response_id
        FROM
            ({person_to_obs_query}
            ORDER BY person_id) AS pto
            JOIN
            `{output_dataset_id}._mapping_person` AS mp
            ON pto.person_id = mp.src_person_id
            JOIN
            `lookup_tables.hpo_site_id_mappings` AS hpo
            ON LOWER(hpo.HPO_ID) = mp.src_hpo_id
        '''

MAP_PER_OBS_QRY = '''
        SELECT
            mp.src_table_id AS src_table_id,
            CASE observation_concept_id
                WHEN {gender_concept_id} THEN pto.person_id + {pto_offset} + {gender_offset} + {hpo_offset} * hpo.Display_Order
                WHEN {race_concept_id} THEN pto.person_id + {pto_offset} + {race_offset} + {hpo_offset} * hpo.Display_Order
                WHEN {dob_concept_id} THEN pto.person_id + {pto_offset} + {dob_offset} + {hpo_offset} * hpo.Display_Order 
                WHEN {ethnicity_concept_id} THEN pto.person_id + {pto_offset} + {ethnicity_offset} + {hpo_offset} * hpo.Display_Order
            END AS observation_id,
            pto.person_id AS src_observation_id,
            mp.src_hpo_id AS src_hpo_id
        FROM
            ({person_to_obs_query}) AS pto
            JOIN
            `{output_dataset_id}._mapping_person` AS mp
            ON pto.person_id = mp.src_person_id
            JOIN
            `lookup_tables.hpo_site_id_mappings` AS hpo
            ON LOWER(hpo.HPO_ID) = mp.src_hpo_id            
        '''


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

    @mock.patch('validation.ehr_union.output_table_for')
    @mock.patch('validation.ehr_union.get_person_to_observation_query')
    @mock.patch('validation.ehr_union.query')
    def test_move_ehr_person_to_observation(
        self, mock_query, mock_get_person_to_observation_query,
        mock_output_table_for):
        dataset_id = 'fake_dataset'
        output_table = 'fake_table'

        mock_get_person_to_observation_query.return_value = "SELECT COLUMN FROM TABLE"
        mock_output_table_for.return_value = output_table

        expected_q = MOVE_PER_OBS_QRY.format(
            output_dataset_id=dataset_id,
            pto_offset=2000000000000000,
            gender_concept_id=4135376,
            gender_offset=100000000000000,
            race_concept_id=4013886,
            race_offset=200000000000000,
            dob_concept_id=4083587,
            dob_offset=300000000000000,
            ethnicity_concept_id=4271761,
            ethnicity_offset=400000000000000,
            hpo_offset=100000000000,
            person_to_obs_query="SELECT COLUMN FROM TABLE")

        #Make sure queries are the same
        eu.move_ehr_person_to_observation(dataset_id)
        mock_query.assert_called_with(expected_q,
                                      output_table,
                                      dataset_id,
                                      write_disposition='WRITE_APPEND')

    @mock.patch('validation.ehr_union.mapping_table_for')
    @mock.patch('validation.ehr_union.get_person_to_observation_query')
    @mock.patch('validation.ehr_union.query')
    def test_map_ehr_person_to_observation(self, mock_query,
                                           mock_get_person_to_observation_query,
                                           mock_mapping_table_for):
        dataset_id = 'fake_dataset'
        mapping_table = 'fake_table'

        mock_get_person_to_observation_query.return_value = "SELECT COLUMN FROM TABLE"
        mock_mapping_table_for.return_value = mapping_table

        expected_q = MAP_PER_OBS_QRY.format(
            output_dataset_id=dataset_id,
            pto_offset=2000000000000000,
            gender_concept_id=4135376,
            gender_offset=100000000000000,
            race_concept_id=4013886,
            race_offset=200000000000000,
            dob_concept_id=4083587,
            dob_offset=300000000000000,
            ethnicity_concept_id=4271761,
            ethnicity_offset=400000000000000,
            hpo_offset=100000000000,
            person_to_obs_query="SELECT COLUMN FROM TABLE")

        #Make sure queries are the same
        eu.map_ehr_person_to_observation(dataset_id)
        mock_query.assert_called_with(expected_q,
                                      mapping_table,
                                      dataset_id,
                                      write_disposition='WRITE_APPEND')

    def tearDown(self):
        pass
