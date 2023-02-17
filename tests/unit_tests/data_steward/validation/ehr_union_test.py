# Python Imports
import unittest
from unittest import mock
from unittest.mock import ANY

# Project imports
import resources
from validation import ehr_union as eu

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
        self.mock_bq_client_patcher = mock.patch(
            'validation.ehr_union.BigQueryClient')
        self.mock_bq_client = self.mock_bq_client_patcher.start()
        self.addCleanup(self.mock_bq_client_patcher.stop)

    @mock.patch('bq_utils.get_hpo_info')
    def test_mapping_subqueries(self, mock_hpo_info):
        """
        Verify the query for loading mapping tables. A constant value should be added to
        destination key fields in all tables except for person where the values in 
        the src_person_id and person_id fields should be equal.
        
        :param mock_hpo_info: simulate hpo_info being returned
        """

        mock_hpo_info.return_value = [{
            'hpo_id': hpo_id
        } for hpo_id in self.hpo_ids]
        tables = ['person', 'visit_occurrence', 'pii_name']
        fake_table_ids = [
            resources.get_table_id(table, hpo_id=hpo_id)
            for hpo_id in self.hpo_ids
            for table in tables
        ]
        fake_table_ids.append(
            resources.get_table_id('condition_occurrence',
                                   hpo_id=self.FAKE_SITE_1))

        mock_table_obj = mock.MagicMock()
        type(mock_table_obj).table_id = mock.PropertyMock(
            side_effect=fake_table_ids)
        mock_fake_tables = [mock_table_obj] * 7
        self.mock_bq_client.list_tables.return_value = mock_fake_tables

        hpo_count = len(self.hpo_ids)

        # offset is added to the visit_occurrence destination key field
        actual = eu._mapping_subqueries(self.mock_bq_client, 'visit_occurrence',
                                        self.hpo_ids, 'fake_dataset',
                                        'fake_project')
        hpo_unique_identifiers = eu.get_hpo_offsets(self.hpo_ids)
        self.assertEqual(hpo_count, len(actual))
        for i in range(0, hpo_count):
            hpo_id = self.hpo_ids[i]
            subquery = actual[i]
            hpo_table = resources.get_table_id('visit_occurrence',
                                               hpo_id=hpo_id)
            hpo_offset = hpo_unique_identifiers[hpo_id]
            self.assertIn(f"'{hpo_table}' AS src_table_id", subquery)
            self.assertIn('visit_occurrence_id AS src_visit_occurrence_id',
                          subquery)
            self.assertIn(
                f'visit_occurrence_id + {hpo_offset} AS visit_occurrence_id',
                subquery)

        # src_person_id and person_id fields both use participant ID value
        # (offset is NOT added to the value)
        type(mock_table_obj).table_id = mock.PropertyMock(
            side_effect=fake_table_ids)
        actual = eu._mapping_subqueries(self.mock_bq_client, 'person',
                                        self.hpo_ids, 'fake_dataset',
                                        'fake_project')
        self.assertEqual(hpo_count, len(actual))
        for i in range(0, hpo_count):
            hpo_id = self.hpo_ids[i]
            subquery = actual[i]
            hpo_table = resources.get_table_id('person', hpo_id=hpo_id)
            self.assertIn(f"'{hpo_table}' AS src_table_id", subquery)
            self.assertIn('person_id AS src_person_id', subquery)
            self.assertIn('person_id AS person_id', subquery)

        # only return queries for tables that exist
        type(mock_table_obj).table_id = mock.PropertyMock(
            side_effect=fake_table_ids)
        actual = eu._mapping_subqueries(self.mock_bq_client,
                                        'condition_occurrence', self.hpo_ids,
                                        'fake_dataset', 'fake_project')
        self.assertEqual(1, len(actual))
        subquery = actual[0]
        hpo_table = resources.get_table_id('condition_occurrence',
                                           hpo_id=self.FAKE_SITE_1)
        hpo_offset = hpo_unique_identifiers[self.FAKE_SITE_1]
        self.assertIn(f"'{hpo_table}' AS src_table_id", subquery)
        self.assertIn('condition_occurrence_id AS src_condition_occurrence_id',
                      subquery)
        self.assertIn(
            f'condition_occurrence_id + {hpo_offset} AS condition_occurrence_id',
            subquery)

    @mock.patch('bq_utils.get_hpo_info')
    def test_mapping_query(self, mock_hpo_info):
        mock_hpo_info.return_value = [{
            'hpo_id': hpo_id
        } for hpo_id in self.hpo_ids]

        fake_table_ids = [
            f'{self.FAKE_SITE_1}_measurement', f'{self.FAKE_SITE_2}_measurement'
        ]
        mock_table_obj = mock.MagicMock()
        type(mock_table_obj).table_id = mock.PropertyMock(
            side_effect=fake_table_ids)
        mock_fake_tables = [mock_table_obj] * 2
        self.mock_bq_client.list_tables.return_value = mock_fake_tables

        dataset_id = 'fake_dataset'
        project_id = 'fake_project'
        table = 'measurement'
        query = eu.mapping_query(self.mock_bq_client, table, self.hpo_ids,
                                 dataset_id, project_id)
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

    @mock.patch('bq_utils.get_hpo_info')
    @mock.patch('validation.ehr_union.output_table_for')
    @mock.patch('validation.ehr_union.get_person_to_observation_query')
    @mock.patch('validation.ehr_union.query')
    def test_move_ehr_person_to_observation(
        self, mock_query, mock_get_person_to_observation_query,
        mock_output_table_for, mock_hpo_info):
        dataset_id = 'fake_dataset'
        output_table = 'fake_table'

        mock_hpo_info.return_value = [{
            'hpo_id': hpo_id
        } for hpo_id in self.hpo_ids]
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

    @mock.patch('bq_utils.get_hpo_info')
    @mock.patch('validation.ehr_union.mapping_table_for')
    @mock.patch('validation.ehr_union.get_person_to_observation_query')
    @mock.patch('validation.ehr_union.query')
    def test_map_ehr_person_to_observation(self, mock_query,
                                           mock_get_person_to_observation_query,
                                           mock_mapping_table_for,
                                           mock_hpo_info):
        dataset_id = 'fake_dataset'
        mapping_table = 'fake_table'
        mock_hpo_info.return_value = [{
            'hpo_id': hpo_id
        } for hpo_id in self.hpo_ids]
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

    @mock.patch('validation.ehr_union.clean_engine.clean_dataset')
    @mock.patch('validation.ehr_union.move_ehr_person_to_observation')
    @mock.patch('validation.ehr_union.map_ehr_person_to_observation')
    @mock.patch('validation.ehr_union.load')
    @mock.patch('validation.ehr_union.mapping')
    @mock.patch('bq_utils.create_standard_table')
    @mock.patch('bq_utils.get_hpo_info')
    def test_excluded_hpo_ids(self, mock_hpo_info, mock_create_std_tbl,
                              mock_mapping, mock_load, mock_map_person,
                              mock_move_person, mock_clean_dataset):
        mock_hpo_info.return_value = [{
            'hpo_id': hpo_id
        } for hpo_id in self.hpo_ids]
        self.mock_bq_client.return_value = 'client'
        eu.main("input_dataset_id",
                "output_dataset_id",
                "project_id",
                hpo_ids_ex=[self.FAKE_SITE_2])
        mock_mapping.assert_called_with(ANY, [self.FAKE_SITE_1],
                                        "input_dataset_id", "output_dataset_id",
                                        "project_id", 'client')

    def tearDown(self):
        pass
