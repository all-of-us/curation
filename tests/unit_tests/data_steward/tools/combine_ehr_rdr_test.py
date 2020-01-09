# Python imports
import unittest
from mock import mock, patch

# Project imports
import common
import tools.combine_ehr_rdr as combine_ehr_rdr 
from constants.tools.combine_ehr_rdr import EHR_CONSENT_TABLE_ID

EXPECTED_MAPPING_QUERY = ('SELECT DISTINCT'
                          ' \'{rdr_dataset_id}\'  AS src_dataset_id,'
                          '  {domain_table}_id  AS src_{domain_table}_id,'
                          '  \'rdr\' as src_hpo_id,'
                          '  {domain_table}_id + {mapping_constant}  AS {domain_table}_id'
                          '  FROM {rdr_dataset_id}.{domain_table}'
                          ''
                          '  UNION ALL'
                          ''
                          '  SELECT DISTINCT'
                          '  \'{ehr_dataset_id}\'  AS src_dataset_id,'
                          '  t.{domain_table}_id AS src_{domain_table}_id,'
                          '  v.src_hpo_id AS src_hpo_id,'
                          '  t.{domain_table}_id  AS {domain_table}_id'
                          '  FROM {ehr_dataset_id}.{domain_table} t'
                          '  JOIN {ehr_dataset_id}._mapping_{domain_table}  v '
                          '  on t.{domain_table}_id = v.{domain_table}_id'
                          '  WHERE EXISTS'
                          '  (SELECT 1 FROM {combined_dataset_id}.{ehr_consent_table_id} c'
                          '  WHERE t.person_id = c.person_id)')

class CombineEhrRdrTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.ehr_dataset_id = 'ehr_dataset' 
        self.rdr_dataset_id = 'rdr_dataset'
        self.combined_dataset_id = 'ehr_rdr_dataset'

    @patch('tools.combine_ehr_rdr.bq_utils.get_combined_dataset_id')
    @patch('tools.combine_ehr_rdr.bq_utils.get_dataset_id')
    @patch('tools.combine_ehr_rdr.bq_utils.get_rdr_dataset_id')
    def test_mapping_query(self, mock_rdr, mock_ehr, mock_combined):
        # pre-condition
        table_name = 'visit_occurrence'

        mock_rdr.return_value = self.rdr_dataset_id
        mock_ehr.return_value = self.ehr_dataset_id
        mock_combined.return_value = self.combined_dataset_id

        # test
        q = combine_ehr_rdr.mapping_query(table_name)

        # post conditions
        expected_query = EXPECTED_MAPPING_QUERY.format(
            rdr_dataset_id=self.rdr_dataset_id,
            ehr_dataset_id=self.ehr_dataset_id,
            combined_dataset_id=self.combined_dataset_id,
            domain_table=table_name,
            mapping_constant=common.RDR_ID_CONSTANT,
            ehr_consent_table_id=EHR_CONSENT_TABLE_ID
        )

        # account for spacing differences
        expected_query = ' '.join(expected_query.split())
        mono_spaced_q = ' '.join(q.split())

        self.assertEqual(expected_query, mono_spaced_q)
