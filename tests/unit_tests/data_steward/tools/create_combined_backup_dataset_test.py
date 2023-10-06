# Python imports
import unittest

# Project imports
import common
import tools.create_combined_backup_dataset as combined_backup
from constants.tools.create_combined_backup_dataset import EHR_CONSENT_TABLE_ID

EXPECTED_MAPPING_QUERY = common.JINJA_ENV.from_string("""
SELECT DISTINCT
    '{{rdr_dataset_id}}' AS src_dataset_id,
    t.{{domain_table}}_id AS src_{{domain_table}}_id,
    v.src_id as src_hpo_id,
    {% if domain_table in ['survey_conduct', 'person'] %}
    t.{{domain_table}}_id AS {{domain_table}}_id,
    {% else %}
    t.{{domain_table}}_id + {{mapping_constant}} AS {{domain_table}}_id,
    {% endif %}
    '{{domain_table}}' as src_table_id
FROM `{{rdr_dataset_id}}.{{domain_table}}` AS t
JOIN `{{rdr_dataset_id}}._mapping_{{domain_table}}` AS v
ON t.{{domain_table}}_id = v.{{domain_table}}_id
{% if domain_table not in ['survey_conduct', 'person'] %}
UNION ALL
SELECT DISTINCT
    '{{ehr_dataset_id}}' AS src_dataset_id,
    t.{{domain_table}}_id AS src_{{domain_table}}_id,
    v.src_hpo_id AS src_hpo_id,
    t.{{domain_table}}_id  AS {{domain_table}}_id,
    '{{domain_table}}' as src_table_id
FROM `{{ehr_dataset_id}}.{{domain_table}}` AS t
JOIN `{{ehr_dataset_id}}._mapping_{{domain_table}}` AS v
ON t.{{domain_table}}_id = v.{{domain_table}}_id
WHERE EXISTS
    (SELECT 1 FROM `{{combined_sandbox_dataset_id}}.{{ehr_consent_table_id}}` AS c
     WHERE t.person_id = c.person_id)
{% endif %}
""")
EXPECTED_SURVEY_CONDUCT_MAPPING_QUERY = common.JINJA_ENV.from_string("""
SELECT DISTINCT
  '{{rdr_dataset_id}}'  AS src_dataset_id,
  t.{{domain_table}}_id  AS src_{{domain_table}}_id,
  v.src_id as src_hpo_id,
  t.{{domain_table}}_id  AS {{domain_table}}_id,
  '{{domain_table}}' as src_table_id
  FROM `{{rdr_dataset_id}}.{{domain_table}}` AS t
    JOIN `{{rdr_dataset_id}}._mapping_{{domain_table}}` AS v
    ON t.{{domain_table}}_id = v.{{domain_table}}_id
""")


class CreateCombinedBackupDatasetTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.ehr_dataset_id = 'ehr_dataset'
        self.rdr_dataset_id = 'rdr_dataset'
        self.combined_dataset_id = 'ehr_rdr_dataset'

    def test_mapping_query(self):
        # pre-condition
        table_name = 'visit_occurrence'

        # test
        q = combined_backup.mapping_query(table_name, self.rdr_dataset_id,
                                          self.ehr_dataset_id,
                                          self.combined_dataset_id)

        # post conditions
        expected_query = EXPECTED_MAPPING_QUERY.render(
            rdr_dataset_id=self.rdr_dataset_id,
            ehr_dataset_id=self.ehr_dataset_id,
            combined_dataset_id=self.combined_dataset_id,
            domain_table=table_name,
            mapping_constant=common.RDR_ID_CONSTANT,
            ehr_consent_table_id=EHR_CONSENT_TABLE_ID,
            combined_sandbox_dataset_id=self.combined_dataset_id)

        # account for spacing differences
        expected_query = ' '.join(expected_query.split())
        mono_spaced_q = ' '.join(q.split())

        self.assertEqual(expected_query, mono_spaced_q)

        table_name = 'survey_conduct'

        # test
        q = combined_backup.mapping_query(table_name, self.rdr_dataset_id,
                                          self.ehr_dataset_id,
                                          self.combined_dataset_id)

        # post conditions
        expected_query = EXPECTED_SURVEY_CONDUCT_MAPPING_QUERY.render(
            rdr_dataset_id=self.rdr_dataset_id,
            ehr_dataset_id=self.ehr_dataset_id,
            combined_dataset_id=self.combined_dataset_id,
            domain_table=table_name,
            mapping_constant=common.RDR_ID_CONSTANT,
            ehr_consent_table_id=EHR_CONSENT_TABLE_ID,
            combined_sandbox_dataset_id=self.combined_dataset_id)

        # account for spacing differences
        expected_query = ' '.join(expected_query.split())
        mono_spaced_q = ' '.join(q.split())

        self.assertEqual(expected_query, mono_spaced_q)
