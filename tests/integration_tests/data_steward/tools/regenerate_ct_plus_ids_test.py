"""
Integration test for the CT+ person domain fact_relationship remap.

Runs load() and assert_person_linkage_survived_the_remap() against real tables, so
the linkage rows are checked end to end rather than by the rendered SQL.
"""
# Python imports
import os
from unittest import TestCase

# Project imports
import app_identity
from common import FACT_RELATIONSHIP, JINJA_ENV, PERSON
from gcloud.bq import BigQueryClient
from tools import regenerate_ct_plus_ids as ct

PIPELINE_DATASET_ID = 'fake_pipeline'
CT_PLUS_IDS_VIEW = 'fake_ids_view'

# CT keyed ids. 101/102 and 104/105 are linked pairs in input person. 902 is in the
# ids view, so it has a _mapping_person row, but not in input person. 105 is in input
# person but not in the ids view, so it has no _mapping_person row.
INPUT_PERSON_IDS = [101, 102, 103, 104, 105]
PERSON_MAPPING = {101: 5101, 102: 5102, 103: 5103, 104: 5104, 902: 5902}

CREATE_MAPPING = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project_id}}.{{dataset_id}}.{{mapping_table}}` (
    src_table_id STRING,
    src_{{domain_table}}_id INT64,
    {{domain_table}}_id INT64
)
""")

INSERT_PERSON = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{dataset_id}}.person`
    (person_id, gender_concept_id, year_of_birth, race_concept_id, ethnicity_concept_id)
VALUES
{%- for person_id in person_ids %}
    ({{person_id}}, 0, 2020, 0, 0){{ ',' if not loop.last }}
{%- endfor %}
""")

INSERT_PERSON_MAPPING = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{dataset_id}}._mapping_person`
    (src_table_id, src_person_id, person_id)
VALUES
{%- for src, dest in mapping.items() %}
    ('{{src_table_id}}', {{src}}, {{dest}}){{ ',' if not loop.last }}
{%- endfor %}
""")

INSERT_LINKAGE = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{dataset_id}}.fact_relationship`
    (domain_concept_id_1, fact_id_1, domain_concept_id_2, fact_id_2, relationship_concept_id)
VALUES
{%- for fact_id_1, fact_id_2 in pairs %}
    (56, {{fact_id_1}}, 56, {{fact_id_2}}, 4053608){{ ',' if not loop.last }}
{%- endfor %}
""")


class RegenerateCtPlusIdsTest(TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = app_identity.get_application_id()
        self.input_dataset_id = os.environ.get('COMBINED_DATASET_ID')
        self.output_dataset_id = os.environ.get('UNIONED_DATASET_ID')
        self.client = BigQueryClient(self.project_id)

        self.mapping_tables = [
            ct.mapping_table_for(domain_table)
            for domain_table in ct.DOMAIN_CONCEPT_ID_TO_TABLE.values()
        ]
        self.fq_input_tables = [
            f'{self.project_id}.{self.input_dataset_id}.{table}'
            for table in [PERSON, FACT_RELATIONSHIP] + self.mapping_tables
        ]
        self.fq_output = (f'{self.project_id}.{self.output_dataset_id}.'
                          f'{FACT_RELATIONSHIP}')

        self.tearDown()
        self.client.create_tables(self.fq_input_tables[:2])
        for domain_table in ct.DOMAIN_CONCEPT_ID_TO_TABLE.values():
            self._run(
                CREATE_MAPPING.render(
                    project_id=self.project_id,
                    dataset_id=self.input_dataset_id,
                    mapping_table=ct.mapping_table_for(domain_table),
                    domain_table=domain_table))
        self._run(
            INSERT_PERSON.render(project_id=self.project_id,
                                 dataset_id=self.input_dataset_id,
                                 person_ids=INPUT_PERSON_IDS))
        self._run(
            INSERT_PERSON_MAPPING.render(
                project_id=self.project_id,
                dataset_id=self.input_dataset_id,
                mapping=PERSON_MAPPING,
                src_table_id=ct.person_mapping_src_table_id(
                    PIPELINE_DATASET_ID, CT_PLUS_IDS_VIEW)))

    def _run(self, q):
        return list(self.client.query(q).result())

    def _insert_linkage(self, pairs):
        self._run(
            INSERT_LINKAGE.render(project_id=self.project_id,
                                  dataset_id=self.input_dataset_id,
                                  pairs=pairs))

    def _remap(self):
        """One run of the load path main() takes for fact_relationship."""
        self.client.delete_table(self.fq_output, not_found_ok=True)
        self.client.create_tables([self.fq_output])
        ct.load(self.client, FACT_RELATIONSHIP, self.input_dataset_id,
                self.output_dataset_id, self.project_id, PIPELINE_DATASET_ID,
                CT_PLUS_IDS_VIEW, self.input_dataset_id,
                ct.DEFAULT_MAPPING_NAMESPACE)
        rows = self._run(f'SELECT fact_id_1, fact_id_2 FROM `{self.fq_output}`')
        return sorted((row['fact_id_1'], row['fact_id_2']) for row in rows)

    def _survived(self):
        return ct.assert_person_linkage_survived_the_remap(
            self.client, self.project_id, self.input_dataset_id,
            self.output_dataset_id)

    def test_linked_pair_is_rekeyed_both_ways_and_absent_adult_is_dropped(self):
        """
        Both directions of 101/102 survive on their CT+ ids. Both directions of
        103/902 are dropped and counted: 902 has a CT+ id but no person row, so the
        link would dangle. Two runs give the same rows.
        """
        self._insert_linkage([(101, 102), (102, 101), (103, 902), (902, 103)])
        input_count = self._run(
            f'SELECT COUNT(*) AS n FROM `{self.fq_input_tables[1]}`')[0]['n']
        self.assertEqual(input_count, 4)

        first = self._remap()

        self.assertEqual(first, [(5101, 5102), (5102, 5101)])
        ct_ids = set(INPUT_PERSON_IDS) | set(PERSON_MAPPING)
        self.assertFalse({fact_id for pair in first for fact_id in pair} &
                         ct_ids)
        self.assertEqual(self._survived(), 2)
        self.assertEqual(self._remap(), first)

    def test_linked_person_with_no_mapping_stops_the_run(self):
        """105 is in input person with no CT+ id, so 104/105 is dropped silently by
        the remap and the guard has to stop the run."""
        self._insert_linkage([(101, 102), (104, 105)])

        self.assertEqual(self._remap(), [(5101, 5102)])
        with self.assertRaises(RuntimeError) as ctx:
            self._survived()

        self.assertIn('kept 1 of 2', str(ctx.exception))

    def tearDown(self):
        for fq_table in self.fq_input_tables + [self.fq_output]:
            self.client.delete_table(fq_table, not_found_ok=True)
