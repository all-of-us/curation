# Python imports
import unittest

from googleapiclient.errors import HttpError
# Third party imports
from jinja2 import Template

# Project imports
import bq_utils
from cdr_cleaner import clean_cdr_engine
from cdr_cleaner.cleaning_rules import repopulate_person_post_deid
from tests import test_util

# Participant 1: has gender and sex at birth observations
# Participant 2: no gender or sex at birth observations
INSERT_FAKE_PARTICIPANTS_TMPLS = [
    # TODO(calbach): Ideally these tests should not manipulate concept table, not currently hermetic.
    Template("""
INSERT INTO `{{project_id}}.{{dataset_id}}.concept` (concept_id, concept_code)
VALUES
  ({{gender_concept_id}}, "gender"),
  ({{gender_nonbinary_concept_id}}, "nonbinary"),
  ({{gender_nonbinary_source_concept_id}}, "nonbinary_src"),
  ({{sex_at_birth_concept_id}}, "sex"),
  ({{sex_female_concept_id}}, "female"),
  ({{sex_female_source_concept_id}}, "female_src")
"""),
    Template("""
INSERT INTO `{{project_id}}.{{dataset_id}}.person` (person_id)
VALUES (1), (2)
"""),
    Template("""
INSERT INTO `{{project_id}}.{{dataset_id}}.observation` (person_id, observation_id, observation_source_concept_id, value_as_concept_id, value_source_concept_id)
VALUES
  (1, 100, {{gender_concept_id}}, {{gender_nonbinary_concept_id}}, {{gender_nonbinary_source_concept_id}}),
  (1, 101, {{sex_at_birth_concept_id}}, {{sex_female_concept_id}}, {{sex_female_source_concept_id}})
""")
]


class RepopulatePersonPostDeidTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = bq_utils.app_identity.get_application_id()
        self.dataset_id = bq_utils.get_combined_dataset_id()
        if not self.project_id or not self.dataset_id:
            # TODO: Fix handling of globals, push these assertions down if they are required.
            raise ValueError(
                f"missing configuration for project ('{self.project_id}') " +
                f"and/or dataset ('{self.dataset_id}')")

        # TODO: Reconcile this with a consistent integration testing model. Ideally each test should
        # clean up after itself so that we don't need this defensive check.
        test_util.delete_all_tables(self.dataset_id)

        create_tables = ['person', 'observation']
        table_fields = {
            'person': 'post_deid_person',
            'observation': 'observation',
            'concept': 'concept'
        }
        for tbl in ['concept']:
            if not bq_utils.table_exists(tbl, dataset_id=self.dataset_id):
                create_tables.append(tbl)
        for tbl in create_tables:
            bq_utils.create_standard_table(table_fields[tbl],
                                           tbl,
                                           dataset_id=self.dataset_id,
                                           force_all_nullable=True)

    def tearDown(self):
        test_util.delete_all_tables(self.dataset_id)

    def assertPersonFields(self, person, want):
        for k in want.keys():
            self.assertIn(k, person)
            self.assertEqual(person[k], want[k])

    def test_execute_queries(self):
        gender_nonbinary_concept_id = 1585841
        gender_nonbinary_source_concept_id = 123
        sex_female_concept_id = 1585847
        sex_female_source_concept_id = 45878463
        for tmpl in INSERT_FAKE_PARTICIPANTS_TMPLS:
            query = tmpl.render(
                project_id=self.project_id,
                dataset_id=self.dataset_id,
                gender_concept_id=repopulate_person_post_deid.GENDER_CONCEPT_ID,
                gender_nonbinary_concept_id=gender_nonbinary_concept_id,
                gender_nonbinary_source_concept_id=
                gender_nonbinary_source_concept_id,
                sex_at_birth_concept_id=repopulate_person_post_deid.
                SEX_AT_BIRTH_CONCEPT_ID,
                sex_female_concept_id=sex_female_concept_id,
                sex_female_source_concept_id=sex_female_source_concept_id)
            try:
                resp = bq_utils.query(query)
            except HttpError as e:
                self.fail("failed to execute query '{}': {}".format(
                    query, e.content))
            self.assertTrue(resp["jobComplete"])

        clean_cdr_engine.clean_dataset(
            self.project_id, self.dataset_id,
            [(repopulate_person_post_deid.
              get_repopulate_person_post_deid_queries,)])

        rows = bq_utils.response2rows(
            bq_utils.query("SELECT * FROM `{}.{}.person`".format(
                self.project_id, self.dataset_id)))
        self.assertEquals(len(rows), 2)

        by_participant = {r["person_id"]: r for r in rows}
        self.assertPersonFields(
            by_participant[1], {
                "gender_concept_id": gender_nonbinary_concept_id,
                "gender_source_value": "nonbinary_src",
                "gender_source_concept_id": gender_nonbinary_source_concept_id,
                "sex_at_birth_concept_id": sex_female_concept_id,
                "sex_at_birth_source_value": "female_src",
                "sex_at_birth_source_concept_id": sex_female_source_concept_id
            })
        self.assertPersonFields(
            by_participant[2], {
                "gender_concept_id": 0,
                "gender_source_value": "No matching concept",
                "gender_source_concept_id": 0,
                "sex_at_birth_concept_id": 0,
                "sex_at_birth_source_value": "No matching concept",
                "sex_at_birth_source_concept_id": 0
            })
