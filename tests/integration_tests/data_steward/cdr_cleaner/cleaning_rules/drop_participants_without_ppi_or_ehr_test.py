# Python imports
import unittest

# Third party imports
from jinja2 import Template

# Project imports
import bq_utils
import common
from cdr_cleaner import clean_cdr_engine
from cdr_cleaner.cleaning_rules import drop_participants_without_ppi_or_ehr
from tests import test_util

# Participant 1: no data (to be removed)
# Participant 2: has the basics only
# Participant 3: has EHR data only
# Participant 4: has both basics and EHR
# Participant 5: has only RDR consent (to be removed)
# Participant 6: has EHR observation only
INSERT_FAKE_PARTICIPANTS_TMPLS = [
    # TODO(calbach): Ideally these tests should not manipulate concept table, not currently hermetic.
    Template("""
INSERT INTO `{{project_id}}.{{dataset_id}}.concept` (concept_id)
VALUES
  ({{rdr_basics_concept_id}}),
  ({{rdr_basics_module_concept_id}}),
  ({{rdr_consent_concept_id}}),
  ({{ehr_obs_concept_id}})
"""),
    Template("""
INSERT INTO `{{project_id}}.{{dataset_id}}.concept_ancestor` (descendant_concept_id, ancestor_concept_id)
VALUES ({{rdr_basics_concept_id}}, {{rdr_basics_module_concept_id}})
"""),
    Template("""
INSERT INTO `{{project_id}}.{{dataset_id}}.person` (person_id)
VALUES (1), (2), (3), (4), (5), (6)
"""),
    Template("""
INSERT INTO `{{project_id}}.{{dataset_id}}.observation` (person_id, observation_id, observation_concept_id)
VALUES
  (2, 100, {{rdr_basics_concept_id}}),
  (2, 101, {{rdr_consent_concept_id}}),
  (4, 102, {{rdr_basics_concept_id}}),
  (5, 103, {{rdr_consent_concept_id}}),
  (6, 104, {{ehr_obs_concept_id}})
"""),
    Template("""
INSERT INTO `{{project_id}}.{{dataset_id}}._mapping_observation` (observation_id, src_hpo_id)
VALUES
  (100, "rdr"),
  (101, "rdr"),
  (102, "rdr"),
  (103, "rdr"),
  (104, "fake-hpo")
"""),
    Template("""
INSERT INTO `{{project_id}}.{{dataset_id}}.drug_exposure` (person_id, drug_exposure_id)
VALUES
  (3, 200),
  (4, 201)
"""),
    Template("""
INSERT INTO `{{project_id}}.{{dataset_id}}._mapping_drug_exposure` (drug_exposure_id, src_hpo_id)
VALUES
  (200, "fake-hpo"),
  (201, "fake-hpo")
""")
]


class DropParticipantsWithoutPpiOrEhrTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def test_get_queries(self):
        results = drop_participants_without_ppi_or_ehr.get_queries('foo', 'bar')

        self.assertEqual(
            len(results), 1 + len(common.CLINICAL_DATA_TABLES),
            'wanted one person deletion query and a deletion query per clinical table'
        )

    @unittest.skip(
        "TODO(calbach): Move into integration test package, once that exists")
    def test_execute_queries(self):
        project_id = bq_utils.app_identity.get_application_id()
        dataset_id = bq_utils.get_combined_dataset_id()
        test_util.delete_all_tables(dataset_id)

        create_tables = (
            ['person'] + common.CLINICAL_DATA_TABLES +
            ['_mapping_' + t for t in common.MAPPED_CLINICAL_DATA_TABLES])
        # TODO(calbach): Make the setup/teardown of these concept tables hermetic.
        for tbl in ['concept', 'concept_ancestor']:
            if not bq_utils.table_exists(tbl, dataset_id=dataset_id):
                create_tables.push(tbl)
        for tbl in create_tables:
            bq_utils.create_standard_table(tbl,
                                           tbl,
                                           dataset_id=dataset_id,
                                           force_all_nullable=True)

        for tmpl in INSERT_FAKE_PARTICIPANTS_TMPLS:
            resp = bq_utils.query(
                tmpl.render(project_id=project_id,
                            dataset_id=dataset_id,
                            rdr_basics_concept_id=123,
                            rdr_consent_concept_id=345,
                            ehr_obs_concept_id=567,
                            rdr_basics_module_concept_id=
                            drop_participants_without_ppi_or_ehr.
                            BASICS_MODULE_CONCEPT_ID))
            self.assertTrue(resp["jobComplete"])

        clean_cdr_engine.clean_dataset(
            project_id, dataset_id,
            [(drop_participants_without_ppi_or_ehr.get_queries,)])

        def table_to_person_ids(t):
            rows = bq_utils.response2rows(
                bq_utils.query("SELECT person_id FROM `{}.{}.{}`".format(
                    project_id, dataset_id, t)))
            return set([r["person_id"] for r in rows])

        # We expect participants 1, 5 to have been removed from all tables.
        self.assertEqual(set([2, 3, 4, 6]), table_to_person_ids("person"))
        self.assertEqual(set([2, 4, 6]), table_to_person_ids("observation"))
        self.assertEquals(set([3, 4]), table_to_person_ids("drug_exposure"))

        test_util.delete_all_tables(dataset_id)
