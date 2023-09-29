"""
Integration test for import_rdr_dataset
"""
# Python imports
import os

# Project imports
from app_identity import get_application_id, PROJECT_ID
from common import (AOU_DEATH, CARE_SITE, METADATA, DEATH, DRUG_ERA,
                    OBSERVATION, PERSON, PID_RID_MAPPING, VISIT_COST)
from resources import cdm_schemas, fields_for, rdr_src_id_schemas
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest
from tools.import_rdr_dataset import create_rdr_tables, get_destination_schemas


class ImportRdrDatasetTest(BaseTest.BigQueryTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        cls.project_id = os.environ.get(PROJECT_ID)
        cls.dataset_id = os.environ.get('COMBINED_DATASET_ID')
        cls.rdr_dataset_id = os.environ.get('RDR_DATASET_ID')

        # fq_table_names cannot be empty so adding PERSON for a placeholder
        cls.fq_table_names = [f'{cls.project_id}.{cls.rdr_dataset_id}.{PERSON}']

        tables = list(
            set(cdm_schemas().keys()) | set(rdr_src_id_schemas().keys()))

        cls.fq_sandbox_table_names = [
            f'{cls.project_id}.{cls.rdr_dataset_id}.{table}' for table in tables
        ] + [
            f'{cls.project_id}.{cls.dataset_id}.{table}' for table in tables
            if table != DEATH
        ] + [f'{cls.project_id}.{cls.dataset_id}.{AOU_DEATH}']

        super().setUpClass()

    def setUp(self):
        super().setUp()

        # Delete PERSON table b/c its schema is CDM, not RDR custom
        _ = self.client.delete_table(self.fq_table_names[0])

        # Create the tables from RDR using `rdr_xyz.json` schema info.
        rdr_schema_dict = cdm_schemas()
        rdr_schema_dict.update(rdr_src_id_schemas())
        rdr_tables = [
            f'{self.project_id}.{self.rdr_dataset_id}.{table}'
            for table in rdr_schema_dict.keys()
        ]
        rdr_schemas = list(rdr_schema_dict.values())
        _ = self.client.create_tables(fq_table_names=rdr_tables,
                                      fields=rdr_schemas)

        insert_death = self.jinja_env.from_string("""
            INSERT INTO `{{project}}.{{dataset}}.death`
            (person_id, death_date, death_type_concept_id, src_id)
            VALUES
            (1, '2022-01-01', 11, 'healthpro'),
            (2, '2022-01-01', 21, 'healthpro'),
            (3, NULL, 31, 'healthpro')
            """).render(project=self.project_id, dataset=self.rdr_dataset_id)

        self.load_test_data([insert_death])

    def test_get_destination_schemas(self):
        """Test get_destination_schemas()
        Confirm the following:
            (1) CDM tables and RDR tables are the keys of the dict,
            (2) DEATH is excluded, AOU_DEATH is included, and
            (3) rdr_xyz schema is used where possible.
        """
        schema_dict = get_destination_schemas()

        # Ensure each type of CDM tables is included in the result.
        # Pick one table per type for a test sample.
        self.assertIn(CARE_SITE, schema_dict.keys())  # clinical
        self.assertIn(DRUG_ERA, schema_dict.keys())  # derived
        self.assertIn(VISIT_COST, schema_dict.keys())  # health_economics
        self.assertIn(METADATA, schema_dict.keys())  # metadata
        self.assertIn(PID_RID_MAPPING, schema_dict.keys())  # RDR

        # Ensure DEATH is excluded. We use AOU_DEATH instead.
        self.assertNotIn(DEATH, schema_dict.keys())
        self.assertIn(AOU_DEATH, schema_dict.keys())

        # Ensure correct schema is used. rdr_xyz schema is used where available.
        self.assertEqual(schema_dict[CARE_SITE], fields_for(f'rdr_{CARE_SITE}'))
        self.assertEqual(schema_dict[PID_RID_MAPPING],
                         fields_for(f'rdr_{PID_RID_MAPPING}'))
        # AOU_DEATH does not have RDR specific schema definition.
        self.assertEqual(schema_dict[AOU_DEATH], fields_for(AOU_DEATH))

    def test_create_rdr_tables_aou_death(self):
        """Test create_rdr_tables for aou_death creation.
        Confirm the following:
            (1) RDR's death records are loaded to our Raw RDR AOU_DEATH table,
            (2) NULL death_date records do not fail the process, and
            (3) We do not create DEATH table in our Raw RDR.
        """
        create_rdr_tables(client=self.client,
                          destination_dataset=self.dataset_id,
                          rdr_project=self.project_id,
                          rdr_source_dataset=self.rdr_dataset_id)

        self.assertTableValuesMatch(
            f'{self.project_id}.{self.dataset_id}.{AOU_DEATH}',
            ['person_id', 'primary_death_record'], [(1, False), (2, False),
                                                    (3, False)])

        self.assertTableDoesNotExist(
            f'{self.project_id}.{self.dataset_id}.{DEATH}')

    def test_create_rdr_tables(self):
        """Test create_rdr_tables for table creation.
        Confirm the following:
            (1) Records from RDR are copied to the corresponding tables in Curation.
            (2) When the RDR's CDM table is empty, an empty table is created in Curation too.
            (3) Even when the CDM table does not exist in RDR, an empty table is created in Curation.
        """

        # Adding records for the test case (1)
        insert_obs = self.jinja_env.from_string("""
            INSERT INTO `{{project}}.{{dataset}}.{{obs}}`
                (observation_id, person_id, observation_concept_id, observation_date,
                 observation_type_concept_id, src_id)
            VALUES
                (101, 1, 0, date('2022-01-01'), 0, 'src_a'),
                (102, 1, 0, date('2022-01-01'), 0, 'src_b'),
                (103, 1, 0, date('2022-01-01'), 0, 'src_c')
        """).render(project=self.project_id,
                    dataset=self.rdr_dataset_id,
                    obs=OBSERVATION)

        self.load_test_data([insert_obs])

        # Deleting a table for the test case (3)
        self.client.delete_table(
            f'{self.project_id}.{self.rdr_dataset_id}.{VISIT_COST}')

        create_rdr_tables(client=self.client,
                          destination_dataset=self.dataset_id,
                          rdr_project=self.project_id,
                          rdr_source_dataset=self.rdr_dataset_id)

        # (1) Records from RDR are copied to the corresponding tables in Curation.
        self.assertTableValuesMatch(
            f'{self.project_id}.{self.dataset_id}.{OBSERVATION}', [
                'observation_id',
            ], [(101,), (102,), (103,)])

        # (2) When the RDR's CDM table is empty, an empty table is created in Curation too.
        self.assertTrue(self.client.table_exists(DRUG_ERA, self.dataset_id))

        # (3) Even when the CDM table does not exist in RDR, an empty table is created in Curation.
        self.assertTrue(self.client.table_exists(VISIT_COST, self.dataset_id))
