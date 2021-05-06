from unittest import TestCase, mock
import re

import bq_utils
import common
import resources
from retraction import retract_data_bq as rbq


class RetractDataBqTest(TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.hpo_id = 'fake'
        self.project_id = 'fake-project-id'
        self.ehr_dataset_id = 'ehr20190801_fake'
        self.unioned_dataset_id = 'unioned_ehr20190801'
        self.combined_dataset_id = 'combined20190801'
        self.deid_dataset_id = 'r2021q2r1_deid'
        self.sandbox_dataset_id = 'sandbox_dataset'
        self.client = 'fake_client'
        self.pid_table_id = 'pid_table'
        self.retraction_type_1 = 'rdr_and_ehr'
        self.retraction_type_2 = 'only_ehr'
        self.tables_to_retract_unioned = rbq.TABLES_FOR_RETRACTION | {
            common.FACT_RELATIONSHIP, common.PERSON
        }
        self.tables_to_retract_combined = rbq.TABLES_FOR_RETRACTION | {
            common.FACT_RELATIONSHIP
        }

    @mock.patch('retraction.retract_data_bq.list_existing_tables')
    def test_queries_to_retract_from_ehr_dataset(self,
                                                 mock_list_existing_tables):
        hpo_person = bq_utils.get_table_id(self.hpo_id, common.PERSON)
        hpo_death = bq_utils.get_table_id(self.hpo_id, common.DEATH)

        # hpo tables
        existing_table_ids = [hpo_person, hpo_death]
        for table in self.tables_to_retract_unioned:
            table_id = bq_utils.get_table_id(self.hpo_id, table)
            existing_table_ids.append(table_id)

        # unioned tables
        ignored_tables = []
        for cdm_table in resources.CDM_TABLES:
            unioned_table_id = rbq.UNIONED_EHR + cdm_table
            existing_table_ids.append(unioned_table_id)

            if cdm_table not in self.tables_to_retract_unioned:
                ignored_tables.append(unioned_table_id)

        mock_list_existing_tables.return_value = existing_table_ids
        person_id_query = rbq.JINJA_ENV.from_string(rbq.PERSON_ID_QUERY).render(
            person_research_id=rbq.PERSON_ID,
            pid_project=self.project_id,
            sandbox_dataset_id=self.sandbox_dataset_id,
            pid_table_id=self.pid_table_id)
        qs = rbq.queries_to_retract_from_ehr_dataset(self.client,
                                                     self.project_id,
                                                     self.ehr_dataset_id,
                                                     self.hpo_id,
                                                     person_id_query)

        expected_tables = set(existing_table_ids) - set(ignored_tables)
        actual_tables = set()
        for query in qs:
            fq_table = re.search('`(.*)`', query)
            if fq_table:
                table = fq_table.group(1).split('.')[-1]
                actual_tables.add(table)
        self.assertSetEqual(expected_tables, actual_tables)

    @mock.patch('retraction.retract_data_bq.list_existing_tables')
    def test_queries_to_retract_from_combined_dataset(
        self, mock_list_existing_tables):
        existing_table_ids = []
        ignored_tables = []
        for cdm_table in resources.CDM_TABLES:
            existing_table_ids.append(cdm_table)
            if cdm_table not in self.tables_to_retract_combined:
                ignored_tables.append(cdm_table)

        mock_list_existing_tables.return_value = existing_table_ids
        person_id_query = rbq.JINJA_ENV.from_string(rbq.PERSON_ID_QUERY).render(
            person_research_id=rbq.PERSON_ID,
            pid_project=self.project_id,
            sandbox_dataset_id=self.sandbox_dataset_id,
            pid_table_id=self.pid_table_id)
        qs = rbq.queries_to_retract_from_dataset(self.client, self.project_id,
                                                 self.combined_dataset_id,
                                                 person_id_query,
                                                 self.retraction_type_2)

        expected_tables = set(existing_table_ids) - set(ignored_tables)
        actual_tables = set()
        for query in qs:
            fq_table = re.search('`(.*)`', query)
            if fq_table:
                table = fq_table.group(1).split('.')[-1]
                actual_tables.add(table)
        self.assertSetEqual(expected_tables, actual_tables)

    @mock.patch('retraction.retract_data_bq.list_existing_tables')
    def test_queries_to_retract_from_deid_dataset(self,
                                                  mock_list_existing_tables):
        existing_table_ids = []
        ignored_tables = []
        for cdm_table in resources.CDM_TABLES:
            existing_table_ids.append(cdm_table)
            if cdm_table not in self.tables_to_retract_combined:
                ignored_tables.append(cdm_table)

        mock_list_existing_tables.return_value = existing_table_ids
        research_id_query = rbq.JINJA_ENV.from_string(
            rbq.PERSON_ID_QUERY).render(
                person_research_id=rbq.RESEARCH_ID,
                pid_project=self.project_id,
                sandbox_dataset_id=self.sandbox_dataset_id,
                pid_table_id=self.pid_table_id)
        qs = rbq.queries_to_retract_from_dataset(self.client, self.project_id,
                                                 self.deid_dataset_id,
                                                 research_id_query,
                                                 self.retraction_type_1)

        expected_tables = set(existing_table_ids) - set(ignored_tables)
        actual_tables = set()
        for query in qs:
            fq_table = re.search('`(.*)`', query)
            if fq_table:
                table = fq_table.group(1).split('.')[-1]
                actual_tables.add(table)
                if table not in [
                        common.PERSON, common.DEATH, common.FACT_RELATIONSHIP
                ]:
                    self.assertIn(str(2 * rbq.ID_CONSTANT_FACTOR), query)
        self.assertSetEqual(expected_tables, actual_tables)

        qs = rbq.queries_to_retract_from_dataset(self.client, self.project_id,
                                                 self.deid_dataset_id,
                                                 research_id_query,
                                                 self.retraction_type_2)

        expected_tables = set(existing_table_ids) - set(ignored_tables)
        actual_tables = set()
        for query in qs:
            fq_table = re.search('`(.*)`', query)
            if fq_table:
                table = fq_table.group(1).split('.')[-1]
                actual_tables.add(table)
        self.assertSetEqual(expected_tables, actual_tables)

    @mock.patch('retraction.retract_data_bq.list_existing_tables')
    def test_queries_to_retract_from_unioned_dataset(self,
                                                     mock_list_existing_tables):
        existing_table_ids = []
        ignored_tables = []
        for cdm_table in resources.CDM_TABLES:
            existing_table_ids.append(cdm_table)
            if cdm_table not in self.tables_to_retract_unioned:
                ignored_tables.append(cdm_table)

        mock_list_existing_tables.return_value = existing_table_ids
        person_id_query = rbq.JINJA_ENV.from_string(rbq.PERSON_ID_QUERY).render(
            person_research_id=rbq.PERSON_ID,
            pid_project=self.project_id,
            sandbox_dataset_id=self.sandbox_dataset_id,
            pid_table_id=self.pid_table_id)
        qs = rbq.queries_to_retract_from_dataset(self.client, self.project_id,
                                                 self.unioned_dataset_id,
                                                 person_id_query,
                                                 self.retraction_type_2)

        expected_tables = set(existing_table_ids) - set(ignored_tables)
        actual_tables = set()
        for query in qs:
            fq_table = re.search('`(.*)`', query)
            if fq_table:
                table = fq_table.group(1).split('.')[-1]
                actual_tables.add(table)
        self.assertSetEqual(expected_tables, actual_tables)
