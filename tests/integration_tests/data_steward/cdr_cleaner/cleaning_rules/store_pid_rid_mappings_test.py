from datetime import datetime
import os

import mock

from app_identity import get_application_id
from common import PID_RID_MAPPING, PRIMARY_PID_RID_MAPPING
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest
from cdr_cleaner.cleaning_rules.store_pid_rid_mappings import StoreNewPidRidMappings


class StoreMappingsTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        # set the test project identifier
        cls.project_id = get_application_id()

        # set the expected test datasets
        cls.dataset_id = os.getenv('RDR_DATASET_ID')
        cls.sandbox_id = f'{cls.dataset_id}_sandbox'

        cls.rule_instance = StoreNewPidRidMappings(cls.project_id,
                                                   cls.dataset_id,
                                                   cls.sandbox_id)

        sb_table_names = cls.rule_instance.get_sandbox_tablenames()
        for table_name in sb_table_names:
            cls.fq_sandbox_table_names.append(
                f'{cls.project_id}.{cls.sandbox_id}.{table_name}')

        cls.import_map_name = f'{cls.project_id}.{cls.dataset_id}.{PID_RID_MAPPING}'
        # This will normally be the PIPELINE_TABLES dataset, but is being
        # mocked for this test
        cls.stable_map_name = f'{cls.project_id}.{cls.dataset_id}.{PRIMARY_PID_RID_MAPPING}'
        cls.fq_table_names = [cls.import_map_name, cls.stable_map_name]

        # call super to set up the client, create datasets, and create
        # empty test tables
        # NOTE:  does not create empty sandbox tables.

        super().setUpClass()

    def setUp(self):
        super().setUp()

        # pre-conditions
        import_map_tmpl = self.jinja_env.from_string("""
            INSERT INTO `{{import_map_name}}`
                (person_id, research_id)
            VALUES
                -- should not get added because research_id is a repeat --
                (100000000, 20),
                -- should not get added because it is a repeat --
                (200000000, 21),
                -- should not get added because person_id is a repeat --
                (300000000, 34),
                -- should get added, it is unique --
                (400000000, 45),
                (600000000, 70)
        """).render(import_map_name=self.import_map_name)

        primary_map_tmpl = self.jinja_env.from_string("""
        INSERT INTO `{{primary_map_name}}`
            (person_id, research_id, shift, import_date)
        VALUES
            (500000000, 88, 78, date('2020-09-01')),
            (1000000000, 100, 222, date('2020-08-01')),
            (200000000, 21, 1, date('2020-07-01')),
            (300000000, 44, 364, date('2020-06-01')),
            (800000000, 20, 90, date('2020-05-01'))
        """).render(primary_map_name=self.stable_map_name)

        queries = [import_map_tmpl, primary_map_tmpl]
        self.load_test_data(queries)

    def test_store_mapping(self):
        tables_and_counts = [{
            'fq_table_name':
                self.stable_map_name,
            'fields': ['person_id', 'research_id', 'import_date'],
            'loaded_ids': [
                500000000, 1000000000, 200000000, 300000000, 800000000
            ],
            'cleaned_values': [
                # Should exist without having been overwritten
                (500000000, 88, datetime.strptime('2020-09-01',
                                                  '%Y-%m-%d').date()),
                (1000000000, 100, datetime.strptime('2020-08-01',
                                                    '%Y-%m-%d').date()),
                (200000000, 21, datetime.strptime('2020-07-01',
                                                  '%Y-%m-%d').date()),
                (300000000, 44, datetime.strptime('2020-06-01',
                                                  '%Y-%m-%d').date()),
                (800000000, 20, datetime.strptime('2020-05-01',
                                                  '%Y-%m-%d').date()),
                # should have been added by the rule, each is unique
                (400000000, 45, datetime.now().date()),
                (600000000, 70, datetime.now().date())
            ]
        }]

        # mock the PIPELINE_TABLES variable so tests on different branches
        # don't overwrite each other.
        with mock.patch(
                'cdr_cleaner.cleaning_rules.store_pid_rid_mappings.PIPELINE_TABLES',
                self.dataset_id):
            self.default_test(tables_and_counts)
