"""
integration test for generate_ext_tables
"""
# Python imports
import os

# Third Party imports
import mock

# Project imports
from app_identity import get_application_id
from common import (EXT_SUFFIX, MAPPING_PREFIX, OBSERVATION,
                    PROCEDURE_OCCURRENCE, SITE_MASKING_TABLE_ID, SURVEY_CONDUCT)
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest
from cdr_cleaner.cleaning_rules.generate_ext_tables import GenerateExtTables


class GenerateExtTablesTest(BaseTest.CleaningRulesTestBase):

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
        cls.mapping_dataset_id = cls.dataset_id

        cls.rule_instance = GenerateExtTables(cls.project_id, cls.dataset_id,
                                              cls.sandbox_id,
                                              cls.mapping_dataset_id)

        cls.kwargs.update({'mapping_dataset_id': cls.mapping_dataset_id})

        sb_table_names = cls.rule_instance.get_sandbox_tablenames()
        for table_name in sb_table_names:
            cls.fq_sandbox_table_names.append(
                f'{cls.project_id}.{cls.sandbox_id}.{table_name}')

        # This will normally be the PIPELINE_TABLES dataset, but is being
        # mocked for this test
        cls.stable_map_name = f'{cls.project_id}.{cls.dataset_id}.{SITE_MASKING_TABLE_ID}'
        cls.mapping_obs = f'{cls.project_id}.{cls.dataset_id}.{MAPPING_PREFIX}{OBSERVATION}'
        cls.mapping_proc_occur = f'{cls.project_id}.{cls.dataset_id}.{MAPPING_PREFIX}{PROCEDURE_OCCURRENCE}'
        cls.mapping_survey_conduct = f'{cls.project_id}.{cls.dataset_id}.{MAPPING_PREFIX}{SURVEY_CONDUCT}'
        cls.fq_table_names = [
            cls.stable_map_name, cls.mapping_obs, cls.mapping_proc_occur,
            cls.mapping_survey_conduct
        ]

        # call super to set up the client, create datasets, and create
        # empty test tables
        # NOTE:  does not create empty sandbox tables.
        super().setUpClass()

    def setUp(self):
        super().setUp()

        # These should be removed after the test finishes, but are created
        # by the rule.  So cannot be created by base class.
        self.obs_ext = f'{self.project_id}.{self.dataset_id}.{OBSERVATION}{EXT_SUFFIX}'
        self.fq_table_names.append(self.obs_ext)
        self.proc_occur_ext = f'{self.project_id}.{self.dataset_id}.{PROCEDURE_OCCURRENCE}{EXT_SUFFIX}'
        self.fq_table_names.append(self.proc_occur_ext)
        self.survey_conduct_ext = f'{self.project_id}.{self.dataset_id}.{SURVEY_CONDUCT}{EXT_SUFFIX}'
        self.fq_table_names.append(self.survey_conduct_ext)

        # pre-conditions
        site_mask_tmpl = self.jinja_env.from_string("""
        INSERT INTO `{{site_mask_name}}`
            (hpo_id, src_id, state, value_source_concept_id)
        VALUES
            ('foo', 'site bar', 'PIIState_XY', 1010101),
            ('baz', 'pi/pm', 'PIIState_XY', 1010101),
            ('phi', 'site yum', 'PIIState_XY', 1010101)
        """).render(site_mask_name=self.stable_map_name)

        mapping_obs_tmpl = self.jinja_env.from_string("""
        INSERT INTO `{{obs_map_name}}`
           (observation_id, src_dataset_id, src_observation_id, src_hpo_id, src_table_id)
        VALUES
            (21, '2021_baz', 1, 'baz', 'observation'),
            (22, '2021_phi', 2, 'phi', 'observation'),
            (23, '2021_foo', 3, 'foo', 'observation')
        """).render(obs_map_name=self.mapping_obs)

        mapping_proc_occur_tmpl = self.jinja_env.from_string("""
        INSERT INTO `{{proc_occur_map_name}}`
           (procedure_occurrence_id, src_dataset_id, src_procedure_occurrence_id, src_hpo_id, src_table_id)
        VALUES
            (31, '2021_baz', 1, 'baz', 'procedure_occurrence'),
            (32, '2021_phi', 2, 'phi', 'procedure_occurrence'),
            (33, '2021_foo', 3, 'foo', 'procedure_occurrence')
        """).render(proc_occur_map_name=self.mapping_proc_occur)

        survey_conduct_tmpl = self.jinja_env.from_string("""
        INSERT INTO `{{survey_conduct_map_name}}`
           (survey_conduct_id, src_dataset_id, src_survey_conduct_id, src_hpo_id, src_table_id)
        VALUES
            (41, '2021_baz', 1, 'baz', 'survey_conduct'),
            (42, '2021_phi', 2, 'phi', 'survey_conduct'),
            (43, '2021_foo', 3, 'foo', 'survey_conduct')
        """).render(survey_conduct_map_name=self.mapping_survey_conduct)

        queries = [
            site_mask_tmpl, mapping_obs_tmpl, mapping_proc_occur_tmpl,
            survey_conduct_tmpl
        ]
        self.load_test_data(queries)

    def test_generate_ext_tables(self):
        """
        Test that ext tables are created as expected.
        For survey_conduct_ext, the column 'language' is NULL at this point.
        The cleaning rule from DC-2627 populates these columns.
        """
        tables_and_counts = [{
            'fq_table_name':
                self.obs_ext,
            'fields': ['observation_id', 'src_id', 'survey_version_concept_id'],
            'loaded_ids': [],
            'tables_created_on_setup': [
                f'{self.project_id}.{self.sandbox_id}.{self.rule_instance.get_sandbox_tablenames()[0]}'
            ],
            'check_preconditions':
                False,
            'cleaned_values': [(21, 'pi/pm', None), (22, 'site yum', None),
                               (23, 'site bar', None)]
        }, {
            'fq_table_name':
                self.proc_occur_ext,
            'fields': ['procedure_occurrence_id', 'src_id'],
            'loaded_ids': [],
            'tables_created_on_setup': [
                f'{self.project_id}.{self.sandbox_id}.{self.rule_instance.get_sandbox_tablenames()[0]}'
            ],
            'check_preconditions':
                False,
            'cleaned_values': [(31, 'pi/pm'), (32, 'site yum'),
                               (33, 'site bar')]
        }, {
            'fq_table_name':
                self.survey_conduct_ext,
            'fields': ['survey_conduct_id', 'src_id', 'language'],
            'loaded_ids': [],
            'tables_created_on_setup': [
                f'{self.project_id}.{self.sandbox_id}.{self.rule_instance.get_sandbox_tablenames()[0]}'
            ],
            'check_preconditions':
                False,
            'cleaned_values': [(41, 'pi/pm', None), (42, 'site yum', None),
                               (43, 'site bar', None)]
        }]

        # mock the PIPELINE_TABLES variable so tests on different branches
        # don't overwrite each other.
        with mock.patch(
                'cdr_cleaner.cleaning_rules.generate_ext_tables.PIPELINE_TABLES',
                self.dataset_id):
            self.rule_instance.setup_rule(self.client)
            self.default_test(tables_and_counts)
