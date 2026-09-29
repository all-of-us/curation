"""
Unit test for build_ct_plus_mainline.

Original Issues: DL-2534
"""
# Python imports
import unittest
from unittest import mock

# Third party imports
from google.api_core.exceptions import NotFound

# Project imports
from common import (CT_PLUS_PUBLISHABLE_TABLES, PEDIATRIC_RELATIONSHIP_EXT,
                    ZIP3_SES_MAP)
from tools import build_ct_plus_mainline as mainline
from tools import regenerate_ct_plus_ids, regenerate_etm_ct_plus_ids

INPUT = 'CP2025q4r7_deid_clean_pre_split'
PEDIATRICS = 'CP2025q4r7_pediatrics'
DOB = 'CP2025q4r7_dob'
DELTA = 'CP2025q4r7_suicide'

INPUT_COLUMNS = {
    'person': ['person_id', 'year_of_birth'],
    'observation': ['observation_id', 'person_id', 'observation_concept_id'],
    'observation_ext': ['observation_id', 'src_id'],
    'death': ['person_id', 'death_date'],
    'fact_relationship': mainline.FACT_RELATIONSHIP_KEY,
    'care_site': ['care_site_id'],
    PEDIATRIC_RELATIONSHIP_EXT: ['subject_person_id', 'related_person_id'],
}

# Every table in CP2025q4r6_deid_clean, listed 2026-09-29
R6_TABLES = '''_cdr_metadata activity_summary aou_death attribute_definition
attribute_definition_ext care_site care_site_ext cdm_source cohort cohort_attribute
cohort_definition cohort_definition_ext concept concept_ancestor concept_class
concept_relationship concept_synonym condition_era condition_era_ext
condition_occurrence condition_occurrence_ext cost cost_ext death delaydiscounting
delaydiscounting_task_view delaydiscounting_trial_level_view device device_cost
device_cost_ext device_exposure device_exposure_ext domain dose_era dose_era_ext
drug_cost drug_cost_ext drug_era drug_era_ext drug_exposure drug_exposure_ext
drug_strength emorecog emorecog_task_view emorecog_trial_level_view
fact_relationship flanker flanker_task_view flanker_trial_level_view gradcpt
gradcpt_task_view gradcpt_trial_level_view heart_rate_intraday heart_rate_summary
location location_ext measurement measurement_ext metadata note note_ext note_nlp
note_nlp_ext observation observation_ext observation_period observation_period_ext
payer_plan_period payer_plan_period_ext person person_ext procedure_cost
procedure_cost_ext procedure_occurrence procedure_occurrence_ext provider
provider_ext relationship sleep_daily_summary sleep_daily_summary_30dayavg
sleep_daily_summary_counts sleep_daily_summary_ext sleep_level sleep_level_short
source_to_concept_map specimen specimen_ext steps_intraday survey_conduct
survey_conduct_ext visit_cost visit_cost_ext visit_detail visit_detail_ext
visit_occurrence visit_occurrence_ext vocabulary wear_study zip3_ses_map'''.split(
)

VIEW = 'flanker_task_view'


class BuildCtPlusMainlineTest(unittest.TestCase):

    def setUp(self):
        self.project_id = 'fake_project'

    def test_names_resolve_to_the_mainline(self):
        self.assertEqual(
            mainline.validate_dataset_names('2025q4r7', INPUT,
                                            [PEDIATRICS, DOB]),
            'CP2025q4r7_deid_clean')

    def test_other_names_are_refused(self):
        for input_dataset_id, addons in [
            ('CP2025q4r7_deid_clean', [PEDIATRICS]),
            ('CP2025q4r6_deid_clean_pre_split', [PEDIATRICS]),
            (INPUT, []),
            (INPUT, [PEDIATRICS, 'CP2025q4r6_dob']),
        ]:
            with self.subTest(input_dataset_id=input_dataset_id, addons=addons):
                with self.assertRaises(ValueError):
                    mainline.validate_dataset_names('2025q4r7',
                                                    input_dataset_id, addons)

    def test_each_table_is_matched_on_what_its_producer_copied_it_by(self):
        for table, expected in [
            ('observation', (['observation_id'], False)),
            ('person', (['person_id'], False)),
            ('observation_ext', (['observation_id'], False)),
            ('fact_relationship', (mainline.FACT_RELATIONSHIP_KEY, False)),
            ('death', (['person_id'], True)),
        ]:
            with self.subTest(table=table):
                self.assertEqual(
                    mainline.get_subtraction_key(table, INPUT_COLUMNS[table]),
                    expected)

    def test_a_table_with_nothing_to_match_on_is_refused(self):
        with self.assertRaises(RuntimeError):
            mainline.get_subtraction_key('metadata',
                                         ['name', 'value_as_string'])

    def test_publishable_set_covers_what_the_re_key_writes(self):
        self.assertLessEqual(set(regenerate_ct_plus_ids.EXT_TABLES),
                             CT_PLUS_PUBLISHABLE_TABLES)
        self.assertLessEqual(set(regenerate_etm_ct_plus_ids.ETM_TABLES),
                             CT_PLUS_PUBLISHABLE_TABLES)
        self.assertNotIn(PEDIATRIC_RELATIONSHIP_EXT, CT_PLUS_PUBLISHABLE_TABLES)
        self.assertFalse(
            [t for t in CT_PLUS_PUBLISHABLE_TABLES if t.startswith('_')])

    def test_every_r6_table_is_published_or_dropped(self):
        self.assertEqual(
            sorted(
                set(R6_TABLES) - CT_PLUS_PUBLISHABLE_TABLES -
                mainline.DROPPED_TABLES), [])
        self.assertLessEqual({'_cdr_metadata', ZIP3_SES_MAP},
                             mainline.DROPPED_TABLES)

    def test_multi_column_keys_compare_as_one_json_string(self):
        query = mainline.MAINLINE_ROWS.render(
            project=self.project_id,
            input_dataset=INPUT,
            table='fact_relationship',
            addon_datasets=[PEDIATRICS, DELTA],
            key_columns=mainline.FACT_RELATIONSHIP_KEY)

        self.assertIn(
            'TO_JSON_STRING(STRUCT(t.domain_concept_id_1, t.fact_id_1', query)
        self.assertIn(f'`{self.project_id}.{PEDIATRICS}.fact_relationship` a',
                      query)
        self.assertIn(f'`{self.project_id}.{DELTA}.fact_relationship` a', query)
        self.assertIn('UNION ALL', query)
        self.assertIn('NOT EXISTS', query)

    def test_whole_participant_claims_are_counted_by_row(self):
        query = mainline.CLAIM_CHECK.render(project=self.project_id,
                                            input_dataset=INPUT,
                                            table='death',
                                            addon_datasets=[PEDIATRICS],
                                            key_columns=['person_id'],
                                            whole_participant=True)

        self.assertIn('c.k = t.person_id', query)
        self.assertNotIn('COUNT(DISTINCT k)', query)

    def _client(self, input_columns=None, addons=None, views=()):
        """addons maps a dataset to its tables; None as the tables means absent."""
        input_columns = INPUT_COLUMNS if input_columns is None else input_columns
        addons = {
            PEDIATRICS: ['person', 'observation', PEDIATRIC_RELATIONSHIP_EXT],
            DOB: ['observation', 'observation_ext', 'dob'],
        } if addons is None else addons
        columns = {INPUT: input_columns}
        columns.update({
            dataset: {
                table: ['x'] for table in tables
            } for dataset, tables in addons.items() if tables is not None
        })

        client = mock.MagicMock()

        def get_dataset(name):
            if name.split('.')[1] not in columns:
                raise NotFound('absent')

        client.get_dataset.side_effect = get_dataset

        def query(sql):
            dataset = sql.split('`')[1].split('.')[1]
            job = mock.MagicMock()
            if 'INFORMATION_SCHEMA.TABLES' in sql:
                job.result.return_value = [{
                    'table_name': view
                } for view in views if dataset == INPUT]
                return job
            job.result.return_value = [{
                'table_name': table,
                'column_name': column
            } for table, cols in columns[dataset].items() for column in cols]
            return job

        client.query.side_effect = query
        return client, list(addons)

    def _plan(self, client, addons):
        return mainline.plan(client, self.project_id, INPUT, addons, PEDIATRICS)

    def test_claims_are_grouped_by_table_and_person_level_tables_skipped(self):
        client, addons = self._client()

        _, claims, _ = self._plan(client, addons)

        self.assertEqual(
            claims, {
                'observation': [DOB, PEDIATRICS],
                'observation_ext': [DOB],
                'person': [PEDIATRICS]
            })

    def test_each_declaration_failure_is_refused(self):
        undeclared = dict(INPUT_COLUMNS, _deid_map=['person_id'])
        for name, kwargs, phrase in [
            ('missing add-on', {
                'addons': {
                    PEDIATRICS: [PEDIATRIC_RELATIONSHIP_EXT],
                    DOB: None
                }
            }, 'does not exist'),
            ('undeclared table', {
                'input_columns': undeclared
            }, "['_deid_map']"),
            ('relationship table not in pediatrics', {
                'addons': {
                    PEDIATRICS: ['person']
                }
            }, f'{PEDIATRIC_RELATIONSHIP_EXT} is in'),
            ('add-on table absent from input', {
                'addons': {
                    PEDIATRICS: [PEDIATRIC_RELATIONSHIP_EXT, 'specimen']
                }
            }, 'specimen matches no table'),
            ('add-on table with no key', {
                'input_columns':
                    dict(INPUT_COLUMNS, cdm_source=['cdm_source_name']),
                'addons': {
                    PEDIATRICS: [PEDIATRIC_RELATIONSHIP_EXT, 'cdm_source']
                }
            }, 'cannot be subtracted'),
        ]:
            with self.subTest(name=name):
                client, addons = self._client(**kwargs)

                with self.assertRaises(RuntimeError) as ctx:
                    self._plan(client, addons)

                self.assertIn(phrase, str(ctx.exception))

    def test_publishable_input_without_pediatric_content_needs_no_pediatrics(
            self):
        columns = {
            k: v
            for k, v in INPUT_COLUMNS.items()
            if k != PEDIATRIC_RELATIONSHIP_EXT
        }
        client, addons = self._client(input_columns=columns,
                                      addons={DELTA: ['observation']})

        _, claims, _ = self._plan(client, addons)

        self.assertEqual(claims, {'observation': [DELTA]})

    def test_dropped_tables_other_than_the_relationship_need_no_add_on(self):
        columns = dict(INPUT_COLUMNS,
                       _cdr_metadata=['name'],
                       zip3_ses_map=['zip3'])

        client, addons = self._client(input_columns=columns)

        self._plan(client, addons)

    def test_views_are_returned_and_never_claimed(self):
        columns = dict(INPUT_COLUMNS, **{VIEW: ['person_id']})
        client, addons = self._client(
            input_columns=columns,
            addons={PEDIATRICS: [PEDIATRIC_RELATIONSHIP_EXT, VIEW]},
            views=[VIEW])

        _, claims, views = self._plan(client, addons)

        self.assertEqual(views, {VIEW})
        self.assertNotIn(VIEW, claims)

    def _view_client(self, queries):
        client = mock.MagicMock()
        client.get_table.side_effect = lambda name: mock.MagicMock(
            view_query=queries[name.split('.')[-1]])
        return client

    def test_views_are_pointed_at_the_mainline(self):
        client = self._view_client(
            {VIEW: f'SELECT * FROM `{self.project_id}.{INPUT}.flanker`'})

        queries = mainline.get_view_queries(client, self.project_id, INPUT,
                                            'CP2025q4r7_deid_clean', [VIEW])

        self.assertEqual(
            queries[VIEW],
            f'SELECT * FROM `{self.project_id}.CP2025q4r7_deid_clean.flanker`')

    def test_a_view_reading_another_dataset_is_refused(self):
        client = self._view_client(
            {VIEW: f'SELECT * FROM `{self.project_id}.C2025q4r6.flanker`'})

        with self.assertRaises(RuntimeError) as ctx:
            mainline.get_view_queries(client, self.project_id, INPUT,
                                      'CP2025q4r7_deid_clean', [VIEW])

        self.assertIn("['C2025q4r6']", str(ctx.exception))

    def test_a_view_is_created_after_the_views_it_reads(self):
        client = mock.MagicMock()
        created = []
        client.create_table.side_effect = lambda t: created.append(t.table_id)

        mainline.recreate_views(
            client, self.project_id, 'out', {
                'a_task_view': 'SELECT * FROM `p.out.z_trial_level_view`',
                'z_trial_level_view': 'SELECT * FROM `p.out.flanker`'
            })

        self.assertEqual(created, ['z_trial_level_view', 'a_task_view'])

    def test_views_reading_each_other_in_a_cycle_are_refused(self):
        with self.assertRaises(RuntimeError):
            mainline.recreate_views(
                mock.MagicMock(), self.project_id, 'out', {
                    'a_view': 'SELECT * FROM `p.out.b_view`',
                    'b_view': 'SELECT * FROM `p.out.a_view`'
                })

    def _check(self, rows):
        client = mock.MagicMock()
        client.query.return_value.result.side_effect = [[row] for row in rows]
        return mainline.check_claims(client, self.project_id, INPUT,
                                     INPUT_COLUMNS, {
                                         'observation': [PEDIATRICS, DOB],
                                         'person': [PEDIATRICS]
                                     })

    def test_matching_claims_are_accepted(self):
        counts = self._check([{
            'input_rows': 10,
            'removed_rows': 4,
            'claimed_rows': 4
        }, {
            'input_rows': 5,
            'removed_rows': 1,
            'claimed_rows': 1
        }])

        self.assertEqual(counts, {'observation': (10, 4), 'person': (5, 1)})

    def test_disagreeing_claims_are_refused_together(self):
        with self.assertRaises(RuntimeError) as ctx:
            self._check([{
                'input_rows': 10,
                'removed_rows': 3,
                'claimed_rows': 4
            }, {
                'input_rows': 5,
                'removed_rows': 2,
                'claimed_rows': 1
            }])

        self.assertIn('observation', str(ctx.exception))
        self.assertIn('person', str(ctx.exception))

    @mock.patch.object(mainline, 'write_table', return_value=6)
    def test_claimed_tables_are_filtered_and_the_rest_copied(self, mock_write):
        client = mock.MagicMock()
        client.get_table.return_value.num_rows = 3

        written = mainline.write_mainline(
            client, self.project_id, INPUT, 'CP2025q4r7_deid_clean',
            dict(INPUT_COLUMNS, **{VIEW: ['person_id']}),
            {'observation': [PEDIATRICS]}, {'observation': (10, 4)}, {VIEW})

        self.assertEqual([call.args[4] for call in mock_write.call_args_list],
                         ['observation'])
        copied = sorted(call.args[0].split('.')[-1]
                        for call in client.copy_table.call_args_list)
        self.assertEqual(copied, [
            'care_site', 'death', 'fact_relationship', 'observation_ext',
            'person'
        ])
        self.assertNotIn(PEDIATRIC_RELATIONSHIP_EXT, written)
        self.assertNotIn(VIEW, written)

    @mock.patch.object(mainline, 'write_table', return_value=7)
    def test_a_written_count_that_does_not_add_up_is_refused(self, _):
        with self.assertRaises(RuntimeError):
            mainline.write_mainline(
                client=mock.MagicMock(),
                project_id=self.project_id,
                input_dataset_id=INPUT,
                output_dataset_id='CP2025q4r7_deid_clean',
                input_columns={'observation': INPUT_COLUMNS['observation']},
                claims={'observation': [PEDIATRICS]},
                counts={'observation': (10, 4)})

    @mock.patch.object(mainline, 'log_counts')
    @mock.patch.object(mainline, 'recreate_views')
    @mock.patch.object(mainline, 'write_mainline')
    @mock.patch.object(mainline, 'assert_output_dataset_is_empty')
    @mock.patch.object(mainline, 'get_view_queries')
    @mock.patch.object(mainline, 'check_claims')
    @mock.patch.object(mainline, 'plan', return_value=({}, {}, set()))
    @mock.patch.object(mainline, 'BigQueryClient')
    def test_main_refuses_before_it_creates_anything(self, mock_client_class,
                                                     mock_plan, mock_check,
                                                     mock_views, mock_empty,
                                                     mock_write, mock_recreate,
                                                     _):
        client = mock_client_class.return_value
        order = mock.MagicMock()
        order.attach_mock(mock_plan, 'plan')
        order.attach_mock(mock_check, 'check')
        order.attach_mock(mock_views, 'views')
        order.attach_mock(mock_empty, 'empty')
        order.attach_mock(client.create_dataset, 'create_dataset')
        order.attach_mock(mock_write, 'write')
        order.attach_mock(mock_recreate, 'recreate')

        mainline.main(self.project_id, '2025q4r7', INPUT, [PEDIATRICS, DOB])

        self.assertEqual([call[0] for call in order.mock_calls], [
            'plan', 'check', 'views', 'empty', 'create_dataset', 'write',
            'recreate'
        ])
        self.assertEqual(mock_plan.call_args.args[4], PEDIATRICS)
        self.assertEqual(client.define_dataset.call_args.args[0],
                         'CP2025q4r7_deid_clean')
        self.assertEqual(mock_empty.call_args.args[2], 'CP2025q4r7_deid_clean')


if __name__ == '__main__':
    unittest.main()
