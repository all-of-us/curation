"""
Integration Test for the clean_height_weight module.

Normalizes all height and weight data into cm and kg and removes invalid/implausible data points (rows)

Original Issue: DC-701

The intent is to delete zero/null/implausible height/weight rows and inserting normalized rows (cm and kg)
"""

# Python imports
import os

# Project imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.clean_height_weight import CleanHeightAndWeight
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest


class CleanHeightAndWeightTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        # Set the test project identifier
        project_id = os.environ.get(PROJECT_ID)
        cls.project_id = project_id

        # Set the expected test datasets
        dataset_id = os.environ.get('BIGQUERY_DATASET_ID')
        sandbox_id = dataset_id + '_test_sandbox'

        cls.query_class = CleanHeightAndWeight(project_id, dataset_id,
                                               sandbox_id)

        sb_table_names = cls.query_class.get_sandbox_tablenames()
        for table_name in sb_table_names:
            cls.fq_sandbox_table_names.append(
                f'{project_id}.{sandbox_id}.{table_name}')

        cls.fq_table_names = [
            f'{project_id}.{dataset_id}.measurement',
            f'{project_id}.{dataset_id}.concept',
            f'{project_id}.{dataset_id}.person',
            f'{project_id}.{dataset_id}.measurement_ext',
            f'{project_id}.{dataset_id}.condition_occurrence',
            f'{project_id}.{dataset_id}.concept_ancestor'
        ]

        print(cls.fq_sandbox_table_names[0])

        # call super to set up the client, create datasets, and create
        # empty test tables
        # NOTE:  does not create empty sandbox tables.
        super().setUpClass()

    # def setUp(self):
    #     """
    #     Create common information for tests.
    #
    #     Creates common expected parameter types from cleaned tables and a common
    #     fully qualified (fq) dataset name string to load the data.
    #     """
    #
    #     fq_dataset_name = self.fq_table_names[0].split('.')
    #     self.fq_dataset_name = '.'.join(fq_dataset_name[:-1])
    #     print(self.fq_dataset_name)
    #
    #     fq_sandbox_name = self.fq_sandbox_table_names[0].split('.')
    #     self.fq_sandbox_table_name = '.'.join(fq_sandbox_name[:-1])
    #     print(self.fq_sandbox_table_name)
    #
    # def test_field_cleaning(self):
    #     """
    #     Tests that the specifications for all the queries perform as designed.
    #
    #     Validates pre conditions, tests execution, and post conditions based on the load
    #     statements and the tables_and_counts variable.
    #     """
    #
    #     measurement_tmpl = self.jinja_env.from_string("""
    #         INSERT INTO `{{fq_dataset_name}}.measurement`
    #         (person_id, measurement_id, measurement_concept_id, measurement_type_concept_id, value_as_number, measurement_date)
    #         VALUES
    #             (84619, 207645468, 3036277, 44818701, 162.2, date('2015-07-15')),
    #             (430313, 207650619, 3036277, 44818701, 147.32, date('2015-07-15')),
    #             (383148, 207147823, 3036277, 44818701, 154.0, date('2015-07-15')),
    #             (7141, 515880982, 3000593, 8160112023, 358.0, date('2015-07-15')),
    #             (448, 413052780, 3008575, 8160112023, 179.0, date('2015-07-15'))"""
    #                                                  )
    #
    #     concept_tmpl = self.jinja_env.from_string("""
    #         INSERT INTO `{{fq_dataset_name}}.concept`
    #         (concept_id, concept_name, domain_id, vocabulary_id, concept_class_id, concept_code, valid_start_date, valid_end_date)
    #         VALUES
    #             (44793713, 'Bitten or struck by dog occurrence at unspecified place', 'Observation', 'SNOMED', 'Context-dependent', '393049007', date(1970,01,01), date(2013,07,31)),
    #             (44793837, 'Bitten or struck by other mammals occurrence on farm', 'Observation', 'SNOMED', 'Context-dependent', '393049008', date(1970,01,01), date(2013,07,31)),
    #             (44799211, 'Unspecified fall occurrence on farm', 'Observation', 'SNOMED', 'Context-dependent', '393049009', date(1970,01,01), date(2013,07,31)),
    #             (44802666, 'Sleep-awake rhythm non-24-hour cycle', 'Observation', 'SNOMED', 'Context-dependent', '393049010', date(1970,01,01), date(2013,07,31)),
    #             (40319408, 'Gangrene', 'Observation', 'SNOMED', 'Context-dependent', '393049011', date(1970,01,01), date(2013,07,31))"""
    #                                              )
    #
    #     person_tmpl = self.jinja_env.from_string("""
    #         INSERT INTO `{{fq_dataset_name}}.person`
    #         (person_id, gender_concept_id, year_of_birth, race_concept_id, ethnicity_concept_id)
    #         VALUES
    #             (84619, 8507, 1951, 8516, 38003564),
    #             (430313, 8507, 1952, 8516, 38003564),
    #             (383148, 8507, 1979, 8516, 38003564),
    #             (7141, 8507, 1984, 8516, 38003564),
    #             (448, 8507, 1986, 8516, 38003564)""")
    #
    #     measurement_ext_tmpl = self.jinja_env.from_string("""
    #         INSERT INTO `{{fq_dataset_name}}.measurement_ext`
    #         (measurement_id, src_id)
    #         VALUES
    #             (207645468, 'EHR site 1234'),
    #             (207650619, 'EHR site 1234'),
    #             (207147823, 'EHR site 1234'),
    #             (515880982, 'EHR site 1234'),
    #             (413052780, 'EHR site 1234')""")
    #
    #     condition_occurrence_tmpl = self.jinja_env.from_string("""
    #         INSERT INTO `{{fq_dataset_name}}.condition_occurrence`
    #         (condition_occurrence_id, person_id, condition_concept_id, condition_start_date, condition_start_datetime, condition_type_concept_id)
    #         VALUES
    #             (6182827, 84619, 432738, date('2015-07-15'), timestamp('2016-05-01 11:00:00'), 38000235),
    #             (19072625, 430313, 380834, date('2015-07-15'), timestamp('2016-05-01 11:00:00'), 38000235),
    #             (17011387, 383148, 440383, date('2015-07-15'), timestamp('2016-05-01 11:00:00'), 38000235),
    #             (31457357, 7141, 74132, date('2015-07-15'), timestamp('2016-05-01 11:00:00'), 38000235),
    #             (61338545, 448, 440129, date('2015-07-15'), timestamp('2016-05-01 11:00:00'), 38000235)"""
    #                                                           )
    #
    #     concept_ancestor_tmpl = self.jinja_env.from_string("""
    #     INSERT INTO `{{fq_dataset_name}}.concept_ancestor`
    #     (ancestor_concept_id, descendant_concept_id, min_levels_of_separation, max_levels_of_separation)
    #     VALUES
    #         (4291007, 44793770, 3, 7),
    #         (44797565, 4291007, 3, 7),
    #         (44799211, 436583, 3, 7),
    #         (44802666, 44802615, 3, 7),
    #         (40319408, 439928, 3, 7)""")
    #
    #     measurement_query = measurement_tmpl.render(
    #         fq_dataset_name=self.fq_dataset_name)
    #     concept_query = concept_tmpl.render(
    #         fq_dataset_name=self.fq_dataset_name)
    #     person_query = person_tmpl.render(fq_dataset_name=self.fq_dataset_name)
    #     measurement_ext_query = measurement_ext_tmpl.render(
    #         fq_dataset_name=self.fq_dataset_name)
    #     condition_occurrence_query = condition_occurrence_tmpl.render(
    #         fq_dataset_name=self.fq_dataset_name)
    #     concept_ancestor_query = concept_ancestor_tmpl.render(
    #         fq_dataset_name=self.fq_dataset_name)
    #
    #     self.load_test_data([
    #         measurement_query, concept_query, person_query,
    #         measurement_ext_query, condition_occurrence_query,
    #         concept_ancestor_query
    #     ])
    #
    #     # Expected results list
    #     tables_and_counts = [{
    #         'fq_table_name': '.'.join([self.fq_dataset_name, 'measurement']),
    #         'fq_sandbox_table_name': [
    #             self.fq_sandbox_table_names[0], self.fq_sandbox_table_names[1],
    #             self.fq_sandbox_table_names[2], self.fq_sandbox_table_names[3]
    #         ],
    #         'loaded_ids': [84619, 430313, 383148, 7141, 448],
    #         'sandboxed_ids': [84619, 430313, 383148],
    #         'fields': [
    #             'person_id, measurement_id,'
    #             ' measurement_concept_id, measurement_type_concept_id, '
    #             'value_as_number'
    #         ],
    #         'cleaned_values': [(84619, 207645468, 3036277, 44818701, 162.2)]
    #     }]
    #     print(tables_and_counts)
    #
    #     self.default_test(tables_and_counts)
