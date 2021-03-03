"""
Integration test for the cleaning_rules.ehr_submission_data_cutoff.py module

Original Issue: DC-1445

Intent of this integration test is to ensure that the data cutoff for the PPI data in all CDM tables is enforced by
 sandboxing and removing any records that persist after the data cutoff date.
"""

# Python imports
import os
from dateutil.parser import parse

# Project imports
import common
from app_identity import PROJECT_ID
from constants import bq_utils as bq_consts
from constants.cdr_cleaner import clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.ehr_submission_data_cutoff import EhrSubmissionDataCutoff
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest

tables = ['visit_occurrence', 'person']


class EhrSubmissionDataCutoffTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        # set the test project identifier
        project_id = os.environ.get(PROJECT_ID)
        cls.project_id = project_id

        # set the expected test datasets
        dataset_id = os.environ.get('UNIONED_DATASET_ID')
        cls.dataset_id = dataset_id
        sandbox_id = dataset_id + '_sandbox'
        cls.sandbox_id = sandbox_id

        # provides the cutoff_date argument
        cutoff_date = "2020-05-01"
        cls.kwargs.update({'cutoff_date': cutoff_date})

        cls.rule_instance = EhrSubmissionDataCutoff(project_id, dataset_id,
                                                    sandbox_id)

        for table_name in tables:
            sandbox_table_name = cls.rule_instance.sandbox_table_for(table_name)
            cls.fq_sandbox_table_names.append(
                f'{cls.project_id}.{cls.sandbox_id}.{sandbox_table_name}')
            cls.fq_table_names.append(
                f'{cls.project_id}.{dataset_id}.{table_name}')

        # call super to set up the client, create datasets, and create
        # empty test tables
        # NOTE:  does not create empty sandbox tables.
        super().setUpClass()

    def setUp(self):
        """
        Create common information for tests.

        Creates common expected parameter types from cleaned tables and a common
        fully qualified (fq) dataset name string used to load the data.
        """

        fq_dataset_name = self.fq_table_names[0].split('.')
        self.fq_dataset_name = '.'.join(fq_dataset_name[:-1])

        fq_sandbox_name = self.fq_sandbox_table_names[0].split('.')
        self.fq_sandbox_name = '.'.join(fq_sandbox_name[:-1])

        super().setUp()

    def test_ehr_submission_data_cutoff(self):
        """
        Validates pre conditions, tests execution, and post conditions based on the load
        statements and the tables_and_counts variable.
        """

        queries = []

        visit_occurrence_tmpl = self.jinja_env.from_string("""
            INSERT INTO `{{fq_dataset_name}}.{{cdm_table}}`
            (visit_occurrence_id, person_id, visit_concept_id, visit_start_date, 
                visit_start_datetime, visit_end_date, visit_end_datetime, visit_type_concept_id)
            VALUES
            (111, 222, 3, date('2018-03-06'), timestamp('2018-03-06 11:00:00'), 
                date('2018-03-07'), timestamp('2018-03-07 11:00:00'), 4),
            (222, 333, 3, date('2019-03-06'), timestamp('2019-03-06 11:00:00'), 
                date('2019-03-07'), timestamp('2019-03-07 11:00:00'), 4),
            (333, 444, 3, date('2020-03-06'), timestamp('2020-03-06 11:00:00'), 
                date('2020-03-07'), timestamp('2020-03-07 11:00:00'), 4),
            (444, 555, 3, date('2021-03-06'), timestamp('2021-03-06 11:00:00'), 
                date('2021-03-07'), timestamp('2021-03-07 11:00:00'), 4),
            (555, 666, 3, date('2022-03-06'), timestamp('2022-03-06 11:00:00'), 
                date('2022-03-07'), timestamp('2022-03-07 11:00:00'), 4)
            """).render(fq_dataset_name=self.fq_dataset_name,
                        cdm_table=common.VISIT_OCCURRENCE)
        queries.append(visit_occurrence_tmpl)

        # Because the perosn table is not being cleaned in this cleaning rule, all of these values
        # will be returned, nothing will be sandboxed and dropped
        person_tmpl = self.jinja_env.from_string("""
        INSERT INTO `{{fq_dataset_name}}.{{cdm_table}}`
        (person_id, gender_concept_id, year_of_birth, 
            birth_datetime, race_concept_id, ethnicity_concept_id)
        VALUES
        (111, 2, 2018, timestamp('2018-03-06 11:00:00'), 3, 4),
        (222, 2, 2019, timestamp('2019-03-06 11:00:00'), 3, 4),
        (333, 2, 2020, timestamp('2020-03-06 11:00:00'), 3, 4),
        (444, 2, 2021, timestamp('2021-03-06 11:00:00'), 3, 4),
        (555, 2, 2022, timestamp('2022-03-06 11:00:00'), 3, 4)
        """).render(fq_dataset_name=self.fq_dataset_name,
                    cdm_table=common.PERSON)
        queries.append(person_tmpl)

        self.load_test_data(queries)

        table_and_counts = [{
            'fq_table_name':
                '.'.join([self.fq_dataset_name, 'visit_occurrence']),
            'fq_sandbox_table_name':
                f'{self.fq_sandbox_name}.{self.rule_instance.sandbox_table_for(common.VISIT_OCCURRENCE)}',
            'loaded_ids': [111, 222, 333, 444, 555],
            'sandboxed_ids': [111, 222, 333],
            'fields': [
                'visit_occurrence_id', 'person_id', 'visit_occurrence_id',
                'visit_start_date', 'visit_start_datetime', 'visit_end_date',
                'visit_end_datetime', 'visit_type_concept_id'
            ],
            'cleaned_values': [
                (444, 555, parse('2021-03-06').date(),
                 parse('2021-03-06 11:00:00'), parse('2021-03-07').date(),
                 parse('2021-03-07 11:00:00'), 4),
                (555, 666, 3, parse('2022-03-06').date(),
                 parse('2022-03-06 11:00:00'), parse('2022-03-07').date,
                 parse('2022-03-07 11:00:00'), 4)
            ]
        }, {
            'fq_table_name':
                '.'.join([self.fq_dataset_name, 'person']),
            'fq_sandbox_table_name':
                f'{self.fq_sandbox_name}.{self.rule_instance.sandbox_table_for(common.PERSON)}',
            'loaded_ids': [111, 222, 333, 444, 555],
            'sandboxed_ids': [],
            'fields': [
                'person_id', 'gender_concept_id', 'year_of_birth',
                'birth_datetime', 'race_concept_id', 'ethnicity_concept_id'
            ],
            'cleaned_values': [
                (111, 2, 2018, parse('2018-03-06 11:00:00'), 3, 4),
                (222, 2, 2019, parse('2019-03-06 11:00:00'), 3, 4),
                (333, 2, 2020, parse('2020-03-06 11:00:00'), 3, 4),
                (444, 2, 2021, parse('2021-03-06 11:00:00'), 3, 4),
                (555, 2, 2022, parse('2022-03-06 11:00:00'), 3, 4)
            ]
        }]
