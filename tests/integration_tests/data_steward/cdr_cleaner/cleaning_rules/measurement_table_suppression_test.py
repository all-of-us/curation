"""
Integration Test for the measurement_table_suppression module.

Remove records containing implausible measurements data.

The intent is to remove measurement table records that offer little or no
data quality.
"""
# Python imports
import os

# Third party imports

# Project imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.measurement_table_suppression import (
    MeasurementRecordsSuppression)
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest


class MeasurementTableSuppressionTest(BaseTest.CleaningRulesTestBase):

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
        # intended to be run on the deid_base dataset.  The combined dataset
        # environment variable should be guaranteed to exist
        dataset_id = os.environ.get('COMBINED_DATASET_ID')
        cls.dataset_id = dataset_id
        sandbox_id = dataset_id + '_sandbox'
        cls.sandbox_id = sandbox_id

        cls.rule_instance = MeasurementRecordsSuppression(
            project_id, dataset_id, sandbox_id)

        sb_table_names = cls.rule_instance.get_sandbox_tablenames()
        for table_name in sb_table_names:
            cls.fq_sandbox_table_names.append(
                f'{project_id}.{sandbox_id}.{table_name}')

        cls.fq_table_names = [
            f"{project_id}.{dataset_id}.measurement",
            f"{project_id}.{dataset_id}.measurement_ext"
        ]
        cls.dataset_id = dataset_id
        # call super to set up the client, create datasets, and create
        # empty test tables
        # NOTE:  does not create empty sandbox tables.
        super().setUpClass()

    def test_duplicates(self):
        """
        Use the default drop rows test function.

        Validates pre-conditions, test execution and post conditions based on
        the load statements and the tables_and_counts variable.
        """
        insert_fake_measurements = [
            self.jinja_env.from_string("""
        INSERT INTO `{{project}}.{{dataset}}.measurement`
        (measurement_id, person_id, measurement_concept_id, measurement_date,
         measurement_type_concept_id, value_as_number, value_as_concept_id,
         measurement_source_concept_id, unit_concept_id, measurement_datetime)
        VALUES

          (801, 337361, 1585899, date('2016-05-01'), 45905771, 100, 100, 100, 100, TIMESTAMP("2020-01-01 05:30:00+00")),
          (802, 337361, 1585899, date('2016-05-01'), 45905771, 100, 100, 100, 100, TIMESTAMP("2020-01-01 05:30:00+00")),
          (803, 337361, 1585899, date('2016-05-01'), 45905771, 100, 100, 100, 100, TIMESTAMP("2020-01-01 05:30:00+00")),
          (804, 337361, 1585899, date('2016-05-01'), 45905771, 1000, 100, 100, 100, TIMESTAMP("2020-01-01 05:30:00+00"))
        """),
            self.jinja_env.from_string("""
        INSERT INTO `{{project}}.{{dataset}}.measurement_ext`
        (measurement_id, src_id)
        VALUES
          (801, 'EHR site 000'),
          (802, 'EHR site 000'),
          (803, 'EHR site 000'),
          (804, 'EHR site 000')
        """)
        ]

        load_statements = []
        for statement in insert_fake_measurements:
            sql = statement.render(project=self.project_id,
                                   dataset=self.dataset_id)
            load_statements.append(sql)

        self.load_test_data(load_statements)

        tables_and_counts = [{
            'name': self.fq_table_names[0].split('.')[-1],
            'fq_table_name': self.fq_table_names[0],
            'fq_sandbox_table_name': self.fq_sandbox_table_names[4],
            'fields': ['measurement_id'],
            'loaded_ids': [801, 802, 803, 804],
            'sandboxed_ids': [802, 803],
            'cleaned_values': [(801,), (804,)]
        }]

        self.default_test(tables_and_counts)

    def test_null_and_zero_suppressions(self):
        """
        Use the default drop rows test function.

        Validates pre-conditions, test execution and post conditions based on
        the load statements and the tables_and_counts variable.
        """
        insert_fake_measurements = [
            self.jinja_env.from_string("""
        INSERT INTO `{{project}}.{{dataset}}.measurement`
        (measurement_id, person_id, measurement_concept_id, measurement_date,
         measurement_type_concept_id, value_as_number, value_as_concept_id)
        VALUES
          (801, 337361, 1585899, date('2016-05-01'), 45905771, 9999999, null),
          (802, 337361, 1585899, date('2019-01-01'), 45905771, 000, 100),
          (803, 337321, 1585899, date('2019-01-01'), 45905771, 000, 100),
          (804, 337361, 1585899, date('2019-01-01'), 45905771, 1, 100),
          (805, 337361, 1585899, date('2019-01-01'), 45905771, null, null)
        """),
            self.jinja_env.from_string("""
        INSERT INTO `{{project}}.{{dataset}}.measurement_ext`
        (measurement_id, src_id)
        VALUES
          (801, 'EHR site 000'),
          (802, 'EHR site 111'),
          (803, 'EHR site 222'),
          (804, 'EHR site 222'),
          (805, 'EHR site 111')
        """)
        ]

        load_statements = []
        for statement in insert_fake_measurements:
            sql = statement.render(project=self.project_id,
                                   dataset=self.dataset_id)
            load_statements.append(sql)

        self.load_test_data(load_statements)

        tables_and_counts = [{
            'name': self.fq_table_names[0].split('.')[-1],
            'fq_table_name': self.fq_table_names[0],
            'fq_sandbox_table_name': self.fq_sandbox_table_names[0],
            'fields': ['measurement_id', 'value_as_number'],
            'loaded_ids': [801, 802, 803, 804, 805],
            'sandboxed_ids': [801],
            'cleaned_values': [(802, 0), (803, 0), (804, 1)]
        }, {
            'name': self.fq_table_names[0].split('.')[-1],
            'fq_table_name': self.fq_table_names[0],
            'fq_sandbox_table_name': self.fq_sandbox_table_names[2],
            'fields': ['measurement_id', 'value_as_number'],
            'loaded_ids': [801, 802, 803, 804, 805],
            'sandboxed_ids': [802],
            'cleaned_values': [(802, 0), (803, 0), (804, 1)]
        }, {
            'name': self.fq_table_names[0].split('.')[-1],
            'fq_table_name': self.fq_table_names[0],
            'fq_sandbox_table_name': self.fq_sandbox_table_names[3],
            'fields': ['measurement_id', 'value_as_number'],
            'loaded_ids': [801, 802, 803, 804, 805],
            'sandboxed_ids': [801, 805],
            'cleaned_values': [(802, 0), (803, 0), (804, 1)]
        }]

        self.default_test(tables_and_counts)
