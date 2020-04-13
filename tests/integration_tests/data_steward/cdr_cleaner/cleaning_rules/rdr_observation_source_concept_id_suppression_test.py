"""
Integration Test for the rdr_observation_source_concept_id_suppression module.

Remove three irrelevant observation_source_concept_ids from the RDR dataset.

Original Issue:  DC-734 implements integration tests for DC-529

The intent is to remove PPI records from the observation table in the RDR
export where observation_source_concept_id in (43530490, 43528818, 43530333).
The records for removal should be archived in the dataset sandbox.  It should
also ensure that records that have null values or do not match the specified
ids are not removed.
"""
# Python imports
import os

# Third party imports
from jinja2 import Template

# Project imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.rdr_observation_source_concept_id_suppression import ObservationSourceConceptIDRowSuppression
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest


class ObservationSourceConceptIDRowSuppressionTest(BaseTest.DropRowsTestBase):
    insert_fake_participants_tmpls = [
        Template("""
    INSERT INTO `{{fq_table_name}}` (observation_id, person_id, observation_concept_id, observation_date, observation_type_concept_id, observation_source_concept_id)
    VALUES
      (801, 337361, 1585899, date('2016-05-01'), 45905771, {{age_prediabetes}}),
      (802, 129884, 1585899, date('2016-05-01'), 45905771, {{meds_prediabetes}}),
      (803, 337361, 1585899, date('2016-05-01'), 45905771, {{now_prediabetes}}),
      (804, 129884, 1585899, date('2016-05-01'), 45905771, null),
      (805, 337361, 1585899, date('2016-05-01'), 45905771, 45)
    """)
    ]

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
        dataset_id = os.environ.get('RDR_DATASET_ID')
        sandbox_id = dataset_id + '_sandbox'

        cls.query_class = ObservationSourceConceptIDRowSuppression(
            project_id, dataset_id, sandbox_id)

        sb_table_names = cls.query_class.get_sandbox_tablenames()
        for table_name in sb_table_names:
            cls.fq_sandbox_table_names.append(
                f'{project_id}.{sandbox_id}.{table_name}')

        # set the table names and initial expected row counts after loading
        table_name = 'observation'
        cls.tables_and_counts = [{
            'name':
                table_name,
            'fq_table_name':
                f"{project_id}.{dataset_id}.{table_name}",
            # I am using the 0 position here because I know there is only one sandbox table
            'fq_sandbox_table_name':
                f"{project_id}.{sandbox_id}.{sb_table_names[0]}",
            'loaded_ids': [801, 802, 803, 804, 805],
            'sandboxed_ids': [801, 802, 803],
            'cleaned_ids': [804, 805]
        }]

        cls.fq_table_names = [f"{project_id}.{dataset_id}.{table_name}"]
        # call super to set up the client, create datasets, and create
        # empty test tables
        # NOTE:  does not create empty sandbox tables.
        super().setUpClass()

    def setUp(self):
        """
        Add data to the tables for the rule to run on.
        """
        age_prediabetes = 43530490
        meds_prediabetes = 43528818
        now_prediabetes = 43530333

        load_statements = []
        # create the string(s) to load the data
        for tmpl in self.insert_fake_participants_tmpls:
            query = tmpl.render(fq_table_name=self.fq_table_names[0],
                                age_prediabetes=age_prediabetes,
                                meds_prediabetes=meds_prediabetes,
                                now_prediabetes=now_prediabetes)
            load_statements.append(query)

        self.load_test_data(load_statements)
