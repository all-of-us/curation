"""
Integration test for the validate_email module

Ensures that emails are correctly identified as matches and non-matches
between EHR and RDR.
"""

# Python imports
import os
from unittest import TestCase

# Third party imports
from google.cloud.bigquery import DatasetReference, Table, TimePartitioning, TimePartitioningType

# Project imports
import bq_utils
from utils import bq
from tests import test_util
from app_identity import PROJECT_ID
from common import JINJA_ENV, PS_API_VALUES, PII_EMAIL
from validation.participants.validate import identify_rdr_ehr_match
from constants.validation.participants.identity_match import IDENTITY_MATCH_TABLE
import resources

POPULATE_PS_VALUES = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{drc_dataset_id}}.{{ps_values_table_id}}` 
(person_id, email)
VALUES 
    (1, 'john@gmail.com'),
    (2, 'rebecca@gmail.com'),
    (3, 'samwjeo'),
    (4,'chris@gmail.com')
""")

POPULATE_ID_MATCH = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{drc_dataset_id}}.{{id_match_table_id}}` 
(person_id, email, algorithm)
VALUES 
    (1, 'missing_ehr', 'no'),
    (2, 'missing_ehr', 'no'),
    (3, 'missing_ehr', 'no'),
    (4,'missing_ehr', 'no')
""")

ID_MATCH_CONTENT_QUERY = JINJA_ENV.from_string("""
    SELECT
        *
    FROM `{{project_id}}.{{drc_dataset_id}}.{{id_match_table_id}}`
""")


class ValidateTest(TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = os.environ.get(PROJECT_ID)
        self.dataset_id = os.environ.get('COMBINED_DATASET_ID')
        self.dataset_ref = DatasetReference(self.project_id, self.dataset_id)
        self.client = bq.get_client(self.project_id)

        self.hpo_id = 'fake_site'
        self.id_match_table_id = f'{IDENTITY_MATCH_TABLE}_{self.hpo_id}'
        self.ps_values_table_id = f'{PS_API_VALUES}_{self.hpo_id}'
        self.pii_email_table_id = f'{self.hpo_id}_pii_email'

        # Create and populate the ps_values site table

        schema = resources.fields_for(f'{PS_API_VALUES}')
        table = Table(
            f'{self.project_id}.{self.dataset_id}.{self.ps_values_table_id}',
            schema=schema)
        table.time_partitioning = TimePartitioning(
            type_=TimePartitioningType.HOUR)
        table = self.client.create_table(table)

        populate_query = POPULATE_PS_VALUES.render(
            project_id=self.project_id,
            drc_dataset_id=self.dataset_id,
            ps_values_table_id=self.ps_values_table_id)
        job = self.client.query(populate_query)
        job.result()

        # Create and populate the drc_id_match_table

        schema = resources.fields_for(f'{IDENTITY_MATCH_TABLE}')
        table = Table(
            f'{self.project_id}.{self.dataset_id}.{self.id_match_table_id}',
            schema=schema)
        table.time_partitioning = TimePartitioning(
            type_=TimePartitioningType.HOUR)
        table = self.client.create_table(table)

        populate_query = POPULATE_ID_MATCH.render(
            project_id=self.project_id,
            drc_dataset_id=self.dataset_id,
            id_match_table_id=self.id_match_table_id)
        job = self.client.query(populate_query)
        job.result()

        # Create and populate pii_email table

        schema = resources.fields_for(f'{PII_EMAIL}')
        table = Table(
            f'{self.project_id}.{self.dataset_id}.{self.pii_email_table_id}',
            schema=schema)
        table.time_partitioning = TimePartitioning(
            type_=TimePartitioningType.HOUR)
        table = self.client.create_table(table)

    def test_identify_rdr_ehr_email_match(self):

        POPULATE_PII_EMAILS = JINJA_ENV.from_string("""
        INSERT INTO `{{project_id}}.{{drc_dataset_id}}.{{pii_email_table_id}}` 
        (person_id, email)
        VALUES 
            (1, 'john2@gmail.com'), -- wrong email (non_match) --
            (2, 'REBECCA@gmail.com'), -- capitalized characters (match) --
            (4, '   chris@GMAIL.com    ') -- whitespace padding (match) --
        """)

        populate_query = POPULATE_PII_EMAILS.render(
            project_id=self.project_id,
            drc_dataset_id=self.dataset_id,
            pii_email_table_id=self.pii_email_table_id)
        job = self.client.query(populate_query)
        job.result()

        # Execute email match
        identify_rdr_ehr_match(self.client,
                               self.project_id,
                               self.hpo_id,
                               self.dataset_id,
                               drc_dataset_id=self.dataset_id)

        # Subset of id match fields to test
        subset_fields = ['person_id', 'email', 'algorithm']

        expected = [{
            'person_id': 1,
            'email': 'no_match',
            'algorithm': 'yes'
        }, {
            'person_id': 2,
            'email': 'match',
            'algorithm': 'yes'
        }, {
            'person_id': 3,
            'email': 'missing_ehr',
            'algorithm': 'yes'
        }, {
            'person_id': 4,
            'email': 'match',
            'algorithm': 'yes'
        }]

        content_query = ID_MATCH_CONTENT_QUERY.render(
            project_id=self.project_id,
            drc_dataset_id=self.dataset_id,
            id_match_table_id=self.id_match_table_id)

        content_job = self.client.query(content_query)
        contents = list(content_job.result())
        actual = [dict(row.items()) for row in contents]
        actual = [{
            key: value for key, value in row.items() if key in subset_fields
        } for row in actual]

        self.assertCountEqual(actual, expected)

    def tearDown(self):
        test_util.delete_all_tables(self.dataset_id)