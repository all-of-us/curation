# Python imports
import os
from unittest import TestCase

# Third-party imports
from google.cloud.bigquery import Table
from pandas import testing

# Project imports
import app_identity
from tools import recreate_person
from utils import bq

POPULATE_PERSON = """
INSERT INTO {{person.project}}.{{person.dataset_id}}.person 
(person_id, gender_concept_id, year_of_birth, race_concept_id, ethnicity_concept_id)
VALUES 
    (1, 5, 1980, 4, 9),
    (2, 5, 1990, 5, 8),
    (3, 6, 1970, 6, 7),
    (4, 6, 2000, 7, 8),
    (5, 6, 1995, 8, 9)
"""

POPULATE_PERSON_EXT = """
INSERT INTO {{person_ext.project}}.{{person_ext.dataset_id}}.person_ext
(person_id, state_of_residence_concept_id, state_of_residence_source_value)
VALUES 
    (1, 1585266, 'PIIState_CA'),
    (2, 1585297, 'PIIState_NY'),
    (3, 1585286, 'PIIState_MA'),
    (4, 1585297, 'PIIState_NY'),
    (5, 1585266, 'PIIState_CA')
"""

POPULATE_PERSON_TMPL = bq.JINJA_ENV.from_string(POPULATE_PERSON)
POPULATE_PERSON_EXT_TMPL = bq.JINJA_ENV.from_string(POPULATE_PERSON_EXT)


class RecreatePersonTest(TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = app_identity.get_application_id()
        self.dataset_id = os.environ.get('UNIONED_DATASET_ID')

        self.fq_person = f'{self.project_id}.{self.dataset_id}.person'
        self.fq_person_ext = f'{self.project_id}.{self.dataset_id}.person_ext'
        self.table_ids = [self.fq_person, self.fq_person_ext]

        self.person = Table.from_string(self.fq_person)
        self.person_ext = Table.from_string(self.fq_person_ext)

        self.client = bq.get_client(self.project_id)
        for table in self.table_ids:
            self.client.delete_table(table, not_found_ok=True)
        bq.create_tables(self.client, self.project_id, self.table_ids)
        self.populate_tables()

    def populate_tables(self):
        query_job = self.client.query(query=POPULATE_PERSON_TMPL.render(
            person=self.person))
        query_job.result()
        query_job = self.client.query(query=POPULATE_PERSON_EXT_TMPL.render(
            person_ext=self.person_ext))
        query_job.result()

    def test_update_person(self):
        recreate_person.update_person(self.client, self.project_id,
                                      self.dataset_id)
        new_person_cols = (
            f'SELECT state_of_residence_concept_id, state_of_residence_source_value '
            f'FROM {self.project_id}.{self.dataset_id}.person')
        person_ext_cols = (
            f'SELECT state_of_residence_concept_id, state_of_residence_source_value '
            f'FROM {self.project_id}.{self.dataset_id}.person_ext')
        new_person_vals = self.client.query(new_person_cols).to_dataframe()
        person_ext_vals = self.client.query(person_ext_cols).to_dataframe()
        testing.assert_frame_equal(
            new_person_vals.set_index(
                'state_of_residence_concept_id').sort_index(),
            person_ext_vals.set_index(
                'state_of_residence_concept_id').sort_index())

    def tearDown(self) -> None:
        for table in self.table_ids:
            self.client.delete_table(table, not_found_ok=True)
