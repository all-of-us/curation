# Python imports
import os
from unittest import TestCase

# Third-party imports
import google.cloud.bigquery as gbq
from pandas import testing

# Project imports
import app_identity
import bq_utils
from tools import recreate_person
from utils import bq
from common import JINJA_ENV

POPULATE_PERSON = JINJA_ENV.from_string("""
INSERT INTO `{{person.project}}.{{person.dataset_id}}.person` 
(person_id, gender_concept_id, year_of_birth, race_concept_id, ethnicity_concept_id)
VALUES 
    (1, 5, 1980, 4, 9),
    (2, 5, 1990, 5, 8),
    (3, 6, 1970, 6, 7),
    (4, 6, 2000, 7, 8),
    (5, 6, 1995, 8, 9)
""")

POPULATE_PERSON_EXT = JINJA_ENV.from_string("""
INSERT INTO `{{person_ext.project}}.{{person_ext.dataset_id}}.person_ext`
(person_id, state_of_residence_concept_id, state_of_residence_source_value, sex_at_birth_concept_id, 
    sex_at_birth_source_concept_id, sex_at_birth_source_value)
VALUES 
    (1, 1585266, 'PIIState_CA', 123, 101, 'gender'),
    (2, 1585297, 'PIIState_NY', 234, 202, 'gender'),
    (3, 1585286, 'PIIState_MA', 345, 303, 'gender'),
    (4, 1585297, 'PIIState_NY', 456, 404, 'gender'),
    (5, 1585266, 'PIIState_CA', 567, 505, 'gender')
""")


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
        self.table_ids = [self.fq_person_ext, self.fq_person]

        self.person = gbq.Table.from_string(self.fq_person)
        self.person_ext = gbq.Table.from_string(self.fq_person_ext)

        self.client = bq.get_client(self.project_id)
        self.tearDown()
        for fq_table in self.table_ids:
            table_id = fq_table.split('.')[2]
            if table_id == 'person':
                bq_utils.create_standard_table(table_id,
                                               table_id,
                                               drop_existing=True,
                                               dataset_id=self.dataset_id)
            else:
                bq.create_tables(self.client,
                                 self.project_id, [fq_table],
                                 exists_ok=True)

        self.populate_tables()

    def populate_tables(self):
        query_job = self.client.query(query=POPULATE_PERSON.render(
            person=self.person))
        query_job.result()
        query_job = self.client.query(query=POPULATE_PERSON_EXT.render(
            person_ext=self.person_ext))
        query_job.result()

    def test_update_person(self):
        recreate_person.update_person(self.client, self.project_id,
                                      self.dataset_id)
        new_person_cols = (
            f'SELECT state_of_residence_concept_id, state_of_residence_source_value, '
            f'sex_at_birth_concept_id, sex_at_birth_source_concept_id, sex_at_birth_source_value '
            f'FROM {self.project_id}.{self.dataset_id}.person')
        person_ext_cols = (
            f'SELECT state_of_residence_concept_id, state_of_residence_source_value, '
            f'sex_at_birth_concept_id, sex_at_birth_source_concept_id, sex_at_birth_source_value '
            f'FROM {self.project_id}.{self.dataset_id}.person_ext')
        new_person_vals = self.client.query(new_person_cols).to_dataframe()
        person_ext_vals = self.client.query(person_ext_cols).to_dataframe()
        testing.assert_frame_equal(
            new_person_vals.set_index(
                ['state_of_residence_concept_id',
                 'sex_at_birth_concept_id']).sort_index(),
            person_ext_vals.set_index(
                ['state_of_residence_concept_id',
                 'sex_at_birth_concept_id']).sort_index())

    def tearDown(self) -> None:
        for table in self.table_ids:
            self.client.delete_table(table, not_found_ok=True)
