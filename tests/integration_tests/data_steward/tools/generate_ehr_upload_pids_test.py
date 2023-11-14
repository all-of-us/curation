# Python imports
import unittest
import mock

# Project imports
import app_identity
from common import (EHR_UPLOAD_PIDS, JINJA_ENV, OBSERVATION, PERSON,
                    UNIONED_DATASET_ID)
from gcloud.bq import BigQueryClient
from constants.utils.bq import HPO_SITE_ID_MAPPINGS_TABLE_ID
from resources import fields_for
from tools import generate_ehr_upload_pids as eup

INSERT_HPO_SITE_ID_MAPPINGS = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{dataset_id}}.{{table}}`
VALUES
    ('Fake Org', 'FAKE', 'Fake site', 1),
    ('NY Org', 'NYC', 'Site in NY', 2),
    ('CHS Org', 'CHS', 'CHS site', 3),
    ('Pitt Org', 'Pitt', 'Pitt site', 4)
""")

INSERT_NYC_PERSON = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{dataset_id}}.nyc_person` (
    person_id, gender_concept_id, year_of_birth, birth_datetime, 
    race_concept_id, ethnicity_concept_id
) VALUES
    (1, 0, 2000, '2000-01-01 01:00:00 UTC', 0, 0),
    (2, 0, 2000, '2000-01-01 01:00:00 UTC', 0, 0),
    (3, 0, 2000, '2000-01-01 01:00:00 UTC', 0, 0)
""")

INSERT_NYC_OBSERVATION = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{dataset_id}}.nyc_observation` (
    observation_id, person_id, observation_concept_id, observation_date, 
    observation_datetime, observation_type_concept_id
) VALUES
    (101, 1, 0, '2000-01-01', '2001-01-01 01:00:00 UTC', 0),
    (102, 2, 0, '2000-01-01', '2001-01-01 01:00:00 UTC', 0)
""")

INSERT_PITT_PERSON = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{dataset_id}}.pitt_person` (
    person_id, gender_concept_id, year_of_birth, birth_datetime, 
    race_concept_id, ethnicity_concept_id
) VALUES
    (1, 0, 2000, '2000-01-01 01:00:00 UTC', 0, 0),
    (4, 0, 2000, '2000-01-01 01:00:00 UTC', 0, 0),
    (5, 0, 2000, '2000-01-01 01:00:00 UTC', 0, 0)
""")

INSERT_PITT_OBSERVATION = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{dataset_id}}.pitt_observation` (
    observation_id, person_id, observation_concept_id, observation_date, 
    observation_datetime, observation_type_concept_id
) VALUES
    (101, 1, 0, '2000-01-01', '2001-01-01 01:00:00 UTC', 0),
    (104, 4, 0, '2000-01-01', '2001-01-01 01:00:00 UTC', 0)
""")

INSERT_CHS_PERSON = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{dataset_id}}.chs_person` (
    person_id, gender_concept_id, year_of_birth, birth_datetime, 
    race_concept_id, ethnicity_concept_id
) VALUES
    (999, 0, 2000, '2000-01-01 01:00:00 UTC', 0, 0)
""")

ASSERT_QUERY = JINJA_ENV.from_string("""
SELECT person_id, HPO_ID
FROM {{project_id}}.{{dataset_id}}.{{table_id}}
""")


class GenerateEhrUploadPids(unittest.TestCase):
    dataset_id = UNIONED_DATASET_ID

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = app_identity.get_application_id()
        self.bq_client = BigQueryClient(self.project_id)

        self.fq_table_names = [
            f'{self.project_id}.{self.dataset_id}.{HPO_SITE_ID_MAPPINGS_TABLE_ID}'
        ] + [
            f'{self.project_id}.{self.dataset_id}.nyc_{table}'
            for table in eup.pid_tables
        ] + [
            f'{self.project_id}.{self.dataset_id}.pitt_{table}'
            for table in eup.pid_tables
        ] + [
            f'{self.project_id}.{self.dataset_id}.chs_{PERSON}',
            f'{self.project_id}.{self.dataset_id}.chs_{OBSERVATION}'
        ]

        self.bq_client.create_tables(
            self.fq_table_names,
            exists_ok=True,
            fields=[fields_for(HPO_SITE_ID_MAPPINGS_TABLE_ID)] +
            [fields_for(table) for table in eup.pid_tables] +
            [fields_for(table) for table in eup.pid_tables] +
            [fields_for(PERSON), fields_for(OBSERVATION)])

        insert_queries = [
            INSERT_HPO_SITE_ID_MAPPINGS, INSERT_NYC_PERSON,
            INSERT_NYC_OBSERVATION, INSERT_PITT_PERSON, INSERT_PITT_OBSERVATION,
            INSERT_CHS_PERSON
        ]

        for query in insert_queries:
            q = query.render(project_id=self.project_id,
                             dataset_id=self.dataset_id,
                             table=HPO_SITE_ID_MAPPINGS_TABLE_ID)
            _ = self.bq_client.query(q).result()

        super().setUp()

    @mock.patch("tools.generate_ehr_upload_pids.OPERATIONS_ANALYTICS",
                dataset_id)
    @mock.patch("tools.generate_ehr_upload_pids.LOOKUP_TABLES_DATASET_ID",
                dataset_id)
    def test_generate_ehr_upload_pids_query(self):
        """
        Test case to confirm the ehr_upload_pids is updated as expected.
        HPO sites in this test:
            FAKE: This one has no submission. Will not be included in the view.
            PITT: This one has incomplete submission. Will not be included in the view.
            NYC and CHS: This one has comoplete submission. Will be included in the view.    
        All the person_ids from NYC and CHS are included in the view.
        """
        expected = [{
            'person_id': 1,
            'HPO_ID': 'NYC'
        }, {
            'person_id': 2,
            'HPO_ID': 'NYC'
        }, {
            'person_id': 3,
            'HPO_ID': 'NYC'
        }, {
            'person_id': 1,
            'HPO_ID': 'Pitt'
        }, {
            'person_id': 4,
            'HPO_ID': 'Pitt'
        }, {
            'person_id': 5,
            'HPO_ID': 'Pitt'
        }]

        _ = eup.update_ehr_upload_pids_view(self.project_id,
                                            self.dataset_id,
                                            bq_client=self.bq_client)

        assert_query = ASSERT_QUERY.render(project_id=self.project_id,
                                           dataset_id=self.dataset_id,
                                           table_id=EHR_UPLOAD_PIDS)
        assert_job = self.bq_client.query(assert_query)
        actual_result = list(assert_job.result())
        actual = [dict(row.items()) for row in actual_result]
        self.assertCountEqual(actual, expected)

    def tearDown(self):
        for table in self.fq_table_names:
            self.bq_client.delete_table(table, not_found_ok=True)

        # NOTE This is a view. delete_table() can delete views.
        self.bq_client.delete_table(
            f'{self.project_id}.{self.dataset_id}.{EHR_UPLOAD_PIDS}',
            not_found_ok=True)

        super().tearDown()
