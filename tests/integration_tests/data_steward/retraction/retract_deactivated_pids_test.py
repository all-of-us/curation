# Python imports
import mock
import os
import unittest

import google.cloud.bigquery as gbq
# Third party imports
import pandas as pd

# Project Imports
import app_identity
from common import JINJA_ENV
from retraction import retract_deactivated_pids as rdp
from utils import sandbox as sb
from gcloud.bq import BigQueryClient

DEACTIVATED_PIDS = JINJA_ENV.from_string("""
INSERT INTO `{{deact_table.project}}.{{deact_table.dataset_id}}.{{deact_table.table_id}}` 
(person_id, suspension_status, deactivated_datetime) 
VALUES
(1,'NO_CONTACT','2009-07-25 01:00:00 UTC'),
(2,'NO_CONTACT','2009-03-14 01:00:00 UTC'),
(3,'NO_CONTACT','2009-11-18 01:00:00 UTC'),
(4,'NO_CONTACT','2009-11-25 01:00:00 UTC'),
(5,'NO_CONTACT','2009-09-20 01:00:00 UTC'),
(6,'NO_CONTACT','2009-10-06 01:00:00 UTC')
""")

TABLE_ROWS = {
    'person':
        JINJA_ENV.from_string("""
INSERT INTO `{{table.project}}.{{table.dataset_id}}.{{table.table_id}}`
(person_id, gender_concept_id, year_of_birth, birth_datetime, race_concept_id, ethnicity_concept_id)
VALUES
(1,8507,1989,'1989-07-25 01:00:00 UTC', 8527, 38003563),
(2,8507,1975,'1975-03-14 02:00:00 UTC', 8527, 38003564),
(3,8507,1981,'1981-11-18 05:00:00 UTC', 8527, 38003564),
(4,8507,1991,'1991-11-25 08:00:00 UTC', 8527, 38003564),
(5,8507,2001,'2001-09-20 11:00:00 UTC', 8527, 38003564),
(6,8507,1979,'1979-10-06 11:00:00 UTC', 8527, 38003564)
"""),
    'observation':
        JINJA_ENV.from_string("""
INSERT INTO `{{table.project}}.{{table.dataset_id}}.{{table.table_id}}`
(observation_id, person_id, observation_concept_id, observation_date, observation_datetime, observation_type_concept_id)
VALUES
(1001,1,0,'2008-07-25','2008-07-25 01:00:00 UTC',45905771),
(1005,2,0,'2008-03-14','2008-03-14 02:00:00 UTC',45905771),
(1002,3,0,'2009-11-18','2009-11-18 05:00:00 UTC',45905771),
(1004,4,0,'2009-11-25','2009-11-25 08:00:00 UTC',45905771),
(1003,5,0,'2010-09-20','2010-09-20 11:00:00 UTC',45905771),
(1006,3,0,'2009-11-18','2009-11-18 00:30:00 UTC',45905771),
(1007,4,0,'2009-11-25',NULL,45905771)
"""),
    'death':
        JINJA_ENV.from_string("""
INSERT INTO `{{table.project}}.{{table.dataset_id}}.{{table.table_id}}`
(person_id, death_date, death_datetime, death_type_concept_id)
VALUES
(2,'2008-03-12','2008-03-12 05:00:00 UTC',8),
(3,'2011-01-18','2011-01-18 05:00:00 UTC',6),
(4,'2009-11-25','2009-11-25 00:30:00 UTC',6),
(5,'2009-09-20',NULL,6)
"""),
    'drug_exposure':
        JINJA_ENV.from_string("""
INSERT INTO `{{table.project}}.{{table.dataset_id}}.{{table.table_id}}`
(drug_exposure_id, person_id, drug_concept_id, drug_exposure_start_date, drug_exposure_start_datetime, 
drug_exposure_end_date, drug_exposure_end_datetime, verbatim_end_date, drug_type_concept_id)
VALUES
(2002,1,50,'2008-06-05','2008-06-05 01:00:00 UTC','2010-07-05','2008-06-05 01:00:00 UTC','2011-04-11',87),
(2003,2,21,'2008-11-22','2008-11-22 02:00:00 UTC',NULL,NULL,'2010-06-18',51),
(2004,3,5241,'2009-08-03','2009-08-03 05:00:00 UTC',NULL,NULL,'2009-11-26',2754),
(2005,4,76536,'2010-02-17','2010-02-17 08:00:00 UTC',NULL,NULL,'2008-03-04',24),
(2006,5,274,'2009-04-19','2009-04-19 11:00:00 UTC',NULL,'2010-11-19 01:00:00 UTC','2011-10-22',436),
(2007,3,50,'2009-11-18','2009-11-18 00:30:00 UTC',NULL,NULL,'2009-11-18',87),
(2008,4,50,'2009-11-25','2009-11-25 00:30:00 UTC','2009-11-25','2009-11-25 00:45:00 UTC','2009-11-24',87),
(2009,6,50,'2009-10-06','2009-10-06 01:30:00 UTC',NULL,NULL,'2009-10-05',87),
(2010,5,274,'2009-09-20','2009-09-20 11:00:00 UTC', '2009-09-20', NULL, NULL, 436)
""")
}

MAPPING_TABLE_ROWS = {
    '_mapping_observation':
        JINJA_ENV.from_string("""
INSERT INTO `{{table.project}}.{{table.dataset_id}}.{{table.table_id}}`
(observation_id, src_hpo_id)
VALUES
(1001,'hpo_1'),
(1002,'hpo_2'),
(1003,'hpo_3'),
(1004,'PPI/PM'),
(1005,'hpo_4'),
(1006,'hpo_4')
""")
}

EXT_TABLE_ROWS = {
    'drug_exposure_ext':
        JINJA_ENV.from_string("""
INSERT INTO `{{table.project}}.{{table.dataset_id}}.{{table.table_id}}`
(drug_exposure_id, src_id)
VALUES
(2002,'PPI/PM'),
(2003,'EHR Site 50'),
(2004,'EHR Site 22'),
(2005,'EHR Site 9'),
(2006,'EHR Site 17')
""")
}


class RetractDeactivatedEHRDataBqTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = app_identity.get_application_id()
        if 'test' not in self.project_id:
            raise RuntimeError(
                f"Make sure the project_id is set to test. Project_id is {self.project_id}"
            )
        self.dataset_id = os.environ.get('UNIONED_DATASET_ID')
        self.deact_dataset_id = os.environ.get('COMBINED_DATASET_ID')
        self.deact_table = f'{self.project_id}.{self.deact_dataset_id}._deactivated_participants'
        self.bq_client = BigQueryClient(self.project_id)
        self.bq_sandbox_dataset_id = sb.get_sandbox_dataset_id(self.dataset_id)
        self.tables = {**TABLE_ROWS, **MAPPING_TABLE_ROWS, **EXT_TABLE_ROWS}
        self.setup_data()

    def setup_data(self):
        self.tearDown()
        # setup deactivated participants table
        deact_table_ref = gbq.TableReference.from_string(self.deact_table)
        self.bq_client.create_tables([self.deact_table], exists_ok=True)
        job_config = gbq.QueryJobConfig()
        job = self.bq_client.query(
            DEACTIVATED_PIDS.render(deact_table=deact_table_ref), job_config)
        job.result()

        # create omop tables and mapping/ext tables
        for table in self.tables:
            fq_table = f'{self.project_id}.{self.dataset_id}.{table}'
            self.bq_client.create_tables([fq_table], exists_ok=True)
            table_ref = gbq.TableReference.from_string(fq_table)
            job_config = gbq.QueryJobConfig()
            job = self.bq_client.query(
                self.tables[table].render(table=table_ref), job_config)
            job.result()

    @mock.patch('retraction.retract_utils.is_deid_label_or_id')
    def test_queries_to_retract_from_fake_dataset(self, mock_deid):
        mock_deid.return_value = False
        rdp.run_deactivation(self.bq_client, self.project_id, [self.dataset_id],
                             self.deact_table)
        person_cols = [
            'person_id', 'gender_concept_id', 'year_of_birth', 'birth_datetime',
            'race_concept_id', 'ethnicity_concept_id'
        ]
        person_data = [
            (1, 8507, 1989, '1989-07-25 01:00:00 UTC', 8527, 38003563),
            (2, 8507, 1975, '1975-03-14 02:00:00 UTC', 8527, 38003564),
            (3, 8507, 1981, '1981-11-18 05:00:00 UTC', 8527, 38003564),
            (4, 8507, 1991, '1991-11-25 08:00:00 UTC', 8527, 38003564),
            (5, 8507, 2001, '2001-09-20 11:00:00 UTC', 8527, 38003564),
            (6, 8507, 1979, '1979-10-06 11:00:00 UTC', 8527, 38003564)
        ]
        person_df = pd.DataFrame.from_records(person_data, columns=person_cols)
        observation_cols = [
            'observation_id', 'person_id', 'observation_concept_id',
            'observation_date', 'observation_datetime',
            'observation_type_concept_id'
        ]
        observation_data = [
            (1001, 1, 0, '2008-07-25', '2008-07-25 01:00:00 UTC', 45905771),
            (1005, 2, 0, '2008-03-14', '2008-03-14 02:00:00 UTC', 45905771),
            (1006, 3, 0, '2009-11-18', '2009-11-18 00:30:00 UTC', 45905771)
        ]
        observation_df = pd.DataFrame.from_records(observation_data,
                                                   columns=observation_cols)
        drug_exposure_data = [
            (2008, 4, 50, '2009-11-25', '2009-11-25 00:30:00 UTC', '2009-11-25',
             '2009-11-25 00:45:00 UTC', '2009-11-24', 87)
        ]
        drug_exposure_cols = [
            'drug_exposure_id', 'person_id', 'drug_concept_id',
            'drug_exposure_start_date', 'drug_exposure_start_datetime',
            'drug_exposure_end_date', 'drug_exposure_end_datetime',
            'verbatim_end_date', 'drug_type_concept_id'
        ]
        drug_exposure_df = pd.DataFrame.from_records(drug_exposure_data,
                                                     columns=drug_exposure_cols)
        death_data = [(2, '2008-03-12', '2008-03-12 05:00:00 UTC', 8),
                      (4, '2009-11-25', '2009-11-25 00:30:00 UTC', 6)]
        death_cols = [
            'person_id', 'death_date', 'death_datetime', 'death_type_concept_id'
        ]
        death_df = pd.DataFrame.from_records(death_data, columns=death_cols)
        expected_dict = {
            'person': person_df,
            'observation': observation_df,
            'drug_exposure': drug_exposure_df,
            'death': death_df
        }
        for table in TABLE_ROWS:
            query = f"SELECT * FROM `{self.project_id}.{self.dataset_id}.{table}`"
            job_config = gbq.QueryJobConfig()
            job = self.bq_client.query(query, job_config)
            result_df = job.result().to_dataframe()
            result_df = result_df.dropna(how='all', axis=1)
            date_cols = [col for col in result_df.columns if 'date' in col]
            for date_col in date_cols:
                if 'datetime' in date_col:
                    result_df[date_col] = result_df[date_col].dt.strftime(
                        '%Y-%m-%d %H:%M:%S %Z')
                else:
                    result_df[date_col] = result_df[date_col].astype(str)
            actual = result_df.sort_values('person_id').set_index('person_id')
            expected = expected_dict[table].sort_values('person_id').set_index(
                'person_id')
            pd.testing.assert_frame_equal(actual, expected)

    def tearDown(self):
        for table in self.tables:
            fq_table = f'{self.project_id}.{self.dataset_id}.{table}'
            table_ref = gbq.TableReference.from_string(fq_table)
            self.bq_client.delete_table(table_ref, not_found_ok=True)
        self.bq_client.delete_table(self.deact_table, not_found_ok=True)
        self.bq_client.delete_dataset(self.bq_sandbox_dataset_id,
                                      delete_contents=True,
                                      not_found_ok=True)
