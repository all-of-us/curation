"""
Integration test for the validate module for emails, phone_numbers, sex, and addresses.

Ensures that names, emails, phone numbers, date_of_birth, sex, and addresses are correctly identified
as matches and non-matches between EHR and RDR.
"""

# Python imports
import os
from unittest import TestCase

# Third party imports
from google.cloud.bigquery import DatasetReference, Table, TimePartitioning, TimePartitioningType, SchemaField

# Project imports
from gcloud.bq import BigQueryClient
from tests import test_util
import app_identity
from common import JINJA_ENV, PS_API_VALUES, PII_EMAIL, PII_PHONE_NUMBER, PII_NAME, PII_ADDRESS
from validation.participants.create_update_drc_id_match_table import (
    create_drc_validation_table, populate_validation_table)
from validation.participants.validate import identify_rdr_ehr_match
from constants.validation.participants.identity_match import IDENTITY_MATCH_TABLE
import resources

person_schema = [
    SchemaField("person_id", "INTEGER", mode="REQUIRED"),
    SchemaField("gender_concept_id", "INTEGER", mode="REQUIRED"),
    SchemaField("birth_datetime", "TIMESTAMP", mode="REQUIRED"),
]

location_schema = [
    SchemaField("location_id", "INTEGER", mode="REQUIRED"),
    SchemaField("address_1", "STRING"),
    SchemaField("address_2", "STRING"),
    SchemaField("city", "STRING"),
    SchemaField("state", "STRING"),
    SchemaField("zip", "STRING"),
]

concept_schema = [
    SchemaField("concept_id", "INTEGER", mode="REQUIRED"),
    SchemaField("concept_name", "STRING", mode="REQUIRED"),
]

POPULATE_PS_VALUES = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{drc_dataset_id}}.{{ps_values_table_id}}`
(person_id, first_name, middle_name, last_name, street_address, street_address2, city, state, zip_code, email, phone_number, date_of_birth, sex)
VALUES
    (1, 'John', 'Jacob', 'Smith', '1 Government Dr', '', 'St. Louis', 'MO', '63110', 'john@gmail.com', '(123)456-7890', date('1978-10-01'), 'SexAtBirth_Female'),
    (2, 'Rebecca', 'Howard', 'Glass', '476 5th Ave', 'Apt 14A', 'New York', 'NY', '10018', 'rebecca@gmail.com', '1234567890', date('1984-10-23'), 'SexAtBirth_Male'),
    (3, 'Sam', 'Felix Rose', 'Smith', '1 2nd 3 4th St', 'APT 7D', 'Andersen AFB', 'ABCD', '96923', 'samwjeo', '123456-7890', date('2003-01-05'), 'SexAtBirth_SexAtBirthNoneOfThese'),
    (4, 'Chris', 'Arthur', 'Smith', '1234 4th ave', 'apt 15B', NULL, 'PIIState_PR', '00921', 'chris@gmail.com', '1234567890', date('2003-05-1'), 'SexAtBirth_Intersex'),
    (5, 'John', NULL, 'Doe', NULL, '915 PR-17, Apt 7D', 'Trenton', 'NJ', '08611', '  johndoe@gmail.com  ', '', date('1993-11-01'), 'PMI_Skip'),
    (6, 'Rebecca', '', 'Mayers-James', '1501 Riverplace Blvd', '', 'Jacksonville', 'FL', '32207', 'rebeccamayers@gmail.co', '814321-0987', NULL, 'PMI_PreferNotToAnswer'),
    (7, 'Leo', '', "O’Keefe", NULL, '', 'Cincinnati', 'OH', '45202', 'leo@yahoo.com', '0987654321', date('1981-10-01'), 'UNSET'),
    (8, '1ois', 'Frankl1n', 'Rhodes', '42 Nason St', 'Apt 21C', 'Maynard', 'MA', '01754', '', '1-800-800-0911', date('1999-12-01'), 'SexAtBirth_SexAtBirthNoneOfThese'),
    (9, 'Jack', 'Isaac', 'Dean', 'Test address 0000', '', 'Portland', 'OR', '97232', 'jd@gmail.com', '555-555-1234', date('2002-03-14'), 'SexAtBirth_Female')
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
        self.maxDiff = None
        self.project_id = app_identity.get_application_id()
        self.dataset_id = os.environ.get('COMBINED_DATASET_ID')
        self.dataset_ref = DatasetReference(self.project_id, self.dataset_id)
        self.bq_client = BigQueryClient(self.project_id)

        self.hpo_id = 'fake_site'
        self.id_match_table_id = f'{IDENTITY_MATCH_TABLE}_{self.hpo_id}'
        self.ps_values_table_id = f'{PS_API_VALUES}'
        self.pii_address_table_id = f'{self.hpo_id}_pii_address'
        self.pii_email_table_id = f'{self.hpo_id}_pii_email'
        self.pii_phone_number_table_id = f'{self.hpo_id}_pii_phone_number'
        self.pii_name_table_id = f'{self.hpo_id}_pii_name'
        self.person_table_id = f'{self.hpo_id}_person'
        self.location_table_id = f'{self.hpo_id}_location'
        self.fq_concept_table = f'{self.project_id}.{self.dataset_id}.concept'

        # Create and populate the ps_values site table

        schema = resources.fields_for(f'{PS_API_VALUES}')
        table = Table(
            f'{self.project_id}.{self.dataset_id}.{self.ps_values_table_id}',
            schema=schema)
        table = self.bq_client.create_table(table, exists_ok=True)

        populate_query = POPULATE_PS_VALUES.render(
            project_id=self.project_id,
            drc_dataset_id=self.dataset_id,
            ps_values_table_id=self.ps_values_table_id)
        job = self.bq_client.query(populate_query)
        job.result()

        # Create and populate pii_name, pii_email, pii_phone_number, and pii_address table

        schema = resources.fields_for(f'{PII_NAME}')
        table = Table(
            f'{self.project_id}.{self.dataset_id}.{self.pii_name_table_id}',
            schema=schema)
        table.time_partitioning = TimePartitioning(
            type_=TimePartitioningType.HOUR)
        table = self.bq_client.create_table(table, exists_ok=True)

        schema = resources.fields_for(f'{PII_EMAIL}')
        table = Table(
            f'{self.project_id}.{self.dataset_id}.{self.pii_email_table_id}',
            schema=schema)
        table.time_partitioning = TimePartitioning(
            type_=TimePartitioningType.HOUR)
        table = self.bq_client.create_table(table, exists_ok=True)

        schema = resources.fields_for(f'{PII_PHONE_NUMBER}')
        table = Table(
            f'{self.project_id}.{self.dataset_id}.{self.pii_phone_number_table_id}',
            schema=schema)
        table.time_partitioning = TimePartitioning(
            type_=TimePartitioningType.HOUR)
        table = self.bq_client.create_table(table, exists_ok=True)

        schema = resources.fields_for(f'{PII_ADDRESS}')
        table = Table(
            f'{self.project_id}.{self.dataset_id}.{self.pii_address_table_id}',
            schema=schema)
        table.time_partitioning = TimePartitioning(
            type_=TimePartitioningType.HOUR)
        table = self.bq_client.create_table(table, exists_ok=True)

        person_table = Table(
            f'{self.project_id}.{self.dataset_id}.{self.person_table_id}',
            schema=person_schema)
        person_table = self.bq_client.create_table(person_table, exists_ok=True)

        location_table = Table(
            f'{self.project_id}.{self.dataset_id}.{self.location_table_id}',
            schema=location_schema)
        location_table = self.bq_client.create_table(location_table,
                                                     exists_ok=True)

        concept_table = Table(f'{self.project_id}.{self.dataset_id}.concept',
                              schema=concept_schema)
        concept_table = self.bq_client.create_table(concept_table,
                                                    exists_ok=True)

        # Creates hpo_site identity match table after deleting existing
        self.bq_client.delete_table(
            f'{self.project_id}.{self.dataset_id}.{self.id_match_table_id}',
            not_found_ok=True)
        create_drc_validation_table(self.bq_client, self.id_match_table_id,
                                    self.dataset_id)

    def test_identify_rdr_ehr_match(self):

        POPULATE_PII_EMAILS = JINJA_ENV.from_string("""
        INSERT INTO `{{project_id}}.{{drc_dataset_id}}.{{pii_email_table_id}}`
        (person_id, email)
        VALUES
            (1, 'john2@gmail.com'), -- wrong email (non_match) --
            (2, 'REBECCA@gmail.com'), -- capitalized characters (match) --
            (4, '   chris@GMAIL.com    '), -- whitespace padding (match) --
            (5, 'johndoe@gmail.com'),
            (6, 'rebeccamayers@gmail.com'),
            (6, 'rebeccamayers@gmail.com'),
            (7, 'leo@gmail.com'),
            (8, 'claire@gmail.com')
        """)
        # person_id = 6 is duplicated but should not have any effect

        POPULATE_PII_PHONE_NUMBER = JINJA_ENV.from_string("""
                INSERT INTO `{{project_id}}.{{drc_dataset_id}}.{{pii_phone_number_table_id}}`
                (person_id, phone_number)
                VALUES
                    (1, '0123456789'), -- wrong phonenumber (non_match) --
                    (2, '1234567890'), -- normal 10 digit phone number (match) --
                    (4, '(123)456-7890'), -- formatted phone number (match) --
                    (5, ''),
                    (6, '8143210987'),
                    (7, '987654321'),
                    (8, '800-8000911')
                """)

        POPULATE_PII_NAME = JINJA_ENV.from_string("""
            INSERT INTO `{{project_id}}.{{drc_dataset_id}}.{{pii_name_table_id}}`
            (person_id, first_name, middle_name, last_name)
            VALUES
                (1, 'john', 'jacob', 'smith'),
                (2, 'rebecca', 'howard', 'glass'),
                (3, 'Sam', 'Felix-rose', 'Smith'), -- hyphentated middle name, still matches --
                (4, 'Kris', 'Arthur', 'Smith'), -- nonmatched first name --
                (5, 'John', 'Christian', 'Doe'), -- missing middle name in rdr --
                (6, 'Rebecca', '', 'MayersJames'),
                (7, 'Leo', '', "o'keefe"),
                (7, 'Leo', '', "o'keefe"),
                (8, 'lois', 'Franklin', 'Rhodes'), -- nonmatched first and middle name --
                (9, 'John', 'Moses', 'Dexter') -- all three names do not match --

        """)
        # person_id = 7 is duplicated but should not have any effect

        POPULATE_PII_ADDRESS = JINJA_ENV.from_string("""
            INSERT INTO `{{project_id}}.{{drc_dataset_id}}.{{pii_address_table_id}}`
            (person_id, location_id)
            VALUES
                (1, 11),
                (2, 12),
                (3, 13),
                (4, 14),
                (5, 15),
                (6, 16),
                (7, 17),
                (8, 18),
                (9, 19)
        """)

        POPULATE_PERSON_TABLE = JINJA_ENV.from_string("""
                INSERT INTO `{{project_id}}.{{drc_dataset_id}}.{{person_table_id}}`
                (person_id, gender_concept_id, birth_datetime)
                VALUES
                    (1, 8532, timestamp ('1978-10-01')),
                    (2, 8507, timestamp('1984-10-23')),
                    (3, 8551, timestamp ('2004-10-01')),
                    (4, 8521, timestamp ('2003-05-01')),
                    (5, 8570, timestamp ('2000-11-01')),
                    (6, 0, timestamp ('1954-01-01')),
                    (7, 4215271, timestamp ('1981-01-10')),
                    (8, 4214687, timestamp ('1999-12-1')),
                    (8, 4214687, timestamp ('1999-12-1'))
                """)
        # person_id = 8 is duplicated but should not affect anything
        # person_id = 9 is missing from ehr person table so should not validate

        POPULATE_LOCATION_TABLE = JINJA_ENV.from_string("""
        INSERT INTO `{{project_id}}.{{drc_dataset_id}}.{{location_table_id}}`
        (location_id, address_1, address_2, city, state, zip)
        VALUES
            (11, ' 1 government drive ', '', 'Saint Louis ', 'mo', '63110'),
            (12, 'Wrong street', 'Wrong apartment', 'Wrong city', 'NJ', '12345'),
            (13, '1st 2 3rd 4 Street', 'Apartment 7 D', 'Andersen Air Force Base', 'Gu', '  96923  '),
            (14, '1234 4th Ave, APT 15B', '', ' San Juan ', 'pr', '921'),
            (15, '50 Riverview Plaza', '915 pr-17, Apt 7-D', NULL, 'NJ', '08611-1234'),
            (15, '50 Riverview Plaza', '915 pr-17, Apt 7-D', NULL, 'NJ', '08611-1234'),
            (16, NULL, '', 'Jacksonville', 'FL', '32207  5678'),
            (17, '', NULL, 'Cincinnati', 'OH', '45202'),
            (18, '42  Nason   St', '', 'Maynard', 'MA', '01754')
        """)
        # location_id = 15 is duplicated but should not have any effect
        """
        Note: Each test entry is testing for the following test cases:
            11: street_1: match,       street_2: missing_rdr ('1 Government Dr', '') vs (' 1 government drive ', '')
            12: street_1: no_match,    street_2: no_match    ('476 5th Ave', 'Apt 14A') vs ('Wrong street', 'Wrong apartment')
            13: street_1: match,       street_2: match       ('1 2nd 3 4th St', 'APT 7D') vs ('1st 2 3rd 4 Street', 'Apartment 7 D')
            14: street_1: match,       street_2: match       ('1234 4th ave', 'apt 15B') vs ('1234 4th Ave, APT 15B', '')
            15: street_1: missing_rdr, street_2: match       (NULL, '915 PR-17, Apt 7D') vs ('50 Riverview Plaza', '915 pr-17, Apt 7-D')
            16: street_1: missing_ehr, street_2: missing_rdr ('1501 Riverplace Blvd', '') vs (NULL, '')
            17: street_1: missing_rdr, street_2: missing_rdr (NULL, '') vs ('', NULL)
            18: street_1: match,       street_2: missing_ehr ('42 Nason St', 'Apt 21C') vs ('42  Nason   St', '')
            19: street_1: missing_ehr, street_2: missing_rdr ('Test address 0000', '') vs No record for 19

            11: city - match (St. vs saint, whitespace padding, uppercase vs lowercase)
            12: city - no_match (different city)
            13: city - match (AFB vs Air Force Base)
            14: city - missing_rdr
            15: city - missing_ehr
            19: city - missing_ehr (No record for 19)

            11: state - match (uppercase vs lowercase)
            12: state - no_match (different state)
            13: state - no_match (Non-existent state (ABCD) vs Gu)
            14: state - match (PIISTATE_)
            19: state - missing_ehr (No record for 19)

            11: zip - match (identical)
            12: zip - no_match (different zip)
            13: zip - match (whitespace padding)
            14: zip - match (00921 vs 921)
            15: zip - match (08611 vs 08611-1234)
            16: zip - match (32207 vs 32207  5678)
            19: zip - missing_ehr (No record for 19)
        """

        # Create and populate concept table
        CONCEPT_TABLE_QUERY = JINJA_ENV.from_string("""
        INSERT INTO `{{project_id}}.{{drc_dataset_id}}.concept`
             (concept_id, concept_name)
            VALUES
            (8532, 'FEMALE'),
            (8507, 'MALE'),
            (8551, 'UNKNOWN'),
            (8521, 'OTHER'),
            (8570, 'AMBIGUOUS'),
            (0, 'No matching concept'),
            (4215271, 'Gender unspecified'),
            (4214687, 'Gender unknown')
        """)

        email_populate_query = POPULATE_PII_EMAILS.render(
            project_id=self.project_id,
            drc_dataset_id=self.dataset_id,
            pii_email_table_id=self.pii_email_table_id)
        job = self.bq_client.query(email_populate_query)
        job.result()

        phone_number_populate_query = POPULATE_PII_PHONE_NUMBER.render(
            project_id=self.project_id,
            drc_dataset_id=self.dataset_id,
            pii_phone_number_table_id=self.pii_phone_number_table_id)
        job = self.bq_client.query(phone_number_populate_query)
        job.result()

        name_populate_query = POPULATE_PII_NAME.render(
            project_id=self.project_id,
            drc_dataset_id=self.dataset_id,
            pii_name_table_id=self.pii_name_table_id)
        job = self.bq_client.query(name_populate_query)
        job.result()

        address_populate_query = POPULATE_PII_ADDRESS.render(
            project_id=self.project_id,
            drc_dataset_id=self.dataset_id,
            pii_address_table_id=self.pii_address_table_id)
        job = self.bq_client.query(address_populate_query)
        job.result()

        populate_person_query = POPULATE_PERSON_TABLE.render(
            project_id=self.project_id,
            drc_dataset_id=self.dataset_id,
            person_table_id=self.person_table_id)
        job = self.bq_client.query(populate_person_query)
        job.result()

        populate_location_query = POPULATE_LOCATION_TABLE.render(
            project_id=self.project_id,
            drc_dataset_id=self.dataset_id,
            location_table_id=self.location_table_id)
        job = self.bq_client.query(populate_location_query)
        job.result()

        populate_concept_query = CONCEPT_TABLE_QUERY.render(
            project_id=self.project_id, drc_dataset_id=self.dataset_id)
        job = self.bq_client.query(populate_concept_query)
        job.result()

        # Populates the validation table for the site after person is populated
        populate_validation_table(self.bq_client, self.id_match_table_id,
                                  self.hpo_id, self.dataset_id, self.dataset_id)

        # Execute email, phone_number and sex match
        identify_rdr_ehr_match(self.bq_client,
                               self.hpo_id,
                               ehr_dataset_id=self.dataset_id,
                               drc_dataset_id=self.dataset_id)

        # Subset of id match fields to test
        subset_fields = [
            'person_id', 'first_name', 'middle_name', 'last_name', 'address_1',
            'address_2', 'city', 'state', 'zip', 'email', 'phone_number', 'sex',
            'birth_date', 'algorithm'
        ]

        expected = [{
            'person_id': 1,
            'first_name': 'match',
            'middle_name': 'match',
            'last_name': 'match',
            'address_1': 'match',
            'address_2': 'missing_rdr',
            'city': 'match',
            'state': 'match',
            'zip': 'match',
            'email': 'no_match',
            'phone_number': 'no_match',
            'birth_date': 'match',
            'sex': 'match',
            'algorithm': 'yes'
        }, {
            'person_id': 2,
            'first_name': 'match',
            'middle_name': 'match',
            'last_name': 'match',
            'address_1': 'no_match',
            'address_2': 'no_match',
            'city': 'no_match',
            'state': 'no_match',
            'zip': 'no_match',
            'email': 'match',
            'phone_number': 'match',
            'birth_date': 'match',
            'sex': 'match',
            'algorithm': 'yes'
        }, {
            'person_id': 3,
            'first_name': 'match',
            'middle_name': 'match',
            'last_name': 'match',
            'address_1': 'match',
            'address_2': 'match',
            'city': 'match',
            'state': 'no_match',
            'zip': 'match',
            'email': 'missing_ehr',
            'phone_number': 'missing_ehr',
            'birth_date': 'no_match',
            'sex': 'match',
            'algorithm': 'yes'
        }, {
            'person_id': 4,
            'first_name': 'no_match',
            'middle_name': 'match',
            'last_name': 'match',
            'address_1': 'match',
            'address_2': 'match',
            'city': 'missing_rdr',
            'state': 'match',
            'zip': 'match',
            'email': 'match',
            'phone_number': 'match',
            'birth_date': 'match',
            'sex': 'no_match',
            'algorithm': 'yes'
        }, {
            'person_id': 5,
            'first_name': 'match',
            'middle_name': 'missing_rdr',
            'last_name': 'match',
            'address_1': 'missing_rdr',
            'address_2': 'match',
            'city': 'missing_ehr',
            'state': 'match',
            'zip': 'match',
            'email': 'match',
            'phone_number': 'match',
            'birth_date': 'no_match',
            'sex': 'missing_rdr',
            'algorithm': 'yes'
        }, {
            'person_id': 6,
            'first_name': 'match',
            'middle_name': 'match',
            'last_name': 'match',
            'address_1': 'missing_ehr',
            'address_2': 'missing_rdr',
            'city': 'match',
            'state': 'match',
            'zip': 'match',
            'email': 'no_match',
            'phone_number': 'match',
            'birth_date': 'missing_rdr',
            'sex': 'missing_rdr',
            'algorithm': 'yes'
        }, {
            'person_id': 7,
            'first_name': 'match',
            'middle_name': 'match',
            'last_name': 'match',
            'address_1': 'missing_rdr',
            'address_2': 'missing_rdr',
            'city': 'match',
            'state': 'match',
            'zip': 'match',
            'email': 'no_match',
            'phone_number': 'no_match',
            'birth_date': 'no_match',
            'sex': 'missing_rdr',
            'algorithm': 'yes'
        }, {
            'person_id': 8,
            'first_name': 'no_match',
            'middle_name': 'no_match',
            'last_name': 'match',
            'address_1': 'match',
            'address_2': 'missing_ehr',
            'city': 'match',
            'state': 'match',
            'zip': 'match',
            'email': 'no_match',
            'phone_number': 'no_match',
            'birth_date': 'match',
            'sex': 'no_match',
            'algorithm': 'yes'
        }]
        # no record for person_id = 9 since not in ehr person table

        content_query = ID_MATCH_CONTENT_QUERY.render(
            project_id=self.project_id,
            drc_dataset_id=self.dataset_id,
            id_match_table_id=self.id_match_table_id)

        content_job = self.bq_client.query(content_query)
        contents = list(content_job.result())
        actual = [dict(row.items()) for row in contents]
        actual = [{
            key: value for key, value in row.items() if key in subset_fields
        } for row in actual]

        self.assertCountEqual(actual, expected)

    def tearDown(self):
        test_util.delete_all_tables(self.bq_client, self.dataset_id)
        self.bq_client.delete_table(self.fq_concept_table)
