"""
Script to update a site's DRC identity match table with indications of matches
between EHR submitted fields and Participant Summary API fields.

There should be a record for each participant and the record should be filled with default values of `missing_rdr` or
    `missing_ehr`. Each record should contain data for the fields: person_id, first_name, middle_name, last_name,
    phone_number, email, address_1, address_2, city, state, zip, birth_date, sex, and algorithm.

The record for each of the above fields should default to `missing_rdr` if the joined record in the
    ps_api_values_<hpo_id> table does not contain any information otherwise, it should default to `missing_ehr`

A match is indicated by a field value of 'match' and a non-match is indicated by a field values of 'non_match'

Original Issue: DC-1127
"""

# Python imports
import logging
import argparse

# Project imports
from utils import bq, pipeline_logging, auth
from tools.create_tier import SCOPES
from common import JINJA_ENV, PS_API_VALUES, DRC_OPS
from .participant_validation_queries import CREATE_COMPARISON_FUNCTION_QUERIES
from constants.validation.participants.identity_match import IDENTITY_MATCH_TABLE
from constants.validation.participants.participant_validation_queries import (
    get_gender_comparison_case_statement, get_state_abbreviations,
    get_with_clause, MATCH, NO_MATCH, MISSING_EHR, MISSING_RDR)

LOGGER = logging.getLogger(__name__)

EHR_OPS = 'ehr_ops'

MATCH_FIELDS_QUERY = JINJA_ENV.from_string("""
    UPDATE `{{project_id}}.{{drc_dataset_id}}.{{id_match_table_id}}` upd
    SET upd.first_name = `{{project_id}}.{{drc_dataset_id}}.CompareName`(ps.first_name, ehr_name.first_name),
        upd.middle_name = `{{project_id}}.{{drc_dataset_id}}.CompareName`(ps.middle_name, ehr_name.middle_name),
        upd.last_name = `{{project_id}}.{{drc_dataset_id}}.CompareName`(ps.last_name, ehr_name.last_name),
        upd.city = `{{project_id}}.{{drc_dataset_id}}.CompareCity`(ps.city, ehr_location.city),
        upd.state = `{{project_id}}.{{drc_dataset_id}}.CompareState`(ps.state, ehr_location.state),
        upd.zip = `{{project_id}}.{{drc_dataset_id}}.CompareZipCode`(ps.zip_code, ehr_location.zip),
        upd.email = `{{project_id}}.{{drc_dataset_id}}.CompareEmail`(ps.email, ehr_email.email),
        upd.phone_number = `{{project_id}}.{{drc_dataset_id}}.ComparePhoneNumber`(ps.phone_number, ehr_phone.phone_number),
        upd.birth_date = `{{project_id}}.{{drc_dataset_id}}.CompareDateOfBirth`(ps.date_of_birth, ehr_dob.date_of_birth),
        upd.sex = `{{project_id}}.{{drc_dataset_id}}.CompareSexAtBirth`(ps.sex, ehr_sex.sex),
        upd.algorithm = 'yes'
    FROM `{{project_id}}.{{drc_dataset_id}}.{{ps_api_table_id}}` ps
    LEFT JOIN `{{project_id}}.{{ehr_ops_dataset_id}}.{{hpo_pii_email_table_id}}` ehr_email
        ON ehr_email.person_id = ps.person_id
    LEFT JOIN `{{project_id}}.{{ehr_ops_dataset_id}}.{{hpo_pii_phone_number_table_id}}` ehr_phone
        ON ehr_phone.person_id = ps.person_id
    LEFT JOIN ( SELECT person_id, DATE(birth_datetime) AS date_of_birth
               FROM `{{project_id}}.{{ehr_ops_dataset_id}}.{{hpo_person_table_id}}` ) AS ehr_dob
        ON ehr_dob.person_id = ps.person_id
    LEFT JOIN ( SELECT person_id, cc.concept_name as sex
                FROM `{{project_id}}.{{ehr_ops_dataset_id}}.{{hpo_person_table_id}}`
                JOIN `{{project_id}}.{{ehr_ops_dataset_id}}.concept` cc
                    ON gender_concept_id = concept_id ) AS ehr_sex
        ON ehr_sex.person_id = ps.person_id
    LEFT JOIN `{{project_id}}.{{ehr_ops_dataset_id}}.{{hpo_pii_name_table_id}}` ehr_name
        ON ehr_name.person_id = ps.person_id
    LEFT JOIN `{{project_id}}.{{ehr_ops_dataset_id}}.{{hpo_pii_address_table_id}}` ehr_address
        ON ehr_address.person_id = ps.person_id
    LEFT JOIN `{{project_id}}.{{ehr_ops_dataset_id}}.{{hpo_location_table_id}}` ehr_location
        ON ehr_location.location_id = ehr_address.location_id
    WHERE upd.person_id = ps.person_id
        AND upd._PARTITIONTIME = ps._PARTITIONTIME
""")

DROP_STANDARDIZED_STREET_TABLE_QUERY = JINJA_ENV.from_string("""
    DROP TABLE IF EXISTS `{{project_id}}.{{dataset_id}}.{{standardized_street_table_id}}` 
""")

CREATE_STANDARDIZED_STREET_TABLE_QUERY = JINJA_ENV.from_string("""
    CREATE TABLE `{{project_id}}.{{dataset_id}}.{{standardized_street_table_id}}` 
    AS
    WITH address_abbreviations AS (
        SELECT *
        FROM UNNEST(ARRAY<STRUCT<abbreviation STRING, expansion STRING>>[
                ('aly', 'alley'),
                ('anx', 'annex'),
                ('apt', 'apartment'),
                ('ave', 'avenue'),
                ('bch', 'beach'),
                ('bldg', 'building'),
                ('blvd', 'boulevard'),
                ('bnd', 'bend'),
                ('btm', 'bottom'),
                ('cir', 'circle'),
                ('ct', 'court'),
                ('co', 'county'),
                ('ctr', 'center'),
                ('dr', 'drive'),
                ('e', 'east'),
                ('expy', 'expressway'),
                ('hts', 'heights'),
                ('hwy', 'highway'),
                ('is', 'island'),
                ('jct', 'junction'),
                ('lk', 'lake'),
                ('ln', 'lane'),
                ('mtn', 'mountain'),
                ('n', 'north'),
                ('ne', 'northeast'),
                ('num', 'number'),
                ('nw', 'northwest'),
                ('pkwy', 'parkway'),
                ('pl', 'place'),
                ('plz', 'plaza'),
                ('po', 'post office'),
                ('rd', 'road'),
                ('rdg', 'ridge'),
                ('rr', 'rural route'),
                ('rm', 'room'),
                ('s', 'south'),
                ('se', 'southeast'),
                ('sq', 'square'),
                ('st', 'street'),
                ('str', 'street'),
                ('sta', 'station'),
                ('ste', 'suite'),
                ('sw', 'southwest'),
                ('ter', 'terrace'),
                ('tpke', 'turnpike'),
                ('trl', 'trail'),
                ('vly', 'valley'),
                ('w', 'west'),
                ('way', 'way')])
    ),
    removed_commas_and_periods AS (
        SELECT location_id, REGEXP_REPLACE(address_1, '[,.]', '') as address_1
        FROM {{project_id}}.{{dataset_id}}.{{source_table_id}}
    ),
    lowercased AS (
        SELECT location_id, LOWER(address_1) as address_1
        FROM removed_commas_and_periods
    ),
    standardized_street_number AS (
        SELECT location_id, REGEXP_REPLACE(address_1,'([0-9])(?:st|nd|rd|th)', r'\1') as address_1
        FROM lowercased
    ),
    standardized_apartment_number AS (
        SELECT location_id, REGEXP_REPLACE(address_1,'([0-9])([a-z])',r'\1 \2') as address_1
        FROM standardized_street_number
    ),
    parts AS (
        SELECT location_id, part_address
        FROM drc_standardized_apartment_number,
            UNNEST(SPLIT(LOWER(address_1), ' ')) as part_address
    ),
    expanded AS (
        SELECT 
            location_id, 
            COALESCE(expansion, part_address) as expanded_part_address
        FROM parts p
        LEFT JOIN address_abbreviations aa
        ON aa.abbreviation = p.part_address
    )
    SELECT 
        location_id, 
        ARRAY_TO_STRING(ARRAY_AGG(expanded_part_address), ' ') as address
    FROM expanded
    GROUP BY location_id
""")

MATCH_FIELDS_STREET_ADDRESS_QUERY = JINJA_ENV.from_string("""
    WITH address_abbreviations AS (
        SELECT *
        FROM UNNEST(ARRAY<STRUCT<abbreviation STRING, expansion STRING>>[
                ('aly', 'alley'),
                ('anx', 'annex'),
                ('apt', 'apartment'),
                ('ave', 'avenue'),
                ('bch', 'beach'),
                ('bldg', 'building'),
                ('blvd', 'boulevard'),
                ('bnd', 'bend'),
                ('btm', 'bottom'),
                ('cir', 'circle'),
                ('ct', 'court'),
                ('co', 'county'),
                ('ctr', 'center'),
                ('dr', 'drive'),
                ('e', 'east'),
                ('expy', 'expressway'),
                ('hts', 'heights'),
                ('hwy', 'highway'),
                ('is', 'island'),
                ('jct', 'junction'),
                ('lk', 'lake'),
                ('ln', 'lane'),
                ('mtn', 'mountain'),
                ('n', 'north'),
                ('ne', 'northeast'),
                ('num', 'number'),
                ('nw', 'northwest'),
                ('pkwy', 'parkway'),
                ('pl', 'place'),
                ('plz', 'plaza'),
                ('po', 'post office'),
                ('rd', 'road'),
                ('rdg', 'ridge'),
                ('rr', 'rural route'),
                ('rm', 'room'),
                ('s', 'south'),
                ('se', 'southeast'),
                ('sq', 'square'),
                ('st', 'street'),
                ('str', 'street'),
                ('sta', 'station'),
                ('ste', 'suite'),
                ('sw', 'southwest'),
                ('ter', 'terrace'),
                ('tpke', 'turnpike'),
                ('trl', 'trail'),
                ('vly', 'valley'),
                ('w', 'west'),
                ('way', 'way')])
    ),
    drc_removed_commas_and_periods AS (
        SELECT location_id, REGEXP_REPLACE(address_1, '[,.]', '') as address_1
        FROM {{project_id}}.{{drc_dataset_id}}.{{ps_api_table_id}}
    ),
    drc_lowercased AS (
        SELECT location_id, LOWER(address_1) as address_1
        FROM drc_removed_commas_and_periods
    ),
    drc_standardized_street_number AS (
        SELECT location_id, REGEXP_REPLACE(address_1,'([0-9])(?:st|nd|rd|th)', r'\1') as address_1
        FROM drc_lowercased
    ),
    drc_standardized_apartment_number AS (
        SELECT location_id, REGEXP_REPLACE(address_1,'([0-9])([a-z])',r'\1 \2') as address_1
        FROM drc_standardized_street_number
    ),
    drc_parts AS (
        SELECT location_id, part_address
        FROM drc_standardized_apartment_number,
            UNNEST(SPLIT(LOWER(address_1), ' ')) as part_address
    ),
    drc_expanded AS (
        SELECT 
            location_id, 
            COALESCE(expansion, part_address) as expanded_part_address
        FROM drc_parts p
        LEFT JOIN address_abbreviations aa
        ON aa.abbreviation = p.part_address
    ),
    drc_standardized AS (
        SELECT 
            location_id, 
            ARRAY_TO_STRING(ARRAY_AGG(expanded_part_address), ' ') as address
        FROM drc_expanded
        GROUP BY location_id
    ),
    ehr_removed_commas_and_periods AS (
        SELECT location_id, REGEXP_REPLACE(address_1, '[,.]', '') as address_1
        FROM {{project_id}}.{{ehr_ops_dataset_id}}.{{hpo_location_table_id}}
    ),
    ehr_lowercased AS (
        SELECT location_id, LOWER(address_1) as address_1
        FROM ehr_removed_commas_and_periods
    ),
    ehr_standardized_street_number AS (
        SELECT location_id, REGEXP_REPLACE(address_1,'([0-9])(?:st|nd|rd|th)', r'\1') as address_1
        FROM ehr_lowercased
    ),
    ehr_standardized_apartment_number AS (
        SELECT location_id, REGEXP_REPLACE(address_1,'([0-9])([a-z])',r'\1 \2') as address_1
        FROM ehr_standardized_street_number
    ),
    ehr_parts AS (
        SELECT location_id, part_address
        FROM ehr_standardized_apartment_number,
            UNNEST(SPLIT(LOWER(address_1), ' ')) as part_address
    ),
    ehr_expanded AS (
        SELECT 
            location_id, 
            COALESCE(expansion, part_address) as expanded_part_address
        FROM ehr_parts p
        LEFT JOIN address_abbreviations aa
        ON aa.abbreviation = p.part_address
    ),
    ehr_standardized AS (
        SELECT 
            location_id, 
            ARRAY_TO_STRING(ARRAY_AGG(expanded_part_address), ' ') as address
        FROM ehr_expanded
        GROUP BY location_id
    )
    UPDATE `{{project_id}}.{{drc_dataset_id}}.{{id_match_table_id}}` upd
    SET upd.address_1 = `{{project_id}}.{{drc_dataset_id}}.CompareStreetAddress`(ps.street_address, ehr_location.address_1),
        upd.algorithm = 'yes'
    FROM drc_standardized ps
    LEFT JOIN `{{project_id}}.{{ehr_ops_dataset_id}}.{{hpo_pii_address_table_id}}` ehr_address
        ON ehr_address.person_id = ps.person_id
    LEFT JOIN ehr_standardized ehr_location
        ON ehr_location.location_id = ehr_address.location_id
    WHERE upd.person_id = ps.person_id
    AND upd._PARTITIONTIME = ps._PARTITIONTIME
""")


def identify_rdr_ehr_match(client,
                           project_id,
                           hpo_id,
                           ehr_ops_dataset_id,
                           drc_dataset_id=DRC_OPS):

    id_match_table_id = f'{IDENTITY_MATCH_TABLE}_{hpo_id}'
    hpo_pii_address_table_id = f'{hpo_id}_pii_address'
    hpo_pii_email_table_id = f'{hpo_id}_pii_email'
    hpo_pii_phone_number_table_id = f'{hpo_id}_pii_phone_number'
    hpo_pii_name_table_id = f'{hpo_id}_pii_name'
    ps_api_table_id = f'{PS_API_VALUES}_{hpo_id}'
    hpo_location_table_id = f'{hpo_id}_location'
    hpo_person_table_id = f'{hpo_id}_person'

    for item in CREATE_COMPARISON_FUNCTION_QUERIES:
        LOGGER.info(f"Creating `{item['name']}` function if doesn't exist.")

        query = item['query'].render(
            project_id=project_id,
            drc_dataset_id=drc_dataset_id,
            match=MATCH,
            no_match=NO_MATCH,
            missing_rdr=MISSING_RDR,
            missing_ehr=MISSING_EHR,
            gender_case_when_conditions=get_gender_comparison_case_statement(),
            state_abbreviations=get_state_abbreviations(),
            city_with_clause=get_with_clause('city'))

        LOGGER.info(f"Running the following create statement: {query}.")

        job = client.query(query)
        job.result()

    match_query = MATCH_FIELDS_QUERY.render(
        project_id=project_id,
        id_match_table_id=id_match_table_id,
        hpo_pii_address_table_id=hpo_pii_address_table_id,
        hpo_pii_name_table_id=hpo_pii_name_table_id,
        hpo_pii_email_table_id=hpo_pii_email_table_id,
        hpo_pii_phone_number_table_id=hpo_pii_phone_number_table_id,
        hpo_location_table_id=hpo_location_table_id,
        hpo_person_table_id=hpo_person_table_id,
        ps_api_table_id=ps_api_table_id,
        drc_dataset_id=drc_dataset_id,
        ehr_ops_dataset_id=ehr_ops_dataset_id,
        match=MATCH,
        no_match=NO_MATCH,
        missing_rdr=MISSING_RDR,
        missing_ehr=MISSING_EHR)

    LOGGER.info(f"Matching fields for {hpo_id}.")
    LOGGER.info(f"Running the following update statement: {match_query}.")

    job = client.query(match_query)
    job.result()

    match_query = MATCH_FIELDS_STREET_ADDRESS_QUERY.render(
        project_id=project_id,
        id_match_table_id=id_match_table_id,
        hpo_pii_address_table_id=hpo_pii_address_table_id,
        hpo_location_table_id=hpo_location_table_id,
        ps_api_table_id=ps_api_table_id,
        drc_dataset_id=drc_dataset_id,
        ehr_ops_dataset_id=ehr_ops_dataset_id,
        match=MATCH,
        no_match=NO_MATCH,
        missing_rdr=MISSING_RDR,
        missing_ehr=MISSING_EHR)

    LOGGER.info(f"Matching fields for {hpo_id}.")
    LOGGER.info(f"Running the following update statement: {match_query}.")

    job = client.query(match_query)
    job.result()


def get_arg_parser():
    parser = argparse.ArgumentParser(
        description=
        """Identify matches between participant summary api and EHR data.""")
    parser.add_argument('-p',
                        '--project_id',
                        action='store',
                        dest='project_id',
                        help='Project associated with the input datasets',
                        required=True)
    parser.add_argument('--hpo_id',
                        action='store',
                        dest='hpo_id',
                        help='awardee name of the site',
                        required=True)
    parser.add_argument('-e',
                        '--run_as_email',
                        action='store',
                        dest='run_as_email',
                        help='Service account email address to impersonate',
                        required=True)

    return parser


def main():
    parser = get_arg_parser()
    args = parser.parse_args()

    #Set up pipeline logging
    pipeline_logging.configure(level=logging.DEBUG, add_console_handler=True)

    # get credentials and create client
    impersonation_creds = auth.get_impersonation_credentials(
        args.run_as_email, SCOPES)

    client = bq.get_client(args.project_id, credentials=impersonation_creds)

    # Populates the validation table for the site
    identify_rdr_ehr_match(client, args.project_id, args.hpo_id, EHR_OPS)

    LOGGER.info('Done.')


if __name__ == '__main__':
    main()
