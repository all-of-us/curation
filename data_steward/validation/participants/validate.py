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
from collections import deque

# Third party imports
import pandas

# Project imports
from app_identity import get_application_id
from bq_utils import get_rdr_project_id
from resources import get_table_id
from utils import bq, pipeline_logging, auth
from tools.create_tier import SCOPES
from common import PS_API_VALUES, DRC_OPS, EHR_OPS
from validation.participants.store_participant_summary_results import fetch_and_store_ps_hpo_data
from validation.participants.create_update_drc_id_match_table import create_and_populate_drc_validation_table
from common import PII_ADDRESS, PII_EMAIL, PII_PHONE_NUMBER, PII_NAME, LOCATION, PERSON, JINJA_ENV
from constants.validation.participants.identity_match import IDENTITY_MATCH_TABLE
from constants.validation.participants import validate as consts

LOGGER = logging.getLogger(__name__)


def get_gender_comparison_case_statement():
    conditions = []
    for match in consts.GENDER_MATCH:
        and_conditions = []
        for dict_ in match[consts.MATCH_STATUS_PAIRS]:
            and_conditions.append(
                f"(rdr_sex in {[pair.lower() for pair in dict_[consts.RDR_SEX]]} "
                f"AND ehr_sex in {[pair.lower() for pair in dict_[consts.EHR_SEX]]})"
            )
        all_matches = ' OR '.join(and_conditions)
        all_matches = all_matches.replace('[', '(').replace(']', ')')
        conditions.append(
            f'WHEN {all_matches} THEN \'{match[consts.MATCH_STATUS]}\'')
    return ' \n'.join(conditions)


def get_state_abbreviations():
    """ Returns lowercase state abbreviations separated by comma as string.
    e.g. 'al','ak','az',...
    """
    return ','.join(f"'{state}'" for state in consts.STATE_ABBREVIATIONS)


def _get_replace_statement(base_statement, rdr_ehr, field, dict_abbreviation):
    """
    Create a nested REGEXP_REPLACE() statement for specified field and rdr/ehr.
    :param: base_statement - Function that returns the base statement to use REGEXP_REPLACE() for
    :param: rdr_ehr - string 'rdr' or 'ehr'
    :param: field - string 'city' or 'street'
    :param: dict_abbreviation - dictionary that has abbreviations
    :return: Nested REGEXP_REPLACE statement as string
    """
    statement_parts = deque([base_statement(rdr_ehr, field)])

    for key in dict_abbreviation:
        statement_parts.appendleft(
            "REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(")
        statement_parts.append(f",'^{key} ','{dict_abbreviation[key]} ')")
        statement_parts.append(f",' {key}$',' {dict_abbreviation[key]}')")
        statement_parts.append(f",' {key} ',' {dict_abbreviation[key]} ')")

    statement_parts.appendleft(f"normalized_{rdr_ehr}_{field} AS (SELECT ")

    statement_parts.append(f" AS {rdr_ehr}_{field})")

    return ''.join(statement_parts)


def get_with_clause(field):
    """
    Create WITH statement for CREATE_{field}_COMPARISON_FUNCTION.
    :param: field - string 'city'
    :return: WITH statement as string
    """
    valid_fields = {'city'}

    if field not in valid_fields:
        raise ValueError(
            f"Invalid field name: {field}. Valid field names: {valid_fields}")

    base_statement = {
        'city':
            lambda rdr_ehr, field:
            f"REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM({rdr_ehr}_{field})),'[^A-Za-z ]',''),' +',' ')"
    }

    abbreviations = {'city': consts.CITY_ABBREVIATIONS}

    statement_parts = [
        "WITH ",
        _get_replace_statement(base_statement[field], 'rdr', field,
                               abbreviations[field]), ",",
        _get_replace_statement(base_statement[field], 'ehr', field,
                               abbreviations[field])
    ]

    statement = ''.join(statement_parts)

    return statement


def identify_rdr_ehr_match(client,
                           project_id,
                           hpo_id,
                           ehr_ops_dataset_id,
                           drc_dataset_id=DRC_OPS):
    """
    
    :param client: BQ client
    :param project_id: BQ project
    :param hpo_id: Identifies the HPO site
    :param ehr_ops_dataset_id: Dataset containing HPO pii* tables
    :param drc_dataset_id: Dataset containing identity_match tables
    :return: 
    """

    id_match_table_id = f'{IDENTITY_MATCH_TABLE}_{hpo_id}'
    hpo_pii_address_table_id = get_table_id(PII_ADDRESS, hpo_id)
    hpo_pii_email_table_id = get_table_id(PII_EMAIL, hpo_id)
    hpo_pii_phone_number_table_id = get_table_id(PII_PHONE_NUMBER, hpo_id)
    hpo_pii_name_table_id = get_table_id(PII_NAME, hpo_id)
    ps_api_table_id = f'{PS_API_VALUES}_{hpo_id}'
    hpo_location_table_id = get_table_id(LOCATION, hpo_id)
    hpo_person_table_id = get_table_id(PERSON, hpo_id)

    for item in consts.CREATE_COMPARISON_FUNCTION_QUERIES:
        LOGGER.info(f"Creating `{item['name']}` function if doesn't exist.")

        query = item['query'].render(
            project_id=project_id,
            drc_dataset_id=drc_dataset_id,
            match=consts.MATCH,
            no_match=consts.NO_MATCH,
            missing_rdr=consts.MISSING_RDR,
            missing_ehr=consts.MISSING_EHR,
            gender_case_when_conditions=get_gender_comparison_case_statement(),
            state_abbreviations=get_state_abbreviations(),
            city_with_clause=get_with_clause('city'))

        job = client.query(query)
        job.result()

    match_query = consts.MATCH_FIELDS_QUERY.render(
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
        match=consts.MATCH,
        no_match=consts.NO_MATCH,
        missing_rdr=consts.MISSING_RDR,
        missing_ehr=consts.MISSING_EHR)

    LOGGER.info(f"Matching fields for {hpo_id}.")
    LOGGER.info(f"Running the following update statement: {match_query}.")

    job = client.query(match_query)
    job.result()

    drop_ehr_standardized_table_query = consts.DROP_STANDARDIZED_STREET_TABLE_QUERY.render(
        project_id=project_id,
        dataset_id=drc_dataset_id,
        standardized_street_table_id=
        f'{hpo_location_table_id}_ehr_standardized_street')

    job = client.query(drop_ehr_standardized_table_query)
    job.result()

    LOGGER.info(
        f"Running the following drop statement: {drop_ehr_standardized_table_query}."
    )

    abb_st = pandas.read_csv(
        '/Users/hm2920/Documents/GitHub/curation/data_steward/resource_files/validation/participants/abbreviation_street.csv',
        header=0)

    abb_string = ",".join([
        f"('{abbreviated}','{unabbreviated}')" for abbreviated, unabbreviated in
        zip(abb_st['abbreviated'], abb_st['unabbreviated'])
    ])

    create_ehr_standardized_table_query = consts.CREATE_STANDARDIZED_STREET_TABLE_QUERY.render(
        project_id=project_id,
        dataset_id=drc_dataset_id,
        standardized_street_table_id=
        f'{hpo_location_table_id}_ehr_standardized_street',
        source_dataset_id=ehr_ops_dataset_id,
        source_table_id=hpo_location_table_id,
        id='location_id',
        street_column='address_1',
        abbreviation_street_tuples=abb_string,
        _PARTITIONTIME_as='',
        _PARTITIONTIME='')

    job = client.query(create_ehr_standardized_table_query)
    job.result()

    LOGGER.info(
        f"Running the following create statement: {create_ehr_standardized_table_query}."
    )

    job = client.query(
        f"select * from {drc_dataset_id}.{hpo_location_table_id}_ehr_standardized_street order by location_id"
    )
    result = job.result()
    actual = [dict(row.items()) for row in result]
    actual = [{key: value for key, value in row.items()} for row in actual]
    for row in actual:
        LOGGER.info(f"RESULT - {row}.")

    drop_rdr_standardized_table_query = consts.DROP_STANDARDIZED_STREET_TABLE_QUERY.render(
        project_id=project_id,
        dataset_id=drc_dataset_id,
        standardized_street_table_id=f'{ps_api_table_id}_rdr_standardized_street'
    )

    job = client.query(drop_rdr_standardized_table_query)
    job.result()

    LOGGER.info(
        f"Running the following drop statement: {drop_rdr_standardized_table_query}."
    )

    create_rdr_standardized_table_query = consts.CREATE_STANDARDIZED_STREET_TABLE_QUERY.render(
        project_id=project_id,
        dataset_id=drc_dataset_id,
        standardized_street_table_id=
        f'{ps_api_table_id}_rdr_standardized_street',
        source_dataset_id=drc_dataset_id,
        source_table_id=ps_api_table_id,
        id='person_id',
        street_column='street_address',
        abbreviation_street_tuples=abb_string,
        _PARTITIONTIME_as='_PARTITIONTIME as partitiontime,',
        _PARTITIONTIME='partitiontime,')

    job = client.query(create_rdr_standardized_table_query)
    job.result()

    LOGGER.info(
        f"Running the following create statement: {create_rdr_standardized_table_query}."
    )

    job = client.query(
        f"select * from {drc_dataset_id}.{ps_api_table_id}_rdr_standardized_street order by person_id"
    )
    actual = [{key: value
               for key, value in row.items()}
              for row in [dict(row.items()) for row in job.result()]]
    for row in actual:
        LOGGER.info(f"RESULT - {row}.")

    match_query = consts.MATCH_FIELDS_STREET_ADDRESS_QUERY.render(
        project_id=project_id,
        id_match_table_id=id_match_table_id,
        hpo_pii_address_table_id=hpo_pii_address_table_id,
        drc_standardized_street_table_id=
        f'{ps_api_table_id}_rdr_standardized_street',
        ehr_standardized_street_table_id=
        f'{hpo_location_table_id}_ehr_standardized_street',
        ps_api_table_id=ps_api_table_id,
        drc_dataset_id=drc_dataset_id,
        ehr_ops_dataset_id=ehr_ops_dataset_id,
        _PARTITIONTIME='partitiontime',
        match=consts.MATCH,
        no_match=consts.NO_MATCH,
        missing_rdr=consts.MISSING_RDR,
        missing_ehr=consts.MISSING_EHR)

    LOGGER.info(f"Running the following update statement: {match_query}.")

    job = client.query(match_query)
    job.result()

    job = client.query(
        f"select person_id, address_1, address_2 from {drc_dataset_id}.{id_match_table_id} order by person_id"
    )
    actual = [{key: value
               for key, value in row.items()}
              for row in [dict(row.items()) for row in job.result()]]
    for row in actual:
        LOGGER.info(f"RESULT - {row}.")

    ## Address two from here

    drop_ehr_standardized_table_query = consts.DROP_STANDARDIZED_STREET_TABLE_QUERY.render(
        project_id=project_id,
        dataset_id=drc_dataset_id,
        standardized_street_table_id=
        f'{hpo_location_table_id}_ehr_standardized_street')

    job = client.query(drop_ehr_standardized_table_query)
    job.result()

    create_ehr_standardized_table_query = consts.CREATE_STANDARDIZED_STREET_TABLE_QUERY.render(
        project_id=project_id,
        dataset_id=drc_dataset_id,
        standardized_street_table_id=
        f'{hpo_location_table_id}_ehr_standardized_street',
        source_dataset_id=ehr_ops_dataset_id,
        source_table_id=hpo_location_table_id,
        id='location_id',
        street_column='address_2',
        abbreviation_street_tuples=abb_string,
        _PARTITIONTIME_as='',
        _PARTITIONTIME='')

    job = client.query(create_ehr_standardized_table_query)
    job.result()

    job = client.query(
        f"select * from {drc_dataset_id}.{hpo_location_table_id}_ehr_standardized_street order by location_id"
    )
    actual = [{key: value
               for key, value in row.items()}
              for row in [dict(row.items()) for row in job.result()]]
    for row in actual:
        LOGGER.info(f"RESULT - {row}.")

    drop_rdr_standardized_table_query = consts.DROP_STANDARDIZED_STREET_TABLE_QUERY.render(
        project_id=project_id,
        dataset_id=drc_dataset_id,
        standardized_street_table_id=f'{ps_api_table_id}_rdr_standardized_street'
    )

    job = client.query(drop_rdr_standardized_table_query)
    job.result()

    create_rdr_standardized_table_query = consts.CREATE_STANDARDIZED_STREET_TABLE_QUERY.render(
        project_id=project_id,
        dataset_id=drc_dataset_id,
        standardized_street_table_id=
        f'{ps_api_table_id}_rdr_standardized_street',
        source_dataset_id=drc_dataset_id,
        source_table_id=ps_api_table_id,
        id='person_id',
        street_column='street_address2',
        abbreviation_street_tuples=abb_string,
        _PARTITIONTIME_as='_PARTITIONTIME as partitiontime,',
        _PARTITIONTIME='partitiontime,')

    job = client.query(create_rdr_standardized_table_query)
    job.result()

    job = client.query(
        f"select * from {drc_dataset_id}.{ps_api_table_id}_rdr_standardized_street order by person_id"
    )
    actual = [{key: value
               for key, value in row.items()}
              for row in [dict(row.items()) for row in job.result()]]
    for row in actual:
        LOGGER.info(f"RESULT - {row}.")

    match_query = consts.MATCH_FIELDS_STREET_ADDRESS_QUERY_2.render(
        project_id=project_id,
        id_match_table_id=id_match_table_id,
        hpo_pii_address_table_id=hpo_pii_address_table_id,
        drc_standardized_street_table_id=
        f'{ps_api_table_id}_rdr_standardized_street',
        ehr_standardized_street_table_id=
        f'{hpo_location_table_id}_ehr_standardized_street',
        ps_api_table_id=ps_api_table_id,
        drc_dataset_id=drc_dataset_id,
        ehr_ops_dataset_id=ehr_ops_dataset_id,
        _PARTITIONTIME='partitiontime',
        match=consts.MATCH,
        no_match=consts.NO_MATCH,
        missing_rdr=consts.MISSING_RDR,
        missing_ehr=consts.MISSING_EHR)

    LOGGER.info(f"Running the following update statement: {match_query}.")

    job = client.query(match_query)
    job.result()

    job = client.query(
        f"select person_id, address_1, address_2 from {drc_dataset_id}.{id_match_table_id} order by person_id"
    )
    actual = [{key: value
               for key, value in row.items()}
              for row in [dict(row.items()) for row in job.result()]]
    for row in actual:
        LOGGER.info(f"RESULT - {row}.")


def setup_and_validate_participants(hpo_id):
    """
    Fetch PS data, set up tables and run validation
    :param hpo_id: Identifies the HPO
    :return: 
    """
    project_id = get_application_id()
    client = bq.get_client(project_id)

    # Fetch Participant summary data
    rdr_project_id = get_rdr_project_id()
    fetch_and_store_ps_hpo_data(client, client.project, rdr_project_id, hpo_id)

    # Populate identity match table based on PS data
    create_and_populate_drc_validation_table(client, hpo_id)

    # Match values
    identify_rdr_ehr_match(client, client.project, hpo_id, EHR_OPS)


def get_participant_validation_summary_query(hpo_id):
    """
    Setup tables, run validation and generate query for reporting
    :param hpo_id: 
    :return: 
    """
    return consts.SUMMARY_QUERY.render(
        project_id=get_application_id(),
        dataset_id=DRC_OPS,
        id_match_table=f'{IDENTITY_MATCH_TABLE}_{hpo_id}')


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
