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

# Third party imports
import pandas
from google.cloud.bigquery import LoadJobConfig, WriteDisposition

# Project imports
from app_identity import get_application_id
from bq_utils import get_rdr_project_id
from resources import get_table_id, VALIDATION_STREET_CSV, VALIDATION_CITY_CSV, VALIDATION_STATE_CSV
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

    street_df = pandas.read_csv(VALIDATION_STREET_CSV, header=0)
    city_df = pandas.read_csv(VALIDATION_CITY_CSV, header=0)
    state_df = pandas.read_csv(VALIDATION_STATE_CSV, header=0)

    street_tuples_str: str = ",\n".join([
        f"('{abbreviated}','{unabbreviated}')" for abbreviated, unabbreviated in
        zip(street_df['abbreviated'], street_df['expanded'])
    ])

    city_tuples_str: str = ",\n".join([
        f"('{abbreviated}','{unabbreviated}')" for abbreviated, unabbreviated in
        zip(city_df['abbreviated'], city_df['expanded'])
    ])

    states_str: str = ",\n".join(
        [f"'{state}'" for state in state_df['abbreviated']])
    job_config = LoadJobConfig()
    job_config.write_disposition = WriteDisposition.WRITE_TRUNCATE
    job = client.load_table_from_dataframe(
        street_df,
        destination=f'{drc_dataset_id}._abbreviation_street',
        job_config=job_config)
    job.result()

    job_config = LoadJobConfig()
    job_config.write_disposition = WriteDisposition.WRITE_TRUNCATE
    job = client.load_table_from_dataframe(
        city_df,
        destination=f'{drc_dataset_id}._abbreviation_city',
        job_config=job_config)
    job.result()

    job_config = LoadJobConfig()
    job_config.write_disposition = WriteDisposition.WRITE_TRUNCATE
    job = client.load_table_from_dataframe(
        state_df,
        destination=f'{drc_dataset_id}._abbreviation_state',
        job_config=job_config)
    job.result()

    states_str: str = ",\n".join(
        [f"'{state}'" for state in state_df['abbreviated']])

    for item in consts.CREATE_COMPARISON_FUNCTION_QUERIES:
        LOGGER.info(f"Creating `{item['name']}` function if doesn't exist.")

        query = item['query'].render(
            project_id=project_id,
            drc_dataset_id=drc_dataset_id,
            match=consts.MATCH,
            no_match=consts.NO_MATCH,
            missing_rdr=consts.MISSING_RDR,
            missing_ehr=consts.MISSING_EHR,
            rdr_street=consts.NORMALIZED_STREET.render(
                abbreviation_tuples=street_tuples_str, street='rdr_street'),
            ehr_street=consts.NORMALIZED_STREET.render(
                abbreviation_tuples=street_tuples_str, street='ehr_street'),
            rdr_city=consts.NORMALIZED_CITY.render(
                abbreviation_tuples=city_tuples_str, city='rdr_city'),
            ehr_city=consts.NORMALIZED_CITY.render(
                abbreviation_tuples=city_tuples_str, city='ehr_city'),
            #rdr_street=consts.NORMALIZED_STREET.render(
            #    drc_dataset_id=drc_dataset_id, street='rdr_street'),
            #ehr_street=consts.NORMALIZED_STREET.render(
            #    drc_dataset_id=drc_dataset_id, street='ehr_street'),
            #rdr_city=consts.NORMALIZED_CITY.render(
            #    drc_dataset_id=drc_dataset_id, city='rdr_city'),
            #ehr_city=consts.NORMALIZED_CITY.render(
            #    drc_dataset_id=drc_dataset_id, city='ehr_city'),
            gender_case_when_conditions=get_gender_comparison_case_statement(),
            state_abbreviations=states_str)

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
