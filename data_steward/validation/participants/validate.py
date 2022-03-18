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

# Project imports
from app_identity import get_application_id
from resources import get_table_id, VALIDATION_STREET_CSV, VALIDATION_CITY_CSV, VALIDATION_STATE_CSV
from utils import pipeline_logging, auth
from common import (PS_API_VALUES, DRC_OPS, EHR_OPS, CDR_SCOPES, PII_ADDRESS,
                    PII_EMAIL, PII_PHONE_NUMBER, PII_NAME, LOCATION, PERSON,
                    UNIONED)
from gcloud.bq import BigQueryClient
from validation.participants.create_update_drc_id_match_table import create_and_populate_drc_validation_table
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


def _get_lookup_tuples(csv_file) -> str:
    """ Helper function that generates a part of WITH statement for loading abbreviations. 
    :param csv_file: path of the CSV file that has abbreviations
    :return: string to be passed as a part of WITH statement
    """
    csv_df = pandas.read_csv(csv_file, header=0)

    return ",\n".join([
        f"('{abbreviated}','{expanded}')" for abbreviated, expanded in zip(
            csv_df['abbreviated'], csv_df['expanded'])
    ])


def update_comparison_udfs(client, dataset_id):
    """
    Creates/overwrites user defined functions

    :param client: BigQuery client
    :param dataset_id: Dataset location for udfs
    :return: 
    """
    state_df = pandas.read_csv(VALIDATION_STATE_CSV, header=0)
    states_str: str = ",\n".join(
        [f"'{state}'" for state in state_df['abbreviated']])

    for item in consts.CREATE_COMPARISON_FUNCTION_QUERIES:
        LOGGER.info(f"Creating `{item['name']}` function if doesn't exist.")

        query = item['query'].render(
            project_id=client.project,
            drc_dataset_id=dataset_id,
            match=consts.MATCH,
            no_match=consts.NO_MATCH,
            missing_rdr=consts.MISSING_RDR,
            missing_ehr=consts.MISSING_EHR,
            gender_case_when_conditions=get_gender_comparison_case_statement(),
            normalized_street_rdr=consts.NORMALIZED_STREET.render(
                lookup_tuples=_get_lookup_tuples(VALIDATION_STREET_CSV),
                street='rdr_street'),
            normalized_street_ehr=consts.NORMALIZED_STREET.render(
                lookup_tuples=_get_lookup_tuples(VALIDATION_STREET_CSV),
                street='ehr_street'),
            normalized_city_rdr=consts.NORMALIZED_CITY.render(
                lookup_tuples=_get_lookup_tuples(VALIDATION_CITY_CSV),
                city='rdr_city'),
            normalized_city_ehr=consts.NORMALIZED_CITY.render(
                lookup_tuples=_get_lookup_tuples(VALIDATION_CITY_CSV),
                city='ehr_city'),
            state_abbreviations=states_str)

        job = client.query(query)
        job.result()


def identify_rdr_ehr_match(client,
                           hpo_id,
                           ehr_dataset_id=EHR_OPS,
                           drc_dataset_id=DRC_OPS,
                           update_udf=True):
    """
    
    :param client: a BigQueryClient
    :param hpo_id: Identifies the HPO site
    :param ehr_dataset_id: Dataset containing HPO pii* tables
    :param drc_dataset_id: Dataset containing identity_match tables
    :param update_udf: Boolean to update udfs, true by default
    :return: 
    """

    id_match_table_id = f'{IDENTITY_MATCH_TABLE}_{hpo_id}'
    hpo_pii_address_table_id = get_table_id(PII_ADDRESS, hpo_id)
    hpo_pii_email_table_id = get_table_id(PII_EMAIL, hpo_id)
    hpo_pii_phone_number_table_id = get_table_id(PII_PHONE_NUMBER, hpo_id)
    hpo_pii_name_table_id = get_table_id(PII_NAME, hpo_id)
    ps_api_table_id = f'{PS_API_VALUES}_{UNIONED}'
    hpo_location_table_id = get_table_id(LOCATION, hpo_id)
    hpo_person_table_id = get_table_id(PERSON, hpo_id)

    if update_udf:
        update_comparison_udfs(client, drc_dataset_id)

    fields_match_query = consts.MATCH_FIELDS_QUERY.render(
        project_id=client.project,
        id_match_table_id=id_match_table_id,
        hpo_pii_address_table_id=hpo_pii_address_table_id,
        hpo_pii_name_table_id=hpo_pii_name_table_id,
        hpo_pii_email_table_id=hpo_pii_email_table_id,
        hpo_pii_phone_number_table_id=hpo_pii_phone_number_table_id,
        hpo_location_table_id=hpo_location_table_id,
        hpo_person_table_id=hpo_person_table_id,
        ps_api_table_id=ps_api_table_id,
        drc_dataset_id=drc_dataset_id,
        ehr_dataset_id=ehr_dataset_id)

    street_combined_match_query = consts.MATCH_STREET_COMBINED_QUERY.render(
        project_id=client.project,
        id_match_table_id=id_match_table_id,
        hpo_pii_address_table_id=hpo_pii_address_table_id,
        hpo_person_table_id=hpo_person_table_id,
        hpo_location_table_id=hpo_location_table_id,
        ps_api_table_id=ps_api_table_id,
        drc_dataset_id=drc_dataset_id,
        ehr_dataset_id=ehr_dataset_id,
        match=consts.MATCH)

    LOGGER.info(f"Matching fields for {hpo_id}.")

    for query in [fields_match_query, street_combined_match_query]:
        LOGGER.info(f"Running the following update statement: {query}.")
        job = client.query(query)
        job.result()


def setup_and_validate_participants(hpo_id, update_udf=True):
    """
    Fetch PS data, set up tables and run validation
    :param hpo_id: Identifies the HPO
    :param update_udf: Boolean to update comparison udfs, true by default
    :return: 
    """
    project_id = get_application_id()
    bq_client = BigQueryClient(project_id)

    # Populate identity match table based on PS data
    create_and_populate_drc_validation_table(bq_client, hpo_id)

    # Match values
    identify_rdr_ehr_match(bq_client, hpo_id, update_udf=update_udf)


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

    # Set up pipeline logging
    pipeline_logging.configure(level=logging.DEBUG, add_console_handler=True)

    # get credentials and create client
    impersonation_creds = auth.get_impersonation_credentials(
        args.run_as_email, CDR_SCOPES)

    bq_client = BigQueryClient(args.project_id, credentials=impersonation_creds)

    # Populates the validation table for the site
    identify_rdr_ehr_match(bq_client, args.hpo_id)

    LOGGER.info('Done.')


if __name__ == '__main__':
    main()
