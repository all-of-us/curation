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
from constants.validation.participants.participant_validation_queries import get_gender_comparision_case_statement

LOGGER = logging.getLogger(__name__)

EHR_OPS = 'ehr_ops'
MATCH = 'match'
NO_MATCH = 'no_match'
MISSING_RDR = 'missing_rdr'
MISSING_EHR = 'missing_ehr'

MATCH_FIELDS_QUERY = JINJA_ENV.from_string("""
    WITH ehr_sex 
    AS 
    (
    SELECT
        person_id,
        cc.concept_name as sex
    FROM
        `{{project_id}}.{{ehr_ops_dataset_id}}.{{hpo_person_table_id}}`
    JOIN
        `{{project_id}}.{{ehr_ops_dataset_id}}.concept` AS cc
    ON
        gender_concept_id = concept_id
    )
    UPDATE `{{project_id}}.{{drc_dataset_id}}.{{id_match_table_id}}` upd
    SET upd.email = `{{project_id}}.{{drc_dataset_id}}.CompareEmail`(ps.email, ehr_email.email),
        upd.phone_number = `{{project_id}}.{{drc_dataset_id}}.ComparePhoneNumber`(ps.phone_number, ehr_phone.phone_number),
        upd.sex = `{{project_id}}.{{drc_dataset_id}}.ComparePhoneNumber` (ps.sex, ehr_sex.sex),
        upd.algorithm = 'yes'
    FROM `{{project_id}}.{{drc_dataset_id}}.{{ps_api_table_id}}` ps
    LEFT JOIN `{{project_id}}.{{ehr_ops_dataset_id}}.{{hpo_pii_email_table_id}}` ehr_email
        ON ehr_email.person_id = ps.person_id
    LEFT JOIN `{{project_id}}.{{ehr_ops_dataset_id}}.{{hpo_pii_phone_number_table_id}}` ehr_phone
        ON ehr_phone.person_id = ps.person_id
    LEFT JOIN  ehr_sex
        ON ehr_sex.person_id = ps.person_id
    WHERE upd.person_id = ps.person_id
        AND upd._PARTITIONTIME = ps._PARTITIONTIME
""")


def identify_rdr_ehr_match(client,
                           project_id,
                           hpo_id,
                           ehr_ops_dataset_id,
                           drc_dataset_id=DRC_OPS):

    id_match_table_id = f'{IDENTITY_MATCH_TABLE}_{hpo_id}'
    hpo_pii_email_table_id = f'{hpo_id}_pii_email'
    hpo_pii_phone_number_table_id = f'{hpo_id}_pii_phone_number'
    ps_api_table_id = f'{PS_API_VALUES}_{hpo_id}'
    hpo_person_table_id = f'{hpo_id}_person'

    for item in CREATE_COMPARISON_FUNCTION_QUERIES:
        LOGGER.info(f"Creating `{item['name']}` function if doesn't exist.")
        query = item['query'].render(project_id=project_id,
                                     drc_dataset_id=drc_dataset_id,
                                     match=MATCH,
                                     no_match=NO_MATCH,
                                     missing_rdr=MISSING_RDR,
                                     missing_ehr=MISSING_EHR)
        job = client.query(query)
        job.result()

    match_query = MATCH_FIELDS_QUERY.render(
        project_id=project_id,
        id_match_table_id=id_match_table_id,
        hpo_pii_email_table_id=hpo_pii_email_table_id,
        hpo_pii_phone_number_table_id=hpo_pii_phone_number_table_id,
        hpo_person_table_id=hpo_person_table_id,
        ps_api_table_id=ps_api_table_id,
        drc_dataset_id=drc_dataset_id,
        ehr_ops_dataset_id=ehr_ops_dataset_id,
        match=MATCH,
        no_match=NO_MATCH,
        missing_rdr=MISSING_RDR,
        missing_ehr=MISSING_EHR,
        gender_case_when_conditions=get_gender_comparision_case_statement())

    LOGGER.info(f"Matching fields for {hpo_id}.")
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
