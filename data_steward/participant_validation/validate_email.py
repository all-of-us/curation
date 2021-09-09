# Python imports
import logging
import argparse

# Project imports
from utils import bq, pipeline_logging, auth
from tools.create_tier import SCOPES
from common import JINJA_ENV, PS_API_VALUES, DRC_OPS

LOGGER = logging.getLogger(__name__)

EHR_OPS = 'ehr_ops'

CREATE_EMAIL_COMPARISON_FUNCTION = JINJA_ENV.from_string("""
    CREATE FUNCTION IF NOT EXISTS `{{project_id}}.{{drc_dataset_id}}.CompareEmail`(rdr_email string, ehr_email string)
    RETURNS string
    AS ((
        WITH normalized_rdr_email AS (
            SELECT LOWER(TRIM(rdr_email)) AS rdr_email
        )
        , normalized_ehr_email AS (
            SELECT LOWER(TRIM(ehr_email)) AS ehr_email
        )
        SELECT
            CASE 
                WHEN normalized_rdr_email.rdr_email = normalized_ehr_email.ehr_email
                    AND REGEXP_CONTAINS(normalized_rdr_email.rdr_email, '@') THEN 'match'
                WHEN normalized_rdr_email.rdr_email IS NOT NULL AND normalized_ehr_email.ehr_email IS NOT NULL THEN 'no_match'
                WHEN normalized_rdr_email.rdr_email IS NULL THEN 'missing_rdr'
                ELSE 'missing_ehr'
            END AS email
        FROM normalized_rdr_email, normalized_ehr_email 

    ));
""")

MATCH_EMAIL_QUERY = JINJA_ENV.from_string("""
    UPDATE `{{project_id}}.{{drc_dataset_id}}.{{id_match_table_id}}` upd
    SET upd.email = `{{project_id}}.{{drc_dataset_id}}.CompareEmail`(ps.email, ehr.email)
    FROM `{{project_id}}.{{drc_dataset_id}}.{{id_match_table_id}}` id_match
    LEFT JOIN `{{project_id}}.{{drc_dataset_id}}.{{ps_api_table_id}}` ps
        ON ps.person_id = id_match.person_id
    LEFT JOIN `{{project_id}}.{{ehr_ops_dataset_id}}.{{hpo_pii_email_table_id}}` ehr
        ON ehr.person_id = id_match.person_id
    WHERE upd.person_id = id_match.person_id
""")


def identify_rdr_ehr_email_match(client, project_id, hpo_id):

    id_match_table_id = f'drc_identity_match_{hpo_id}'
    hpo_pii_email_table_id = f'{hpo_id}_pii_email'
    ps_api_table_id = f'{PS_API_VALUES}_{hpo_id}'

    create_email_query = CREATE_EMAIL_COMPARISON_FUNCTION.render(
        project_id=project_id, drc_dataset_id=DRC_OPS)

    match_email_query = MATCH_EMAIL_QUERY.render(
        project_id=project_id,
        id_match_table_id=id_match_table_id,
        hpo_pii_email_table_id=hpo_pii_email_table_id,
        ps_api_table_id=ps_api_table_id,
        drc_dataset_id=DRC_OPS,
        ehr_ops_dataset_id=EHR_OPS)

    LOGGER.info("Creating `CompareEmail` function if doesn't exist.")
    job = client.query(create_email_query)
    job.result()

    LOGGER.info(f"Matching email field for {hpo_id}.")
    job = client.query(match_email_query)
    job.result()


def get_arg_parser():
    parser = argparse.ArgumentParser(
        description=""" Create and update DRC match table for hpo sites.""")
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
    identify_rdr_ehr_email_match(client, args.project_id, args.hpo_id)

    LOGGER.info('Done.')


if __name__ == '__main__':
    main()
