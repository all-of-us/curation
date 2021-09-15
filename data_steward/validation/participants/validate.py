# Python imports
import logging
import argparse

# Project imports
from utils import bq, pipeline_logging, auth
from tools.create_tier import SCOPES
from common import JINJA_ENV, PS_API_VALUES, DRC_OPS
from .participant_validation_queries import CREATE_COMPARISON_FUNCTION_QUERIES

LOGGER = logging.getLogger(__name__)

EHR_OPS = 'ehr_ops'
MATCH = 'match'
NO_MATCH = 'no_match'
MISSING_RDR = 'missing_rdr'
MISSING_EHR = 'missing_ehr'

MATCH_FIELDS_QUERY = JINJA_ENV.from_string("""
    UPDATE `{{project_id}}.{{drc_dataset_id}}.{{id_match_table_id}}` upd
    SET upd.email = `{{project_id}}.{{drc_dataset_id}}.CompareEmail`(ps.email, ehr.email),
        upd.algorithm = 'yes'
    FROM `{{project_id}}.{{drc_dataset_id}}.{{ps_api_table_id}}` ps
    LEFT JOIN `{{project_id}}.{{ehr_ops_dataset_id}}.{{hpo_pii_email_table_id}}` ehr
        ON ehr.person_id = ps.person_id
    WHERE upd.person_id = ps.person_id
        -- AND PARTITION = PARTITION --
""")


def identify_rdr_ehr_match(client, project_id, hpo_id, ehr_ops_dataset_id):

    id_match_table_id = f'drc_identity_match_{hpo_id}'
    hpo_pii_email_table_id = f'{hpo_id}_pii_email'
    ps_api_table_id = f'{PS_API_VALUES}_{hpo_id}'

    for item in CREATE_COMPARISON_FUNCTION_QUERIES:
        LOGGER.info(f"Creating `{item['name']}` function if doesn't exist.")
        query = item['query'].render(project_id=project_id,
                                     drc_dataset_id=DRC_OPS,
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
        ps_api_table_id=ps_api_table_id,
        drc_dataset_id=DRC_OPS,
        ehr_ops_dataset_id=ehr_ops_dataset_id,
        match=MATCH,
        no_match=NO_MATCH,
        missing_rdr=MISSING_RDR,
        missing_ehr=MISSING_EHR)

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
