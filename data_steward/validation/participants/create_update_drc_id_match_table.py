"""
Utility to create or update a site's DRC identity match table.

There should be a record for each participant and the record should be filled with default values of `missing_rdr` or
    `missing_ehr`. Each record should contain data for the fields: person_id, first_name, middle_name, last_name,
    phone_number, email, address_1, address_2, city, state, zip, birth_date, sex, and algorithm.

The record for each of the above fields should default to `missing_rdr` if the joined record in the
    ps_api_values_<hpo_id> table does not contain any information otherwise, it should default to `missing_ehr`

Original Issue: DC-1216
"""

# Python imports
import logging
import argparse

# Project imports
import resources
from utils import bq, auth
from gcloud.bq import BigQueryClient
import bq_utils
from common import JINJA_ENV, PS_API_VALUES, DRC_OPS, CDR_SCOPES
from constants.validation.participants.identity_match import IDENTITY_MATCH_TABLE

LOGGER = logging.getLogger(__name__)

IDENTITY_MATCH_PS_API_FIELD_MAP = {
    'person_id': 'person_id',
    'first_name': 'first_name',
    'middle_name': 'middle_name',
    'last_name': 'last_name',
    'phone_number': 'phone_number',
    'email': 'email',
    'address_1': 'street_address',
    'address_2': 'street_address2',
    'city': 'city',
    'state': 'state',
    'zip': 'zip_code',
    'birth_date': 'date_of_birth',
    'sex': 'sex'
}

CREATE_TABLE = JINJA_ENV.from_string("""
CREATE TABLE `{{project_id}}.{{drc_dataset_id}}.{{id_match_table_id}}` ({{fields}})
PARTITION BY TIMESTAMP_TRUNC(_PARTITIONTIME, HOUR)
""")

POPULATE_VALIDATION_TABLE = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{drc_dataset_id}}.{{id_match_table_id}}` (_PARTITIONTIME, {{fields}}) 
SELECT
    _PARTITIONTIME, 
    person_id, {{case_statements}}, '' algorithm
FROM `{{project_id}}.{{drc_dataset_id}}.{{ps_values_table_id}}`
WHERE ABS(TIMESTAMP_DIFF(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP, HOUR), _PARTITIONTIME, HOUR)) < 2
""")

CASE_EXPRESSION = JINJA_ENV.from_string("""
CASE WHEN {{ps_api_field}} IS NULL THEN 'missing_rdr' ELSE 'missing_ehr' END AS {{identity_match_field}}
""")


def create_drc_validation_table(client, table_id, drc_dataset_id=DRC_OPS):
    """
    Creates the partitioned DRC validation table, partitioned by HOUR.

    :param client: a BigQueryClient
    :param table_id: ID of the table
    """
    fields = resources.fields_for(IDENTITY_MATCH_TABLE)

    create_table = CREATE_TABLE.render(project_id=client.project,
                                       drc_dataset_id=drc_dataset_id,
                                       id_match_table_id=table_id,
                                       fields=bq.get_bq_fields_sql(fields))
    job = client.query(create_table)
    job.result()

    LOGGER.info(f'Created HOUR partitioned table {drc_dataset_id}.{table_id}')

    return table_id


def get_case_statements():
    """
    This method generates the CASE_STATEMENT query
    """
    case_statements = []
    field_list = []

    schema_list = bq.get_table_schema(IDENTITY_MATCH_TABLE)
    for item in schema_list:
        field_list.append(item.name)

    # this removes the person_id as it is primary key and will not be updated in case statement
    field_list.remove('person_id')
    # this removes algorithm as it is not updated in case statement
    field_list.remove('algorithm')

    for item in field_list:
        ps_api_item = IDENTITY_MATCH_PS_API_FIELD_MAP[item]
        case_statements.append(
            CASE_EXPRESSION.render(identity_match_field=item,
                                   ps_api_field=ps_api_item))

    return ', '.join(case_statements)


def populate_validation_table(client, table_id, hpo_id, drc_dataset_id=DRC_OPS):
    """
    Populates validation table with 'missing_rdr' or 'missing_ehr' data. Populated with 'missing_rdr' if data IS NOT
        found in the ps_values table. Populated with 'missing_ehr' as default.

    :param client: A BigQueryClient
    :param table_id: ID for the table
    :param hpo_id: ID for the HPO site
    """

    schema_list = bq.get_table_schema(IDENTITY_MATCH_TABLE)
    id_match_table_id = table_id
    ps_values_table_id = f'{PS_API_VALUES}_{hpo_id}'

    fields_name_str = ', '.join([item.name for item in schema_list])

    populate_query = POPULATE_VALIDATION_TABLE.render(
        project_id=client.project,
        drc_dataset_id=drc_dataset_id,
        id_match_table_id=id_match_table_id,
        fields=fields_name_str,
        case_statements=get_case_statements(),
        ps_values_table_id=ps_values_table_id)

    job = client.query(populate_query)
    job.result()

    LOGGER.info(
        f'Populated default values in {drc_dataset_id}.{id_match_table_id}')


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


def create_and_populate_drc_validation_table(client, hpo_id):
    """
    Create and populate drc validation table
    :param client: a BigQueryClient
    :param hpo_id: Identifies the HPO
    :return: 
    """

    table_id = f'{IDENTITY_MATCH_TABLE}_{hpo_id}'

    # Creates hpo_site identity match table if it does not exist
    if not bq_utils.table_exists(table_id, DRC_OPS):
        create_drc_validation_table(client, table_id)

    # Populates the validation table for the site
    populate_validation_table(client, table_id, hpo_id)


def main():
    parser = get_arg_parser()
    args = parser.parse_args()

    # get credentials and create client
    impersonation_creds = auth.get_impersonation_credentials(
        args.run_as_email, CDR_SCOPES)

    bq_client = BigQueryClient(args.project_id, credentials=impersonation_creds)

    create_and_populate_drc_validation_table(bq_client, args.hpo_id)


if __name__ == '__main__':
    main()
