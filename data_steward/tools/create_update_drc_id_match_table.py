"""
Utility to create or update a site's DRC identity match table.

There should be a record for each participant and the record should be filled with default values of `missing_rdr` or
    `missing_ehr`. Each record should contain data for the fields: person_id, first_name, middle_name, last_name,
    phone_number, email, address_1, address_2, city, state, zip, birth_date, sex, and algorithm.

The record for each of the above fields should default to `missing_rdr` if the joined record in the
    ps_api_values_<hpo_id> table does not contain any information otherwise, it should default to `missing_rdr`

Original Issue: DC-1216
"""

# Python imports
import logging
import argparse

# Third party imports
from google.cloud import bigquery

# Project imports
import resources
from utils import bq
from utils import auth
from bq_utils import table_exists
from tools.create_tier import SCOPES
from common import JINJA_ENV, PS_API_VALUES, DRC_OPS
from constants.validation.participants.identity_match import IDENTITY_MATCH_TABLE

LOGGER = logging.getLogger(__name__)

CREATE_TABLE = JINJA_ENV.from_string("""
CREATE TABLE `{{project_id}}.{{drc_dataset_id}}.{{id_match_table_id}}` ({{fields}})
PARTITION BY DATE_TRUNC(_PARTITIONTIME, HOUR)
""")

INSERT_QUERY = JINJA_ENV.from_string("""
MERGE `{{project_id}}.{{drc_dataset_id}}.{{id_match_table_id}}` b 
USING `{{project_id}}.{{drc_dataset_id}}.{{ps_values_table_id}}` a
ON a.person_id = b.person_id
WHEN NOT MATCHED THEN 
INSERT (_PARTITIONTIME, {{fields}}) 
VALUES (TIMESTAMP_TRUNC(CURRENT_TIMESTAMP, HOUR), {{fields}})
""")

MISSING_EHR_QUERY = JINJA_ENV.from_string("""
UPDATE `{{project_id}}.{{drc_dataset_id}}.{{id_match_table_id}}`
SET {{col}} = 'missing_ehr'
WHERE {{col}} IS NULL
""")

MISSING_RDR_QUERY = JINJA_ENV.from_string("""
UPDATE `{{project_id}}.{{drc_dataset_id}}.{{id_match_table_id}}`
SET {{set_expression}}
WHERE COALESCE({{fields}}) IS NULL
""")

SET_EXPRESSION = JINJA_ENV.from_string("""
{{col}} = 'missing_rdr'""")


def create_drc_validation_table(client, project_id, table_id):
    """
    Creates the partitioned DRC validation table. Table will be partitioned by HOUR.

    :param client: bq client
    :param project_id: the project containing the dataset
    :param table_id: ID of the table
    """

    fields = resources.fields_for(IDENTITY_MATCH_TABLE)

    create_table = CREATE_TABLE.render(project_id=project_id,
                                       drc_dataset_id=DRC_OPS,
                                       id_match_table_id=table_id,
                                       fields=bq.get_bq_fields_sql(fields))
    client.query(create_table)

    LOGGER.info(f'Created {table_id}.')

    return table_id


def copy_ps_values_data_to_id_match_table(client, project_id, table_id, hpo_id):
    """
    Copies data from the ps_values table to the DRC identity match table.

    :param client: bq client
    :param project_id: the project containing the dataset
    :param table_id: ID for the table
    :param hpo_id: ID for the HPO site
    """

    schema_list = bq.get_table_schema(IDENTITY_MATCH_TABLE)
    id_match_table_id = table_id
    ps_values_table_id = f'{PS_API_VALUES}_{hpo_id}'

    fields_name_str = ',\n'.join([item.name for item in schema_list])

    insert_query = INSERT_QUERY.render(project_id=project_id,
                                       drc_dataset_id=DRC_OPS,
                                       id_match_table_id=id_match_table_id,
                                       fields=fields_name_str,
                                       ps_values_table_id=ps_values_table_id)

    client.query(insert_query)

    LOGGER.info(
        f'Inserted data from `{ps_values_table_id}` to `{id_match_table_id}`')


def get_set_expression():
    """
    This method generates the SET_EXPRESSION query
    """
    set_expression = []
    schema_list = bq.get_table_schema(IDENTITY_MATCH_TABLE)
    ', '.join([item.name for item in schema_list])

    for item in schema_list:
        set_expression.append(SET_EXPRESSION.render(col=item.name))

    # Work around to remove person_id from the fields list since person_id is required
    # and will never need to be updated to missing_rdr
    set_expression.remove("\nperson_id = 'missing_rdr'")

    return ', '.join(set_expression)


def update_site_drc_table(client, project_id, table_id):
    """
    Updates the site's DRC identity match table. If the record is empty, each field should default
        to `missing_rdr`, else `missing_ehr` for each field that does not have any data.

    :param client: bq client
    :param project_id: the project containing the dataset
    :param table_id: ID of the table
    """
    schema_list = bq.get_table_schema(IDENTITY_MATCH_TABLE)
    fields_name_str = ',\n'.join([item.name for item in schema_list])
    fields = fields_name_str.replace('person_id,', '')
    id_match_table_id = table_id

    # Updates missing rows to contain 'missing_rdr'
    missing_rdr_query = MISSING_RDR_QUERY.render(
        project_id=project_id,
        drc_dataset_id=DRC_OPS,
        id_match_table_id=id_match_table_id,
        set_expression=get_set_expression(),
        fields=fields)

    job = client.query(missing_rdr_query)
    job.result()
    LOGGER.info(
        f'Updated missing row values in `{id_match_table_id}` with \'missing_rdr\''
    )

    # Updates remaining fields to contain 'missing_ehr' in site's drc id match table
    for item in schema_list:
        if item.name is not 'person_id':
            missing_ehr_query = MISSING_EHR_QUERY.render(
                project_id=project_id,
                drc_dataset_id=DRC_OPS,
                id_match_table_id=id_match_table_id,
                col=item.name)

            job = client.query(missing_ehr_query)
            job.result()
        LOGGER.info(
            f'Updated missing column values in `{id_match_table_id}` with \'missing_ehr\''
        )

    LOGGER.info(f'Completed column update of `{id_match_table_id}` table.')


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
    parser.add_argument('-s',
                        '--console_log',
                        dest='console_log',
                        action='store_true',
                        required=False,
                        help='Log to the console as well as to a file.')
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

    # get credentials and create client
    impersonation_creds = auth.get_impersonation_credentials(
        args.run_as_email, SCOPES)

    client = bq.get_client(args.project_id, credentials=impersonation_creds)

    table_id = f'drc_identity_match_{args.hpo_id}'

    # Creates hpo_site identity match table if it does not exist
    if not table_exists(table_id, DRC_OPS):
        create_drc_validation_table(client, args.project_id, table_id)

    # Copy values from ps_values table and updates site DRC id match table
    copy_ps_values_data_to_id_match_table(client, args.project_id, table_id,
                                          args.hpo_id)
    update_site_drc_table(client, args.project_id, table_id)


if __name__ == '__main__':
    main()
