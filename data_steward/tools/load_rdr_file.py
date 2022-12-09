from argparse import ArgumentParser, Namespace
from datetime import datetime
import json
import logging

from google.cloud import bigquery

from common import CDR_SCOPES
from gcloud.bq import BigQueryClient
from utils import auth
from utils import pipeline_logging
from utils.parameter_validators import (validate_bq_project_name,
                                        validate_qualified_bq_tablename,
                                        validate_file_exists,
                                        validate_email_address,
                                        validate_bucket_filepath)

LOGGER = logging.getLogger(__name__)


def load_rdr_table(client: BigQueryClient, src_uri: str, table_id: str,
                   schema_filepath: str) -> None:
    """
    Load a table from the RDR bucket as a service account with permissions.

    The person running the script must have
    Service Account Token creator permissions.  This function will read the
    JSON schema file, build SchemaField objects, create a LoadJobConfig, and
    load the file into the fully qualified table name if the table does not exist.
    """
    # load the schema from the filepath
    with open(schema_filepath, 'r') as fp:
        fields = json.load(fp)

    # turn into SchemaField list
    schema_list = [
        bigquery.SchemaField.from_api_repr(field) for field in fields
    ]

    job_config = bigquery.LoadJobConfig(
        schema=schema_list,
        skip_leading_rows=1,
        # The source format defaults to CSV, so the line below is optional.
        source_format=bigquery.SourceFormat.CSV,
        field_delimiter=',',
        allow_quoted_newlines=True,
        quote_character='"',
        write_disposition=bigquery.job.WriteDisposition.WRITE_EMPTY)

    # create job id prefix from filename
    filename = src_uri.split('/')[-1]
    job_id_prefix = f'{filename[:15]}_{datetime.now().strftime("%Y%m%d_%H%M%S")}'

    load_job = client.load_table_from_uri(
        src_uri, table_id, job_config=job_config,
        job_id_prefix=job_id_prefix)  # Make an API request.

    LOGGER.info(
        f"Load job created and awaiting result.\njob_id prefix is: '{job_id_prefix}'"
    )
    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)  # Make an API request.
    LOGGER.info(f"Loaded {destination_table.num_rows} rows into '{table_id}'.")


def parse_args(raw_args=None) -> Namespace:
    """
    Accept incoming arguments for parsing that may be shared by multiple modules.

    return:  a Namespace object applicable to loading ad hoc files from the
    secure bucket.
    """
    parser = ArgumentParser(
        description='Arguments pertaining to a file in the RDR bucket')

    parser.add_argument('--bucket_filepath',
                        action='store',
                        dest='bucket_filepath',
                        type=validate_bucket_filepath,
                        help=('Full filepath including the '
                              '"gs://" portion of the name'),
                        required=True)
    parser.add_argument('--run_as',
                        action='store',
                        dest='run_as_email',
                        type=validate_email_address,
                        help='Service account email address to impersonate',
                        required=True)
    parser.add_argument('--curation_project',
                        action='store',
                        dest='curation_project_id',
                        type=validate_bq_project_name,
                        help='Curation project to load the RDR data into.',
                        required=True)
    parser.add_argument('-l',
                        '--console_log',
                        dest='console_log',
                        action='store_true',
                        required=False,
                        help='Log to the console as well as to a file.')
    parser.add_argument('--destination_table',
                        action='store',
                        dest='fq_dest_table',
                        required=True,
                        type=validate_qualified_bq_tablename,
                        help=('Fully qualified GCP table name.  '
                              'It cannot exist prior to running '
                              'the script.  Should follow GCP '
                              'naming conventions.  Formatted as: '
                              'project_id.dataset_id.table_name'))
    parser.add_argument('--schema_filepath',
                        action='store',
                        dest='schema_filepath',
                        required=True,
                        type=validate_file_exists,
                        help=('Path to a schema file that contians a list of '
                              'dictionary field definitions.  The dictionary '
                              'should minimally define the field name, '
                              'mode, type, and description.  Yes, description '
                              'is required.  If you dont know it, make '
                              'a reasonable description yourself.'))

    return parser.parse_args(raw_args)


def main(raw_args=None) -> None:
    """
    Load an ad hoc file from an RDR owned and secured bucket.

    Assumes you are passing arguments either via command line or as
    a list from another module.
    """
    args = parse_args(raw_args)

    pipeline_logging.configure(level=logging.INFO,
                               add_console_handler=args.console_log)

    # get credentials and create client
    impersonation_creds = auth.get_impersonation_credentials(
        args.run_as_email, CDR_SCOPES)

    bq_client = BigQueryClient(args.curation_project_id,
                               credentials=impersonation_creds)

    load_rdr_table(bq_client,
                   args.bucket_filepath,
                   args.fq_dest_table,
                   schema_filepath=args.schema_filepath)

    LOGGER.info("Returned from loading ad hoc file from RDR bucket.  Done.")


if __name__ == '__main__':
    main()