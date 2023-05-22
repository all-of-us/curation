"""
Deid runner.

A central script to execute deid for each table needing de-identification.
"""

# Python imports
from datetime import datetime
import logging
import os
from argparse import ArgumentParser

# Third party imports
import google

# Project imports
import app_identity
import bq_utils
import deid.aou as aou
from deid.parser import odataset_name_verification
from resources import fields_for, fields_path, DEID_PATH
from gcloud.bq import BigQueryClient
from google.cloud.bigquery.job import CopyJobConfig, WriteDisposition
from common import JINJA_ENV, PIPELINE_TABLES, EXT_SUFFIX

LOGGER = logging.getLogger(__name__)
DEID_TABLES = [
    'person', 'observation', 'visit_occurrence', 'visit_detail',
    'condition_occurrence', 'drug_exposure', 'procedure_occurrence',
    'device_exposure', 'death', 'measurement', 'location', 'care_site',
    'specimen', 'observation_period', 'provider', 'survey_conduct', 'aou_death'
]
# these tables will be suppressed.  This means an empty table with the same schema will
# exist.  It overrides the DEID_TABLES list
SUPPRESSED_TABLES = ['note', 'note_nlp', 'location', 'care_site', 'provider']
VOCABULARY_TABLES = [
    'concept', 'vocabulary', 'domain', 'concept_class', 'concept_relationship',
    'relationship', 'concept_synonym', 'concept_ancestor',
    'source_to_concept_map', 'drug_strength'
]
DEID_MAP_TABLE = 'primary_pid_rid_mapping'
PIPELINE_TABLES_DATASET = 'pipeline_tables'

LOGS_PATH = 'LOGS'

COPY_PID_RID_QUERY = JINJA_ENV.from_string("""
CREATE or REPLACE TABLE {{map_table}} as
SELECT
  *
FROM
  `{{project}}.{{lookup_dataset}}.{{pid_rid_table}}`
WHERE
  person_id IN (
  SELECT
    person_id
  FROM (
    SELECT
      DISTINCT person_id,
      `{{project}}.{{PIPELINE_TABLES}}.calculate_age`(CURRENT_DATE, EXTRACT(DATE FROM birth_datetime)) AS age
    FROM `{{project}}.{{input_dataset}}.person`
    ORDER BY 2)
  WHERE
    age < {{max_age}})
""")


def add_console_logging(add_handler):
    """
    This config should be done in a separate module, but that can wait
    until later.  Useful for debugging.
    """
    try:
        os.makedirs(LOGS_PATH)
    except OSError:
        # directory already exists.  move on.
        pass

    name = datetime.now().strftime(
        os.path.join(LOGS_PATH, 'run_deid-%Y-%m-%d.log'))
    logging.basicConfig(
        filename=name,
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    if add_handler:
        handler = logging.StreamHandler()
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(levelname)s - %(name)s - %(message)s')
        handler.setFormatter(formatter)
        logging.getLogger('').addHandler(handler)


def get_known_tables(field_path):
    """
    Get all table names known to curation.

    :param field_path:  imported string path to the table schemas

    :return:  a list of table names
    """
    known_tables = []
    for _, _, files in os.walk(field_path):
        known_tables.extend(files)

    known_tables = [item.split('.json')[0] for item in known_tables]
    return known_tables


def get_output_tables(client, input_dataset, known_tables, skip_tables,
                      only_tables):
    """
    Get list of output tables deid should produce.

    Specifically excludes table names that start with underscores, pii, or
    are explicitly suppressed.

    :param client: a BigQueryClient
    :param input_dataset:  dataset to read when gathering all possible table names.
    :param known_tables:  list of tables known to curation.  If a table exists in
        the input dataset but is not known to curation, it is skippped.
    :param skip_tables:  command line csv string of tables to skip for deid.
        Useful to perform deid on a subset of tables.

    :return: a list of table names to execute deid over.
    """
    tables = client.list_tables(input_dataset)
    table_ids = [table.table_id for table in tables]
    skip_tables = [table.strip() for table in skip_tables.split(',')]
    only_tables = [table.strip() for table in only_tables.split(',')]

    allowed_tables = []
    for table in table_ids:
        if table.startswith('_') or table.startswith(
                'pii') or table in SUPPRESSED_TABLES:
            continue
        # doing this to eliminate the 'deid_map' table and any other non-OMOP table
        if table not in known_tables or table in skip_tables:
            continue

        if (only_tables == [''] or
                table in only_tables) and table in DEID_TABLES:
            allowed_tables.append(table)

    return allowed_tables


def copy_suppressed_table_schemas(known_tables, dest_dataset):
    """
    Copy only table schemas for suppressed tables.

    :param known_tables:  list of tables the software 'knows' about for deid purposes.
    :param dest_dataset:  name of the dataset to copy tables to.
    """
    for table in SUPPRESSED_TABLES:
        if table in known_tables:
            field_list = fields_for(table)
            # create a table schema only.
            bq_utils.create_table(table,
                                  field_list,
                                  drop_existing=True,
                                  dataset_id=dest_dataset)


def copy_vocabulary_tables(input_dataset, dest_dataset):
    project_id = app_identity.get_application_id()
    bq_client = BigQueryClient(project_id)
    for table in VOCABULARY_TABLES:
        if bq_client.table_exists(table, dataset_id=input_dataset):
            pass


def parse_args(raw_args=None):
    """
    Parse command line arguments.

    Returns a dictionary of namespace arguments.
    """
    parser = ArgumentParser(description='Parse deid command line arguments')
    parser.add_argument('-i',
                        '--idataset',
                        action='store',
                        dest='input_dataset',
                        help='Name of the input dataset',
                        required=True)
    parser.add_argument('-p',
                        '--private_key',
                        dest='private_key',
                        action='store',
                        required=True,
                        help='Service account file location')
    parser.add_argument('-o',
                        '--odataset',
                        action='store',
                        dest='odataset',
                        type=odataset_name_verification,
                        help='Name of the output dataset must end with _deid ',
                        required=True)
    parser.add_argument(
        '-a',
        '--action',
        dest='action',
        action='store',
        required=True,
        choices=['submit', 'simulate', 'debug'],
        help=('simulate: generate simulation without creating an '
              'output table\nsubmit: create an output table\n'
              'debug: print output without simulation or submit '
              '(runs alone)'))
    parser.add_argument(
        '-s',
        '--skip-tables',
        dest='skip_tables',
        action='store',
        required=False,
        default='',
        help=('A comma separated list of table to skip.  Useful '
              'to avoid de-identifying a table that has already '
              'undergone deid.'))
    parser.add_argument(
        '--tables',
        dest='tables',
        action='store',
        required=False,
        default='',
        help=('A comma separated list of specific tables to execute '
              'deid on.  Defaults to all tables.'))
    parser.add_argument(
        '--interactive',
        dest='interactive_mode',
        action='store_true',
        required=False,
        help=('Execute queries in INTERACTIVE mode.  Defaults to '
              'execute queries in BATCH mode.'))
    parser.add_argument('-c',
                        '--console-log',
                        dest='console_log',
                        action='store_true',
                        required=False,
                        help='Log to the console as well as to a file.')
    parser.add_argument('--version', action='version', version='deid-02')
    parser.add_argument('-m',
                        '--age_limit',
                        dest='age_limit',
                        action='store',
                        required=True,
                        help='Set the maximum allowable age of participants.')
    return parser.parse_args(raw_args)


def copy_deid_map_table(client, deid_map_table, lookup_dataset_id,
                        input_dataset_id, age_limit):
    """
    Copies research_ids for participants whose age is below max_age limit from pipeline_tables._deid_map table
     to input_dataset._deid_map table.

    :param client: a BigQueryClient
    :param deid_map_table: Fully Qualified(fq) _deid_map table name to create
    :param lookup_dataset_id: Name of the dataset where the master _deid_map table is stored
    :param input_dataset_id: Name of the dataset where _deid_map dataset needs to be created.
    :param age_limit: Allowed Max_age of a participant
    :return: None
    """
    q = COPY_PID_RID_QUERY.render(map_table=deid_map_table,
                                  project=client.project,
                                  lookup_dataset=lookup_dataset_id,
                                  input_dataset=input_dataset_id,
                                  max_age=age_limit,
                                  PIPELINE_TABLES=PIPELINE_TABLES,
                                  pid_rid_table=DEID_MAP_TABLE)

    query_job = client.query(q)
    query_job.result()
    if query_job.exception():
        logging.error(f"The _deid_map table was not copied successfully")


def load_deid_map_table(client, deid_map_dataset_name, age_limit):
    # Create _deid_map table in input dataset
    deid_map_table = f'{client.project}.{deid_map_dataset_name}._deid_map'

    # Copy master _deid_map table records to _deid_map table
    if client.table_exists(DEID_MAP_TABLE, dataset_id=PIPELINE_TABLES_DATASET):
        copy_deid_map_table(client, deid_map_table, PIPELINE_TABLES_DATASET,
                            deid_map_dataset_name, age_limit)
        logging.info(
            f"copied participants younger than {age_limit} to the table {deid_map_table}"
        )
    else:
        raise RuntimeError(
            f'{DEID_MAP_TABLE} is not available in {client.project}.{PIPELINE_TABLES_DATASET}'
        )


def copy_ext_tables(bq_client, input_dataset: str, output_dataset: str) -> list:
    """
    Copy extension tables to the deid dataset

    :param bq_client: a BigQueryClient
    :param input_dataset: Name of the input dataset
    :param output_dataset: Name of the output dataset
    :return: job_list: list of job_ids
    """
    source_tables = bq_client.list_tables(
        f'{bq_client.project}.{input_dataset}')
    job_config = CopyJobConfig(write_disposition=WriteDisposition.WRITE_EMPTY)
    job_list = []
    for table in source_tables:
        if table.table_id.endswith(EXT_SUFFIX):
            destination_table = f'{output_dataset}.{table.table_id}'
            job_config.labels.update({
                'table_name': table.table_id.lower(),
                'copy_from': input_dataset.lower(),
                'copy_to': output_dataset.lower()
            })
            job = bq_client.copy_table(table,
                                       destination_table,
                                       job_config=job_config)
            job_list.append(job.job_id)
    return job_list


def main(raw_args=None):
    """
    Execute deid as a single script.

    Responsible for aggregating the tables deid will execute on and calling deid.
    """
    args = parse_args(raw_args)
    add_console_logging(args.console_log)

    project_id = app_identity.get_application_id()
    bq_client = BigQueryClient(project_id)

    known_tables = get_known_tables(fields_path)
    deid_tables_path = os.path.join(DEID_PATH, 'config', 'ids', 'tables')
    configured_tables = get_known_tables(deid_tables_path)
    tables = get_output_tables(bq_client, args.input_dataset, known_tables,
                               args.skip_tables, args.tables)
    logging.info(f"Loading {DEID_MAP_TABLE} table...")
    load_deid_map_table(bq_client,
                        deid_map_dataset_name=args.input_dataset,
                        age_limit=args.age_limit)
    logging.info(f"Loaded {DEID_MAP_TABLE} table.")

    exceptions = []
    successes = []
    for table in tables:
        tablepath = None
        if table in configured_tables:
            tablepath = os.path.join(deid_tables_path, table + '.json')
        else:
            tablepath = table

        parameter_list = [
            '--rules',
            os.path.join(DEID_PATH, 'config', 'ids', 'config.json'),
            '--private_key', args.private_key, '--table', tablepath, '--action',
            args.action, '--idataset', args.input_dataset, '--log', LOGS_PATH,
            '--odataset', args.odataset, '--age-limit', args.age_limit
        ]

        if args.interactive_mode:
            parameter_list.append('--interactive')

        field_names = [field.get('name') for field in fields_for(table)]
        if 'person_id' in field_names:
            parameter_list.append('--cluster')

        LOGGER.info(
            f"Executing deid with:\n\tpython deid/aou.py {' '.join(parameter_list)}"
        )

        try:
            aou.main(parameter_list)
        except google.api_core.exceptions.GoogleAPIError:
            LOGGER.exception("Encountered deid exception:\n")
            exceptions.append(table)
        else:
            LOGGER.info(f"Successfully executed deid on table: {table}")
            successes.append(table)

    copy_suppressed_table_schemas(known_tables, args.odataset)

    logging.info(
        f"Copying ext tables from {args.input_dataset} dataset to {args.odataset} dataset..."
    )
    copy_job_list = copy_ext_tables(bq_client, args.input_dataset,
                                    args.odataset)
    bq_client.wait_on_jobs(copy_job_list)
    logging.info(f"Finished copying ext tables.")

    LOGGER.info(
        "Deid has finished.  Successfully executed on tables: {}".format(
            '\n'.join(successes)))
    for exc in exceptions:
        LOGGER.error(f"Deid encountered exceptions when processing table: {exc}"
                     f".  Fix problems and re-run deid for table if needed.")


if __name__ == '__main__':
    main()
