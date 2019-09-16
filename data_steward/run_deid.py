"""
Deid runner.

A central script to execute deid for each table needing de-identification.
"""
from argparse import ArgumentParser
import logging
import os

import google

import bq_utils
from resources import fields_for, fields_path
import deid.aou as aou


LOGGER = logging.getLogger(__name__)
DEID_TABLES = ['person', 'observation', 'visit_occurrence', 'condition_occurrence',
               'drug_exposure', 'procedure_occurrence', 'device_exposure', 'death',
               'measurement', 'location', 'care_site', 'specimen', 'observation_period']
SUPPRESSED_TABLES = ['note', 'note_nlp']

def add_console_logging(add_handler):
    """
    This config should be done in a separate module, but that can wait
    until later.  Useful for debugging.
    """
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    if add_handler:
        handler = logging.StreamHandler()
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(levelname)s - %(name)s - %(message)s')
        handler.setFormatter(formatter)
        LOGGER.addHandler(handler)


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


def get_output_tables(input_dataset, known_tables, skip_tables, only_tables):
    """
    Get list of output tables deid should produce.

    Specifically excludes table names that start with underscores, pii, or
    are explicitly suppressed.

    :param input_dataset:  dataset to read when gathering all possible table names.
    :param known_tables:  list of tables known to curation.  If a table exists in
        the input dataset but is not known to curation, it is skippped.
    :param skip_tables:  command line csv string of tables to skip for deid.
        Useful to perform deid on a subset of tables.

    :return: a list of table names to execute deid over.
    """
    tables = bq_utils.list_dataset_contents(input_dataset)
    skip_tables = [table.strip() for table in skip_tables.split(',')]
    only_tables = [table.strip() for table in only_tables.split(',')]

    allowed_tables = []
    for table in tables:
        if table.startswith('_'):
            continue
        if table.startswith('pii'):
            continue
        if table in SUPPRESSED_TABLES:
            continue
        # doing this to eliminate the 'deid_map' table and any other non-OMOP table
        if table not in known_tables:
            continue
        if table in skip_tables:
            continue

        if (only_tables == [''] or table in only_tables) and table in DEID_TABLES:
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
            bq_utils.create_table(
                table,
                field_list,
                drop_existing=True,
                dataset_id=dest_dataset
            )


def parse_args(raw_args=None):
    """
    Parse command line arguments.

    Returns a dictionary of namespace arguments.
    """
    parser = ArgumentParser(description='Parse deid command line arguments')
    parser.add_argument('-i', '--idataset',
                        action='store', dest='input_dataset',
                        help=('Name of the input dataset (an output dataset '
                              'with suffix _deid will be generated)'),
                        required=True)
    parser.add_argument('-p', '--private_key', dest='private_key', action='store',
                        required=True,
                        help='Service account file location')
    parser.add_argument('-a', '--action', dest='action', action='store', required=True,
                        choices=['submit', 'simulate', 'debug'],
                        help=('simulate: generate simulation without creating an '
                              'output table\nsubmit: create an output table\n'
                              'debug: print output without simulation or submit '
                              '(runs alone)')
                       )
    parser.add_argument('-s', '--skip-tables', dest='skip_tables', action='store',
                        required=False, default='',
                        help=('A comma separated list of table to skip.  Useful '
                              'to avoid de-identifying a table that has already '
                              'undergone deid.')
                       )
    parser.add_argument('--tables', dest='tables', action='store', required=False, default='',
                        help=('A comma separated list of specific tables to execute '
                              'deid on.  Defaults to all tables.')
                       )
    parser.add_argument('--interactive', dest='interactive_mode', action='store_true',
                        required=False,
                        help=('Execute queries in INTERACTIVE mode.  Defaults to '
                              'execute queries in BATCH mode.')
                       )
    parser.add_argument('--version', action='version', version='deid-02')
    return parser.parse_args(raw_args)


def main(raw_args=None):
    """
    Execute deid as a single script.

    Responsible for aggregating the tables deid will execute on and calling deid.
    """
    add_console_logging(False)
    args = parse_args(raw_args)
    known_tables = get_known_tables(fields_path)
    configured_tables = get_known_tables('deid/config/ids/tables')
    tables = get_output_tables(args.input_dataset, known_tables, args.skip_tables, args.tables)

    exceptions = []
    successes = []
    for table in tables:
        tablepath = None
        if table in configured_tables:
            tablepath = 'deid/config/ids/tables/' + table + '.json'
        else:
            tablepath = table

        parameter_list = [
            '--rules', 'deid/config/ids/config.json',
            '--private_key', args.private_key,
            '--table', tablepath,
            '--action', args.action,
            '--idataset', args.input_dataset
        ]

        if args.interactive_mode:
            parameter_list.append('--interactive')

        field_names = [field.get('name') for field in fields_for(table)]
        if 'person_id' in field_names:
            parameter_list.append('--cluster')

        LOGGER.info('Executing deid with:\n\tpython deid/aou.py %s', ' '.join(parameter_list))

        try:
            aou.main(parameter_list)
        except google.api_core.exceptions.GoogleAPIError:
            LOGGER.exception("Encountered deid exception:\n")
            exceptions.append(table)
        else:
            LOGGER.info('Successfully executed deid on table: %s', table)
            successes.append(table)

    copy_suppressed_table_schemas(tables, args.input_dataset + '_deid')

    LOGGER.info('Deid has finished.  Successfully executed on tables:  %s',
                '\n'.join(successes))
    for exc in exceptions:
        LOGGER.info("Deid encountered exceptions when processing table: %s"
                    ".  Fix problems and re-run deid for table if needed.",
                    exc)


if __name__ == '__main__':
    main()
