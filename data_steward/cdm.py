"""
OMOP CDM utility functions
"""

import argparse
import common
import bq_utils
import logging

logger = logging.getLogger(__name__)


def create_table(table, dataset_id):
    """
    Create OMOP table in the specified dataset

    :param table: CDM table to generate
    :param dataset_id: identifies the dataset to create the tables in
    :return:
    """
    logger.debug('Creating table {dataset_id}.{table}...'.format(table=table, dataset_id=dataset_id))
    bq_utils.create_standard_table(table, table, drop_existing=True, dataset_id=dataset_id)


def create_vocabulary_tables(dataset_id):
    """
    Create OMOP vocabulary tables in the specified dataset
    :param dataset_id:
    :return:
    """
    logger.debug('Creating vocabulary tables in {dataset_id}...'.format(dataset_id=dataset_id))
    for table in common.VOCABULARY_TABLES:
        create_table(table, dataset_id)


def create_all_tables(dataset_id):
    """
    Create all the OMOP clinical data tables in the specified dataset

    :param dataset_id: identifies the dataset to create the tables in
    :return:
    """
    logger.debug('Creating all CDM tables in {dataset_id}...'.format(dataset_id=dataset_id))
    for table in common.CDM_TABLES:
        create_table(table, dataset_id)


if __name__ == '__main__':
    # TODO parse args, support multiple commands
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--table',
                        help='A specific CDM table to create (creates all by default)',
                        choices=list(common.CDM_TABLES))
    parser.add_argument('--component',
                        help='Subset of CDM tables to create',
                        choices=list(common.CDM_COMPONENTS))
    parser.add_argument('dataset_id',
                        help='Identifies the dataset to create OMOP table(s) in')
    args = parser.parse_args()
    if args.table:
        if args.component:
            raise RuntimeError('Cannot process both table and component')
        create_table(args.table, args.dataset_id)
    elif args.component:
        if args.component == common.VOCABULARY:
            create_vocabulary_tables(args.dataset_id)
        elif args.component == common.ACHILLES:
            # TODO implement creating achilles tables; need to fix interdependency of common, resources, cdm
            raise NotImplementedError('Creating achilles tables not yet implemented')
    else:
        create_all_tables(args.dataset_id)
