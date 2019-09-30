import argparse

import cdm
from bq_utils import *


def create_empty_dataset(project_id, dataset_id, snapshot_dataset_id):
    """
    Create the empty tables in the new snapshot dataset
    :param project_id:
    :param dataset_id:
    :param snapshot_dataset_id:
    :return:
    """
    create_dataset(
        project_id=project_id,
        dataset_id=snapshot_dataset_id,
        description='Snapshot of {dataset_id}'.format(dataset_id=dataset_id),
        overwrite_existing=True)


def create_empty_cdm_tables(snapshot_dataset_id):
    """
    Copy the table content from the current dataset to the snapshot dataset
    :param snapshot_dataset_id:
    :return:
    """
    cdm.create_all_tables(snapshot_dataset_id)
    cdm.create_vocabulary_tables(snapshot_dataset_id)


def copy_tables_to_new_dataset(project_id, dataset_id, snapshot_dataset_id):
    """
    lists the tables in the dataset and copies each table to a new dataset.
    :param dataset_id:
    :param project_id:
    :param snapshot_dataset_id:
    :return:
    """

    copy_table_job_ids = []
    for table_id in list_all_table_ids(dataset_id):
        select_all_query = ('SELECT * FROM `{project_id}.{dataset_id}.{table_id}` ')
        q = select_all_query.format(project_id=project_id,
                                    dataset_id=dataset_id, table_id=table_id)
        results = query(q, use_legacy_sql=False, destination_table_id=table_id,
                        destination_dataset_id=snapshot_dataset_id, batch=True)
        copy_table_job_ids.append(results['jobReference']['jobId'])
    incomplete_jobs = wait_on_jobs(copy_table_job_ids)
    if len(incomplete_jobs) > 0:
        raise BigQueryJobWaitError(incomplete_jobs)


def create_snapshot_dataset(project_id, dataset_id, snapshot_dataset_id):
    """
    :param project_id:
    :param dataset_id:
    :param snapshot_dataset_id:
    :return:
    """
    create_empty_dataset(project_id, dataset_id, snapshot_dataset_id)

    create_empty_cdm_tables(snapshot_dataset_id)

    copy_tables_to_new_dataset(project_id, dataset_id, snapshot_dataset_id)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Parse project_id and dataset_id',
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-p', '--project_id',
                        action='store', dest='project_id',
                        help='Project associated with the input and output datasets',
                        required=True)
    parser.add_argument('-d', '--dataset_id',
                        action='store', dest='dataset_id',
                        help='Dataset where cleaning rules are to be applied',
                        required=True)
    parser.add_argument('-n', '--snapshot_dataset_id',
                        action='store', dest='snapshot_dataset_id',
                        help='Name of the new dataset that needs to be created',
                        required=True)
    args = parser.parse_args()

    create_snapshot_dataset(args.project_id, args.dataset_id, args.snapshot_dataset_id)
