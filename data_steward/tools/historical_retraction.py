# coding=utf-8
"""
Simulate cron retraction
"""
import logging

import bq_utils
import constants.global_variables
from cdr_cleaner import clean_cdr
from cdr_cleaner.args_parser import add_kwargs_to_args
from constants.cdr_cleaner.clean_cdr import CRON_RETRACTION
from gcloud.bq import BigQueryClient
from retraction import retract_data_bq, retract_data_gcs


def run_cron_retraction():
    """
    Simulate cron retraction

    :return:
    """
    constants.global_variables.DISABLE_SANDBOX = True
    project_id = bq_utils.app_identity.get_application_id()
    hpo_id = bq_utils.get_retraction_hpo_id()
    retraction_type = bq_utils.get_retraction_type()
    pid_table_id = bq_utils.get_retraction_pid_table_id()
    sandbox_dataset_id = bq_utils.get_retraction_sandbox_dataset_id()

    # Dataset and table containing list of datasets
    datasets_to_retract_dataset = bq_utils.get_retraction_dataset_ids_dataset()
    datasets_to_retract_table = bq_utils.get_retraction_dataset_ids_table()

    # retract from bq
    if not datasets_to_retract_table or not datasets_to_retract_dataset:
        logging.info(
            f"Retraction cannot run without RETRACTION_DATASET_IDS_TABLE and RETRACTION_DATASET_IDS_DATASET"
        )
        return 'retraction-skipped'

    bq_client = BigQueryClient(project_id)
    dataset_query_job = bq_client.query(
        f"SELECT * FROM {project_id}.{datasets_to_retract_dataset}.{datasets_to_retract_table}"
    )
    dataset_ids_result = dataset_query_job.result()
    dataset_ids = dataset_ids_result.to_dataframe()["datasets"].to_list()
    logging.info(f"Dataset id/s to target from table: {dataset_ids}")
    logging.info(f"Running retraction on BQ datasets")

    # retract from default dataset
    # retract_data_bq.run_bq_retraction(project_id,
    #                                   sandbox_dataset_id,
    #                                   pid_table_id,
    #                                   hpo_id,
    #                                   dataset_ids,
    #                                   retraction_type,
    #                                   skip_sandboxing=True,
    #                                   bq_client=bq_client)
    logging.info(f"Completed retraction on BQ datasets")

    # Run cleaning rules
    for dataset_id in dataset_ids:
        logging.info(f"Running CRs for {dataset_id}...")
        cleaning_args = [
            '-p', project_id, '-d', dataset_id, '-b', sandbox_dataset_id,
            '--data_stage', CRON_RETRACTION, '--run_as',
            f'{project_id}@appspot.gserviceaccount.com', '-s'
        ]
        all_cleaning_args = add_kwargs_to_args(cleaning_args, None)
        clean_cdr.main(args=all_cleaning_args)
        logging.info(f"Completed running CRs for {dataset_id}...")

        # retract from gcs
    if retraction_type == 'bucket':
        folder = bq_utils.get_retraction_submission_folder()
        logging.info(
            f"Submission folder/s to target from env variable: {folder}")
        logging.info(f"Running retraction from internal bucket folders")
        retract_data_gcs.run_gcs_retraction(project_id,
                                            sandbox_dataset_id,
                                            pid_table_id,
                                            hpo_id,
                                            folder,
                                            force_flag=True)
        logging.info(f"Completed retraction from internal bucket folders")


if __name__ == '__main__':
    from utils import pipeline_logging
    pipeline_logging.configure()
    run_cron_retraction()
