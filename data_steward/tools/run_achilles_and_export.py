"""
Run achilles, achilles_heel, and export on an existing OMOP CDM BigQuery dataset

The following environment variables must be set:
  * BIGQUERY_DATASET_ID: BQ dataset where the OMOP CDM is stored
  * APPLICATION_ID: GCP project ID (e.g. all-of-us-ehr-dev)
  * GOOGLE_APPLICATION_CREDENTIALS: location of service account key json file (e.g.
  /path/to/all-of-us-ehr-dev-abc123.json)

Note: Any missing CDM tables will be created and will remain empty
"""
import argparse

from bq_utils import get_dataset_id
from validation.main import _upload_achilles_files
from validation.main import run_achilles as _run_achilles
from validation.main import run_export as _run_export
from validation.main import run_export_dashboard_bucket as _run_export_dashboard_bucket


def main(args):
    folder = args.folder
    dataset_id = get_dataset_id()
    target_bucket = args.bucket
    achilles_transfer_bucket = args.transfer_bucket
    data_stage = args.data_stage
    folder_prefix = folder + '/'
    _run_achilles()
    _run_export(hpo_id=dataset_id,
                folder_prefix=folder_prefix,
                target_bucket=target_bucket)
    if data_stage == 'combined':
        _run_export_dashboard_bucket(hpo_id=dataset_id,
                                     target_bucket=achilles_transfer_bucket)
    _upload_achilles_files(folder_prefix=folder_prefix,
                           target_bucket=target_bucket)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--bucket',
                        help='Identifier for the Achilles Transfer Bucket')
    parser.add_argument(
        '--folder',
        help='Identifier for the folder in which achilles results sit.',
        required=True)
    parser.add_argument(
        '--transfer_bucket',
        help='Achilles Transfer bucket name',
    )
    parser.add_argument('--data_stage',
                        required=True,
                        help='Specify the data stage')
    args = parser.parse_args()
    main(args)
