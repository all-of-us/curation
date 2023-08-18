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

from common import BIGQUERY_DATASET_ID
from bq_utils import app_identity
from gcloud.bq import BigQueryClient
from validation.main import _upload_achilles_files
from validation.main import run_achilles as _run_achilles
from validation.main import run_export as _run_export


def main(args):
    dataset_id = BIGQUERY_DATASET_ID
    target_bucket = args.bucket
    folder_prefix = args.folder + '/'
    project = app_identity.get_application_id()
    bq_client = BigQueryClient(project)
    _run_achilles(client=bq_client)
    _run_export(datasource_id=dataset_id,
                folder_prefix=folder_prefix,
                target_bucket=target_bucket)
    _upload_achilles_files(folder_prefix=folder_prefix,
                           target_bucket=target_bucket)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--bucket', help='Identifier for the storage bucket.')
    parser.add_argument(
        '--folder',
        default='',
        help='Identifier for the folder in which achilles results sit.')
    args = parser.parse_args()
    main(args)
