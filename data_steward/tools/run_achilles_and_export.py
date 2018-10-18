"""
Run achilles, achilles_heel, and export on an existing OMOP CDM BigQuery dataset

The following environment variables must be set:
  * BIGQUERY_DATASET_ID: BQ dataset where the OMOP CDM is stored
  * APPLICATION_ID: GCP project ID (e.g. all-of-us-ehr-dev)
  * GOOGLE_APPLICATION_CREDENTIALS: location of service account key json file (e.g. /path/to/all-of-us-ehr-dev-abc123.json)

Note: Any missing CDM tables will be created and will remain empty
"""
import argparse
from validation.main import run_export as _run_export
from validation.main import run_achilles as _run_achilles
from validation.main import _upload_achilles_files
from gcs_utils import get_hpo_bucket


def main(args):
    folder = args.folder
    target_bucket = args.bucket
    folder_prefix = folder + '/'
    _run_achilles()
    _run_export(folder_prefix=folder_prefix, target_bucket=target_bucket)
    _upload_achilles_files(folder_prefix=folder_prefix, target_bucket=target_bucket)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--bucket',
                        default=get_hpo_bucket('nyc'),
                        help='Identifier for the bucket. Output tables will be prepended with {hpo_id}_.')
    parser.add_argument('--folder',
                        default='',
                        help='Identifier for the folder in which achilles results sit. Output tables will be prepended with {hpo_id}_.')
    args = parser.parse_args()
    main(args)
