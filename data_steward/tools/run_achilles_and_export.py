"""
Run achilles, achilles_heel, and export on an existing OMOP CDM BigQuery dataset

The following environment variables must be set:
  * BIGQUERY_DATASET_ID: BQ dataset where the OMOP CDM is stored
  * APPLICATION_ID: GCP project ID (e.g. all-of-us-ehr-dev)
  * GOOGLE_APPLICATION_CREDENTIALS: location of service account key json file (e.g. /path/to/all-of-us-ehr-dev-abc123.json)

Note: Any missing CDM tables will be created and will remain empty
"""
import argparse
import common
import bq_utils
from validation.main import run_export as _run_export
from validation.main import run_achilles as _run_achilles


def main(args):
    hpo_id = args.hpo_id
    for table_name in common.CDM_TABLES:
        table_id = hpo_id + '_' + table_name
        if bq_utils.table_exists(table_id):
            print table_id, ' exists'
        else:
            print table_id, ' being created'
            bq_utils.create_standard_table(table_name, table_id, False)

    _run_achilles(hpo_id)
    _run_export(hpo_id)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--hpo_id',
                        default='fake',
                        help='Identifier for the HPO. Output tables will be prepended with {hpo_id}_.')
    args = parser.parse_args()
    main(args)
