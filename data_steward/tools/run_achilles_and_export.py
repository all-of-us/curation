"""
run achilles, achilles_heel, and export on a particular dataset as defined by the environment variables.

    -- hpo_id is an arugment wherein '{hpo_id}_' will be prepended when writing/reading tables.
    -- cdm tables that dont exist will be created as empty tables.
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
                        help='which HPO to run this as')
    args = parser.parse_args()
    main(args)
