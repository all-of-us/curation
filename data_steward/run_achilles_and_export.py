"""
Combine synthetic EHR and RDR data sets to form another data set

 * Create a mapping table which arbitrarily maps EHR person_id to RDR person_id and assigns a cdr_id
 * For each CDM table, load the EHR data and append RDR data (ignore RDR person table)
 * RDR entity IDs (e.g. visit_occurrence_id, measurement_id) start at 1B
"""
import sys
sys.path.append('./lib/')
sys.path.append('/usr/lib/google-cloud-sdk/platform/google_appengine/')

import argparse
import json
import os
import logging

import common
from validation import export
import gcs_utils
import StringIO


import bq_utils
from validation import achilles
from validation import achilles_heel
from google.appengine.ext import testbed


def _run_export(hpo_id):
    results = []
    logging.info('running export for hpo_id %s' % hpo_id)
    # TODO : add check for required tables
    hpo_bucket = gcs_utils.get_hpo_bucket(hpo_id)
    for export_name in common.ALL_REPORTS:
        sql_path = os.path.join(export.EXPORT_PATH, export_name)
        result = export.export_from_path(sql_path, hpo_id)
        content = json.dumps(result)
        fp = StringIO.StringIO(content)
        result = gcs_utils.upload_object(hpo_bucket, export_name + '.json', fp)
        results.append(result)
    return results


def _run_achilles(hpo_id):
    """checks for full results and run achilles/heel

    :hpo_id: hpo on which to run achilles
    :returns:
    """
    logging.info('running achilles for hpo_id %s' % hpo_id)
    achilles.create_tables(hpo_id, True)
    achilles.load_analyses(hpo_id)
    achilles.run_analyses(hpo_id=hpo_id)
    logging.info('running achilles_heel for hpo_id %s' % hpo_id)
    achilles_heel.create_tables(hpo_id, True)
    achilles_heel.run_heel(hpo_id=hpo_id)


def main(args):
    hpo_id = args.hpo_id
    for table_name in common.CDM_TABLES:
        table_id = hpo_id+'_'+table_name
        if bq_utils.table_exists(table_id):
            print table_id,' exists'
        else:
            print table_id,' being created'
            # bq_utils.create_standard_table(table_name, table_id, False)

    _run_achilles(hpo_id)
    _run_export(hpo_id)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--hpo_id',
                        default='fake',
                        help='which HPO to run this as')
    args = parser.parse_args()
    main(args)
