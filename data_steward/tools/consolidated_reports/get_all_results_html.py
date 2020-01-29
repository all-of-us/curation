from __future__ import print_function

import json
import os

import gcs_utils
from tools.consolidated_reports import query_reports

DRC_BUCKET_PATH = 'gs://%s/' % gcs_utils.get_drc_bucket()


def get_hpo_id(p):
    rel_path = p[len(DRC_BUCKET_PATH):]
    return rel_path[:rel_path.index('/')]


def transform_bq_list(uploads):
    """
    Get paths to all most recent report files
    :param uploads: object representing loaded json data
    :return: a list of dictionaries which contains parsed data
    """
    results = []
    for upload in uploads:
        dte, p = upload['upload_timestamp'], upload['file_path']
        hpo_id = get_hpo_id(p)
        report_path = p.replace('person.csv', 'results.html')
        result = {'hpo_id': hpo_id, 'updated': dte, 'report_path': report_path}
        results.append(result)
    return results


def download_report(path_dict):
    """
    Download most recent report files
    :param path_dict: A Dictionary Which containing details of bucket parsed from the path.
    :return: None
    """
    # Save it to curation_report/data/<hpo_id>
    cdir = os.getcwd()
    try:
        os.mkdir('%s/result_data' % (cdir))
    except OSError:
        # log the exception but keep moving because it doesn't hurt your code.
        print("The file %s/result_data/%s already exists", cdir,
              path_dict['hpo_id'])
    cmd = 'gsutil -m cp -r %s ./result_data/%s_results.html' % (
        path_dict['report_path'], path_dict['hpo_id'])
    print('Downloading %s rpt with cmd: `%s`...' % (path_dict['hpo_id'], cmd))
    os.system(cmd)


def main():
    bq_list = query_reports.get_most_recent(report_for='results')
    reports = transform_bq_list(bq_list)
    for report in reports:
        print('processing report: \n %s\n...' % json.dumps(report, indent=4))
        download_report(report)


if __name__ == '__main__':
    main()
