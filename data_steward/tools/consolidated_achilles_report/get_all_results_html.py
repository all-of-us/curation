from __future__ import print_function
import gcs_utils
import json
import os
import query_reports

DRC_BUCKET_PATH = 'gs://%s/' % gcs_utils.get_drc_bucket()


def get_hpo_id(p):
    rel_path = p[len(DRC_BUCKET_PATH):]
    return rel_path[:rel_path.index('/')]


def transform_bq_list(uploads):
    """
    Get paths to all most recent report files

    :return:
    """
    results = []
    for upload in uploads:
        dte, p = upload['upload_timestamp'], upload['file_path']
        hpo_id = get_hpo_id(p)
        report_path = p.replace('person.csv', 'results.html')
        result = dict(hpo_id=hpo_id, updated=dte, report_path=report_path)
        results.append(result)
    return results


def download_report(s):
    """
    Download most recent report files
    :param s:
    :return:
    """
    # Save it to curation_report/data/<hpo_id>
    cdir = os.getcwd()
    if not os.path.exists('%s/result_data' % (cdir)):
        os.mkdir('%s/result_data' % (cdir))

    if os.path.exists('%s/result_data/%s' % (cdir, s['hpo_id'])):
        cmd = 'gsutil -m cp -r %s ./result_data/%s/' % (s['report_path'], s['hpo_id'])
        print('Downloading %s rpt with cmd: `%s`...' % (s['hpo_id'], cmd))
        os.system(cmd)
    else:
        os.mkdir('%s/result_data/%s' % (cdir, s['hpo_id']))
        cmd = 'gsutil -m cp -r %s ./result_data/%s/' % (s['report_path'], s['hpo_id'])
        print('Downloading %s rpt with cmd: `%s`...' % (s['hpo_id'], cmd))
        os.system(cmd)


def main():

    bq_list = query_reports.get_most_recent(report_for='results')
    reports = transform_bq_list(bq_list)
    for report in reports:
        print('processing report: \n %s\n...' % json.dumps(report, indent=4))
        download_report(report)


if __name__ == '__main__':
    main()
