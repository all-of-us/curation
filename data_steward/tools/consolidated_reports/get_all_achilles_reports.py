from __future__ import print_function

import json
import os

import gcs_utils
import query_reports
import common

DRC_BUCKET_PATH = 'gs://%s/' % gcs_utils.get_drc_bucket()
DATASOURCES_PATH = 'curation_report/data/datasources.json'


def get_hpo_id(p):
    rel_path = p[len(DRC_BUCKET_PATH):]
    return rel_path[:rel_path.index('/')]


def get_report_path(p, hpo_id):
    return p.replace('datasources.json', hpo_id)


def get_submission_name(p):
    parts = p.split('/')
    for i in range(0, len(parts)):
        part = parts[i]
        if part == 'curation_report':
            return parts[i - 1]
    raise RuntimeError('Invalid submission path: %s' % p)


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
        report_path = p.replace('datasources.json', hpo_id)
        name = get_submission_name(p)
        result = {'hpo_id': hpo_id, 'updated': dte, 'report_path': report_path, 'name': name}
        results.append(result)
    return results


def read_text(p):
    with open(p, 'r') as fp:
        return fp.read()


def write_text(p, t):
    with open(p, 'w') as fp:
        fp.write(t)


def write_json(pth, obj):
    with open(pth, 'w') as fp:
        json.dump(obj, fp, indent=4)


def update_source_name(rpt):
    pth = 'curation_report/data/%s/person.json' % rpt['hpo_id']
    txt = read_text(pth).replace('my_source', rpt['hpo_id'])
    print('Updating source name in %s...' % pth)
    write_text(pth, txt)


def datasource_for(rpt):
    return {'folder': rpt['hpo_id'], 'cdmVersion': 5, 'name': rpt['hpo_id']}


def update_datasources(rpts):
    datasources = []
    for rpt in rpts:
        datasource = datasource_for(rpt)
        datasources.append(datasource)
    obj = {'datasources': datasources}
    print('Saving datasources to %s...' % DATASOURCES_PATH)
    write_json(DATASOURCES_PATH, obj)


def download_report(path_dict):
    """
    Download most recent report files
    :param path_dict: A Dictionary Which containing details of bucket parsed from the path.
    :return: None
    """
    # Save it to curation_report/data/<hpo_id>
    cdir = os.getcwd()
    try:
        os.mkdir('%s/curation_report/data' % cdir)

    except OSError:
        # log the exception but keep moving because it doesn't hurt your code.
        print("The file %s/result_data/%s already exists", cdir, path_dict['hpo_id'])
    cmd = 'gsutil -m cp -r %s ./curation_report/data/' % (path_dict['report_path'])
    print('Downloading %s rpt with cmd: `%s`...' % (path_dict['hpo_id'], cmd))
    os.system(cmd)


def main():
    bq_list = query_reports.get_most_recent(report_for=common.REPORT_FOR_ACHILLES)
    reports = transform_bq_list(bq_list)
    for report in reports:
        print('processing report: \n %s\n...' % json.dumps(report, indent=4))
        download_report(report)
        update_source_name(report)
    update_datasources(reports)


if __name__ == '__main__':
    main()
