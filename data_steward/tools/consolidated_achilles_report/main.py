import gcs_utils
import json
import os
import query_reports

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

    :return:
    """
    results = []
    for upload in uploads:
        dte, p = upload['upload_timestamp'], upload['file_path']
        hpo_id = get_hpo_id(p)
        report_path = p.replace('datasources.json', hpo_id)
        name = get_submission_name(p)
        result = dict(hpo_id=hpo_id, updated=dte, report_path=report_path, name=name)
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
    print 'Updating source name in %s...' % pth
    write_text(pth, txt)


def datasource_for(rpt):
    return {'folder': rpt['hpo_id'], 'cdmVersion': 5, 'name': rpt['hpo_id']}


def update_datasources(rpts):
    datasources = []
    for rpt in rpts:
        datasource = datasource_for(rpt)
        datasources.append(datasource)
    obj = {'datasources': datasources}
    print 'Saving datasources to %s...' % DATASOURCES_PATH
    write_json(DATASOURCES_PATH, obj)


def download_report(s):
    """
    Download most recent report files
    :param s:
    :return:
    """
    # Save it to curation_report/data/<hpo_id>
    cdir = os.getcwd()
    if not os.path.exists('%s/curation_report/data' % (cdir)):
        os.mkdir('%s/curation_report/data' % (cdir))

    if os.path.exists('%s/curation_report/data/%s' % (cdir, s['hpo_id'])):
            cmd = 'gsutil -m cp -r %s ./curation_report/data/' % (s['report_path'])
            print 'Downloading %s rpt with cmd: `%s`...' % (s['hpo_id'], cmd)
            os.system(cmd)
    else:
            os.mkdir('%s/curation_report/data/%s' % (cdir, s['hpo_id']))
            cmd = 'gsutil -m cp -r %s ./curation_report/data/' % (s['report_path'])
            print 'Downloading %s rpt with cmd: `%s`...' % (s['hpo_id'], cmd)
            os.system(cmd)


def main():
    bq_list = query_reports.get_most_recent()
    reports = transform_bq_list(bq_list)
    for report in reports:
        print 'processing report: \n %s\n...' % json.dumps(report, indent=4)
        download_report(report)
        update_source_name(report)
    update_datasources(reports)


if __name__ == '__main__':
    main()
