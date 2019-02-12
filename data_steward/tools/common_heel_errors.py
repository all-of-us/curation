"""
Functionality: This script loops through achilles_heel_results table of each site and gets the 5 most common heel errors
per site and stores them in a csv file.
"""
import resources
import bq_utils
import os
import json
import csv
from google.appengine.api.app_identity import app_identity
import argparse

hpo_id_list = [item['hpo_id'] for item in resources.hpo_csv()]
HEEL_ERRORS_JSON = 'heel_errors.json'
HEEL_ERRORS_CSV = 'heel_errors.csv'
heel_error_query ='''select '{hpo_id}' as dataset_name,
            analysis_id, 
            achilles_heel_warning as heel_error,
            rule_id,
            record_count
            FROM `{app_id}.{dataset_id}.{hpo_id}_achilles_heel_results`
            WHERE achilles_heel_warning like 'ERROR:%'
            order by record_count desc limit 10'''

heel_error_query2 = '''select '{dataset_id}' as dataset_name,
            analysis_id, 
            achilles_heel_warning as heel_error,
            rule_id,
            record_count
            FROM `{app_id}.{dataset_id}.achilles_heel_results`
            WHERE achilles_heel_warning like 'ERROR:%'
            order by record_count desc limit 10'''


def parse_json_csv():
    """
    :param key: hard coded value used to select the keys of json files in first iteration of the for loop as the header
    :return: None
    """
    input_file = open(HEEL_ERRORS_JSON)
    parsed_json = json.load(input_file)
    input_file.close()
    error_data = open(HEEL_ERRORS_CSV, 'a')
    output = csv.writer(error_data)
    output.writerow(parsed_json[0].keys())
    for row in parsed_json:
        output.writerow(row.values())


def most_common_heel_errors(app_id, dataset_id, hpo_ids):
    """
    :param app_id: Application Id
    :param dataset_id: Dataset Id
    :param hpo_ids: list of Hpo_ids
    :return: None
    """
    print(hpo_ids)
    heel_errors = list()
    if not os.path.exists(HEEL_ERRORS_JSON) and not os.path.exists(HEEL_ERRORS_CSV):
        if len(hpo_ids) == 1:
            for hpo_id in hpo_ids:
                query = heel_error_query2.format(hpo_id=hpo_id, app_id=app_id, dataset_id=dataset_id)
                query_job = bq_utils.query(query)
                result = bq_utils.response2rows(query_job)
                heel_errors.extend(result)
        else:
            for hpo_id in hpo_ids:
                if bq_utils.table_exists(table_id='{hpo_id}_achilles_heel_results'.
                                         format(hpo_id=hpo_id), dataset_id=dataset_id):
                    query = heel_error_query.format(app_id=app_id, dataset_id=dataset_id, hpo_id=hpo_id)
                    query_job = bq_utils.query(query)
                    result = bq_utils.response2rows(query_job)
                    heel_errors.extend(result)
    with open(HEEL_ERRORS_JSON, 'w') as fp:
        json.dump(heel_errors, fp, sort_keys=True, indent=4)
    parse_json_csv()


def main(report_for, dataset_id, app_id=None):
    if app_id is None:
        app_id = app_identity.get_application_id()
    if dataset_id is None:
        dataset_id = bq_utils.get_dataset_id()
    if report_for == 'hpo':
        print(report_for)
        most_common_heel_errors(hpo_ids=hpo_id_list, app_id=app_id, dataset_id=dataset_id)
    else:
        print(report_for)
        dataset_list = [report_for]
        most_common_heel_errors(hpo_ids=dataset_list, app_id=app_id, dataset_id=dataset_id)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('report_for',
                        help='Name of the dataset on which the heel_results should be obtained.("hpo_ids" for '
                             'all the hpos. Dataset name for any other dataset.')
    parser.add_argument('dataset_id',
                        help='Name of the dataset')

    args = parser.parse_args()
    if args.report_for:
        main(args.report_for, args.dataset_id)
