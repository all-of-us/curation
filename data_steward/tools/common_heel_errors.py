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

hpo_ids = [item['hpo_id'] for item in resources.hpo_csv()]
HEEL_ERRORS_JSON = 'heel_errors.json'
HEEL_ERRORS_CSV = 'heel_errors.csv'
heel_error_query ='''select '{hpo_id}' as hpo_name,
            analysis_id, 
            achilles_heel_warning as heel_error,
            rule_id,
            record_count
            FROM `{app_id}.{dataset_id}.{hpo_id}_achilles_heel_results`
            WHERE achilles_heel_warning like 'ERROR:%'
            order by record_count desc limit 5'''


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


def most_common_heel_errors(app_id=None,dataset_id=None, hpo_ids=None):
    """
    :param app_id: Application Id
    :param dataset_id: Dataset Id
    :param hpo_ids: list of Hpo_ids
    :return: None
    """
    heel_errors = list()
    if app_id is None:
        app_id = app_identity.get_application_id()
    if dataset_id is None:
        dataset_id = bq_utils.get_dataset_id()
    if not os.path.exists(HEEL_ERRORS_JSON) and not os.path.exists(HEEL_ERRORS_CSV):
        for hpo_id in hpo_ids:
            if bq_utils.table_exists(table_id='{hpo_id}_achilles_heel_results'.format(hpo_id=hpo_id), dataset_id=dataset_id):
                query = heel_error_query.format(app_id=app_id, dataset_id=dataset_id, hpo_id=hpo_id)
                query_job = bq_utils.query(query)
                result = bq_utils.response2rows(query_job)
                heel_errors.extend(result)
    with open(HEEL_ERRORS_JSON, 'w') as fp:
        json.dump(heel_errors, fp, sort_keys=True, indent=4)
    parse_json_csv()


if __name__ == '__main__':
    most_common_heel_errors(hpo_ids=hpo_ids)