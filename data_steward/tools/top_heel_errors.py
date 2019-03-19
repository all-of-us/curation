"""
Fetch the most prevalent achilles heel errors in a dataset
"""
import resources
import bq_utils
import os
import json
import csv
from google.appengine.api.app_identity import app_identity
from functools import partial


HPO_ID_LIST = [item['hpo_id'] for item in resources.hpo_csv()]
JSON = 'json'
CSV = 'csv'
OUTPUT_FORMATS = [CSV, JSON]

# Query-related data variables #
# Output field names
FIELD_DATASET_NAME = 'dataset_name'
FIELD_ANALYSIS_ID = 'analysis_id'
FIELD_ACHILLES_HEEL_WARNING = 'achilles_heel_warning'
FIELD_HEEL_ERROR = 'heel_error'
FIELD_RULE_ID = 'rule_id'
FIELD_RECORD_COUNT = 'record_count'
FIELDS = [FIELD_DATASET_NAME, FIELD_ANALYSIS_ID, FIELD_HEEL_ERROR, FIELD_RULE_ID, FIELD_RECORD_COUNT]
RESULT_LIMIT = 10

# Query template
HEEL_ERROR_QUERY = '''
 SELECT '{dataset_name}' AS {field_dataset_name},
  analysis_id,
  achilles_heel_warning AS {field_heel_error},
  rule_id,
  record_count
 FROM `{app_id}.{dataset_id}.{table_id}`
 WHERE achilles_heel_warning LIKE 'ERROR:%'
 ORDER BY record_count DESC LIMIT {result_limit}
'''

# The function query_format is used to fill in the remaining components after substituting aliases, result limit
# ex: query_format(dataset_name='', app_id='')
query_format = partial(HEEL_ERROR_QUERY.format,
                       field_dataset_name=FIELD_DATASET_NAME,
                       field_heel_error=FIELD_HEEL_ERROR,
                       result_limit=RESULT_LIMIT)
UNION_ALL = '''
UNION ALL
'''


def save_csv(l, file_name):
    """
    Save results to a local comma-separated file

    :param l: List of dict
    :param file_name: Path of file to save to
    """
    with open(file_name, 'w') as fp:
        writer = csv.DictWriter(fp, FIELDS)
        writer.writeheader()
        writer.writerows(l)


def save_json(l, file_name):
    """
    Save results to a local json file

    :param l: List of dict
    :param file_name: Path of file to save to
    """
    with open(file_name, 'w') as fp:
        json.dump(l, fp, sort_keys=True, indent=4)


def get_hpo_subqueries(app_id, dataset_id, all_table_ids):
    result = []
    for hpo_id in HPO_ID_LIST:
        table_id = bq_utils.get_table_id(hpo_id, resources.ACHILLES_HEEL_RESULTS)
        if table_id in all_table_ids:
            subquery = query_format(dataset_name=hpo_id,
                                    app_id=app_id,
                                    dataset_id=dataset_id,
                                    table_id=table_id)
            result.append(subquery)
    return result


def construct_query(app_id, dataset_id, all_hpo=False):
    """
    Construct appropriate and executable query to retrieve most prevalent errors from achilles heel results table(s)

    :param app_id: Identifies the google cloud project containing the dataset
    :param dataset_id: Identifies the dataset where from achilles heel results should be obtained
    :param all_hpo: If `True` query <hpo_id>_achilles_heel_results, otherwise just achilles_heel_results (default)
    :return: The query or :None if no achilles heel results table is found whatsoever
    """
    query = None
    # Resulting query should only reference existing tables in dataset
    all_tables = bq_utils.list_tables(dataset_id)
    all_table_ids = [r['tableReference']['tableId'] for r in all_tables]
    if all_hpo:
        # Fetch and union results from all <hpo_id>_achilles_heel_results tables
        subqueries = get_hpo_subqueries(app_id, dataset_id, all_table_ids)
        enclosed = map(lambda s: '(%s)' % s, subqueries)
        query = UNION_ALL.join(enclosed)
    else:
        # Fetch from achilles_heel_results table
        table_id = resources.ACHILLES_HEEL_RESULTS
        if table_id in all_table_ids:
            query = query_format(dataset_name=dataset_id,
                                 app_id=app_id,
                                 dataset_id=dataset_id,
                                 table_id=table_id)
    return query


def top_heel_errors(app_id, dataset_id, all_hpo=False):
    """
    Retrieve most prevalent errors from achilles heel results

    :param app_id: Identifies the google cloud project containing the dataset
    :param dataset_id: Identifies the dataset where from achilles heel results should be obtained
    :param all_hpo: If `True` query <hpo_id>_achilles_heel_results, otherwise just achilles_heel_results (default)
    :return: Results as a list of dict, :None if no achilles heel results table is found whatsoever
    """
    result = None
    query = construct_query(app_id, dataset_id, all_hpo)
    if query:
        # Found achilles_heel_results table(s), run the query
        response = bq_utils.query(query)
        result = bq_utils.response2rows(response)
    return result


def main(app_id, dataset_id, file_name, all_hpo=False, file_format=None):
    """
    Retrieve most prevalent errors from achilles heel results table(s) and save results to a file at specified path

    :param app_id: Identifies the google cloud project containing the dataset
    :param dataset_id: Identifies the dataset where from achilles heel results should be obtained
    :param file_name: Path of file to save to
    :param all_hpo: If `True` query <hpo_id>_achilles_heel_results, otherwise just achilles_heel_results (default)
    :param file_format: csv or json
    """
    if app_id is None:
        app_id = app_identity.get_application_id()
    if dataset_id is None:
        dataset_id = bq_utils.get_dataset_id()
    if os.path.exists(file_name):
        # Do not overwrite existing
        raise IOError('File %s already exists' % file_name)
    if file_format is None:
        # Attempt to determine format
        file_name_comps = file_name.lower().split('.')
        file_format = file_name_comps[-1]
    if file_format not in OUTPUT_FORMATS:
        raise ValueError('File format must be one of (%s)' % ', '.join(OUTPUT_FORMATS))
    heel_errors = top_heel_errors(app_id, dataset_id, all_hpo)
    if file_format == CSV:
        save_csv(heel_errors, file_name)
    elif file_format == JSON:
        save_json(heel_errors, file_name)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--app_id',
                        required=True,
                        help='Identifies the google cloud project')
    parser.add_argument('--dataset_id',
                        required=True,
                        help='Name of the dataset where from achilles heel results should be obtained')
    parser.add_argument('--all_hpo',
                        help='If specified fetch top results for all HPOs',
                        action='store_true')
    parser.add_argument('--format', help='Output format', choices=OUTPUT_FORMATS)
    parser.add_argument('file_name', help='Path of file to save results to')
    args = parser.parse_args()
    main(args.app_id, args.dataset_id, args.file_name, args.all_hpo, args.format)
