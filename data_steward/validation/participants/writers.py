#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
A module to write participant identity matching table data.
"""
# Python imports
import logging
import os
from io import StringIO

# Third party imports
import googleapiclient
import oauth2client

# Project imports
import bq_utils
import constants.validation.participants.writers as consts
import gcs_utils
from resources import fields_path

LOGGER = logging.getLogger(__name__)


def write_to_result_table(project, dataset, site, match_values):
    """
    Append items in match_values to the table generated from site name.

    Attempts to limit the insert query to less than 1MB.

    :param site:  string identifier for the hpo site.
    :param match_values:  dictionary of person_ids and match values for a field
    :param project:  the project BigQuery project name
    :param dataset:  name of the dataset containing the table to append to

    :return: query results value
    :raises:  oauth2client.client.HttpAccessTokenRefreshError,
              googleapiclient.errors.HttpError
    """
    if not match_values:
        LOGGER.info(f"No values to insert for site: {site}")
        return None

    result_table = site + consts.VALIDATION_TABLE_SUFFIX
    bucket = gcs_utils.get_drc_bucket()
    path = dataset + '/intermediate_results/' + site + '.csv'

    field_list = [consts.PERSON_ID_FIELD]
    field_list.extend(consts.VALIDATION_FIELDS)
    field_list.append(consts.ALGORITHM_FIELD)

    results = StringIO()
    field_list_str = ','.join(field_list) + '\n'
    results.write(field_list_str)

    LOGGER.info(f"Generating csv values to write to storage for site: {site}")

    for person_key, person_values in match_values.items():
        str_list = [str(person_key)]
        for field in consts.VALIDATION_FIELDS:
            value = str(person_values.get(field, consts.MISSING))
            str_list.append(value)

        str_list.append(consts.YES)
        val_str = ','.join(str_list)
        results.write(val_str + '\n')

    LOGGER.info(f"Writing csv file to cloud storage for site: {site}")

    # write results
    results.seek(0)
    gcs_utils.upload_object(bucket, path, results)
    results.close()

    LOGGER.info(
        f"Wrote {len(match_values)} items to cloud storage for site: {site}")

    # wait on results to be written

    table_name = 'identity_match'

    LOGGER.info(
        f"Beginning load of identity match values from csv into BigQuery "
        "for site: {site}")
    try:
        # load csv file into bigquery
        results = bq_utils.load_csv(table_name,
                                    'gs://' + bucket + '/' + path,
                                    project,
                                    dataset,
                                    result_table,
                                    write_disposition=consts.WRITE_TRUNCATE)

        # ensure the load job finishes
        query_job_id = results['jobReference']['jobId']
        incomplete_jobs = bq_utils.wait_on_jobs([query_job_id])
        if incomplete_jobs != []:
            raise bq_utils.BigQueryJobWaitError(incomplete_jobs)

    except (oauth2client.client.HttpAccessTokenRefreshError,
            googleapiclient.errors.HttpError):
        LOGGER.exception(
            f"Encountered an exception when loading records from csv for site: {site}"
        )
        raise

    LOGGER.info(f"Loaded match values for site: {site}")

    return results


def _get_match_rank(match_list):
    if consts.MISMATCH in match_list:
        return consts.MISMATCH
    elif consts.MISSING in match_list:
        return consts.MISSING

    return consts.MATCH


def get_address_match(address_values):
    """
    Return a value indicating whether an address matches or was even validated.

    The address pieces are individually validated, but this allows the report
    to show the address as a single unit that is matched, not matched, or
    missing.  The values returned are identified by the constants file.
    If (address_line_1 OR address_line_2 OR (address_line_1 AND address_line_2))
    AND (city AND state AND ZIP) match, the address matches.
    If (address_line_1 AND address_line_2 AND (address_line_1 AND address_line_2))
    AND (city OR state OR ZIP) do not match, the address does not match.
    If (address_line_1 AND address_line_2 AND (address_line_1 AND address_line_2))
    are missing OR (city OR state OR ZIP) are missing, the address is missing.

    :param address_values:  a list of address value matches in their commonly
        expected order:  [address line 1, address line 2, city, state, zip]

    :return:  A constant string value indicating the address should be considered
        a match, not match, or not validated.
    """
    address_one = address_values[0]
    address_two = address_values[1]
    city = address_values[2]
    state = address_values[3]
    zip_code = address_values[4]

    streets_match = _get_match_rank([address_one, address_two])

    areas_match = _get_match_rank([city, state, zip_code])

    return _get_match_rank([streets_match, areas_match])


def create_site_validation_report(project, dataset, hpo_list, bucket, filename):
    """
    Write the validation csv from the site validation table.

    :param project:  The project name
    :param dataset:  The dataset where the validtion table exists
    :param hpo_list:  A list of hpo strings to create a csv for
    :param bucket:  The bucket to write the csv to.
    :param filename:  The file name to give the csv report.
    """
    if not isinstance(hpo_list, list):
        hpo_list = [hpo_list]

    fields = [
        consts.PERSON_ID_FIELD, consts.FIRST_NAME_FIELD, consts.LAST_NAME_FIELD,
        consts.BIRTH_DATE_FIELD, consts.SEX_FIELD, consts.ADDRESS_MATCH_FIELD,
        consts.PHONE_NUMBER_FIELD, consts.EMAIL_FIELD, consts.ALGORITHM_FIELD
    ]

    fields_str = ','.join(fields) + '\n'

    # sets up a file stream to write to the bucket
    report_file = StringIO()
    report_file.write(fields_str)

    # write to the report file
    read_errors = 0
    for site in hpo_list:
        result_table = site + consts.VALIDATION_TABLE_SUFFIX
        query_string = consts.VALIDATION_RESULTS_VALUES.format(
            project=project, dataset=dataset, table=result_table)

        try:
            results = bq_utils.query(query_string, batch=True)
        except (oauth2client.client.HttpAccessTokenRefreshError,
                googleapiclient.errors.HttpError):
            LOGGER.exception(
                "Encountered an exception when selecting site records")
            report_file.write("Unable to report id validation match records "
                              "for site:\t{}.\n".format(site))
            read_errors += 1
            continue

        row_results = bq_utils.large_response_to_rowlist(results)
        for item in row_results:
            address_values = [
                item.get(consts.ADDRESS_ONE_FIELD),
                item.get(consts.ADDRESS_TWO_FIELD),
                item.get(consts.CITY_FIELD),
                item.get(consts.STATE_FIELD),
                item.get(consts.ZIP_CODE_FIELD)
            ]
            values = [
                str(item.get(consts.PERSON_ID_FIELD)),
                item.get(consts.FIRST_NAME_FIELD),
                item.get(consts.LAST_NAME_FIELD),
                item.get(consts.BIRTH_DATE_FIELD),
                item.get(consts.SEX_FIELD),
                get_address_match(address_values),
                item.get(consts.PHONE_NUMBER_FIELD),
                item.get(consts.EMAIL_FIELD),
                item.get(consts.ALGORITHM_FIELD)
            ]
            values_str = ','.join(values) + '\n'
            report_file.write(values_str)

    # reset the stream and write to the bucket
    report_file.seek(0)
    report_result = gcs_utils.upload_object(bucket, filename, report_file)
    report_file.close()

    LOGGER.info(f"Wrote validation report csv: {bucket}{filename}")
    return report_result, read_errors
