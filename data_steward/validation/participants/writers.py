#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
A module to write participant identity matching table data.
"""
# Python imports
from copy import copy
import logging
import os
import StringIO

# Third party imports
import googleapiclient
import oauth2client

# Project imports
import bq_utils
import constants.validation.participants.writers as consts
import gcs_utils
from resources import fields_path

LOGGER = logging.getLogger(__name__)


def append_to_result_table(
        site,
        match_values,
        project,
        dataset,
        field_name,
        field_list
    ):
    """
    Append items in match_values to the table generated from site name.

    Attempts to limit the insert query to less than 1MB.

    :param site:  string identifier for the hpo site.
    :param match_values:  dictionary of person_ids and match values for a field
    :param project:  the project BigQuery project name
    :param dataset:  name of the dataset containing the table to append to
    :param field_name:  name of the field to insert values into
    :param field_list: ordered list of expected fields

    :return: query results value
    :raises:  oauth2client.client.HttpAccessTokenRefreshError,
              googleapiclient.errors.HttpError
    """
    if not match_values:
        LOGGER.info("No values to insert for site: %s\t\tfield: %s", site, field_name)
        return None

    result_table = site + consts.VALIDATION_TABLE_SUFFIX
    bucket = gcs_utils.get_drc_bucket()
    path = dataset + '/intermediate_results/' + site + '_' + field_name + '.csv'

    field_index = None
    id_index = None
    alg_index = None
    for index, field in enumerate(field_list):
        if field == field_name:
            field_index = index
        elif field == consts.PERSON_ID_FIELD:
            id_index = index
        elif field == consts.ALGORITHM_FIELD:
            alg_index = index

    if field_index is None or id_index is None or alg_index is None:
        raise RuntimeError("The field %s values were not written for site %s."
                           "person_id index: %d\nalg_index: %d\nfield_index: %d",
                           field_name, site, id_index, alg_index, field_index)

    results = StringIO.StringIO()
    field_list_str = ','.join(field_list) + '\n'
    results.write(field_list_str)

    LOGGER.debug("Generating csv values to write to storage for field: %s\t\tsite: %s",
                 field_name, site)

    str_list = [''] * len(field_list)
    for dict_index, dict_value in enumerate(match_values.iteritems()):
        key = str(dict_value[0])
        value = str(dict_value[1])

        alpha = copy(str_list)
        alpha[field_index] = value
        alpha[id_index] = key
        alpha[alg_index] = consts.YES
        val_str = ','.join(alpha)
        results.write(val_str + '\n')

    LOGGER.info("Writing csv file to cloud storage for field: %s\t\tsite: %s",
                field_name, site)

    # write results
    results.seek(0)
    response = gcs_utils.upload_object(bucket, path, results)
    results.close()

    LOGGER.info("Wrote %d items to cloud storage for site: %s",
                len(match_values), site)

    # wait on results to be written

    schema_path = os.path.join(fields_path, 'identity_match.json')

    LOGGER.info("Beginning load of identity match values from csv into BigQuery "
                "for site: %s\t\tfield: %s",
                site, field_name)
    try:
        # load csv file into bigquery
        results = bq_utils.load_csv(schema_path,
                                    'gs://' + bucket + '/' + path,
                                    project,
                                    dataset,
                                    result_table,
                                    write_disposition=consts.WRITE_APPEND)

        # ensure the load job finishes
        query_job_id = results['jobReference']['jobId']
        incomplete_jobs = bq_utils.wait_on_jobs([query_job_id])
        if incomplete_jobs != []:
            raise bq_utils.BigQueryJobWaitError(incomplete_jobs)

    except (oauth2client.client.HttpAccessTokenRefreshError,
            googleapiclient.errors.HttpError):
        LOGGER.exception(
            "Encountered an exception when loading records from csv for site: %s", site
        )
        raise

    LOGGER.info("Loaded match values for site: %s\t\tfield: %s", site, field_name)

    return results


def remove_sparse_records(project, dataset, site):
    """
    Remove sparsely populated records.

    The sparse records were perviously merged into a unified record.  This
    removes the sparse data records.

    :param project: The project string identifier
    :param dataset: The validation dataset identifier
    :param site: The site to delete sparse validation results for

    :raises:  oauth2client.client.HttpAccessTokenRefreshError,
              googleapiclient.errors.HttpError
    """
    result_table = site + consts.VALIDATION_TABLE_SUFFIX

    LOGGER.debug("Removing lingering sparse records from %s.%s.%s",
                 project,
                 dataset,
                 result_table
                )

    query = consts.SELECT_FULL_RECORDS.format(
        project=project,
        dataset=dataset,
        table=result_table,
        field_one=consts.VALIDATION_FIELDS[0],
        field_two=consts.VALIDATION_FIELDS[1],
        field_three=consts.VALIDATION_FIELDS[2],
        field_four=consts.VALIDATION_FIELDS[3],
        field_five=consts.VALIDATION_FIELDS[4],
        field_six=consts.VALIDATION_FIELDS[5],
        field_seven=consts.VALIDATION_FIELDS[6],
        field_eight=consts.VALIDATION_FIELDS[7],
        field_nine=consts.VALIDATION_FIELDS[8],
        field_ten=consts.VALIDATION_FIELDS[9],
        field_eleven=consts.VALIDATION_FIELDS[10],
        field_twelve=consts.VALIDATION_FIELDS[11]
    )

    try:
        results = bq_utils.query(
            query,
            destination_table_id=result_table,
            destination_dataset_id=dataset,
            write_disposition=consts.WRITE_TRUNCATE,
            batch=True
        )
    except (oauth2client.client.HttpAccessTokenRefreshError,
            googleapiclient.errors.HttpError):
        LOGGER.exception(
            "Encountered an exception when removing sparse records for:\t%s", site
        )
        raise

    LOGGER.info("Removed sparse records from:  %s.%s.%s", project, dataset, result_table)

    return results


def merge_fields_into_single_record(project, dataset, site):
    """
    Merge records with a single populated field into a fully populated record.

    The records are inserted into the table via insert methods for each validated
    field type.  This means the rows are primarily comprised of null fields.
    This merges the data from the primarily null fields into a single populated
    record.

    :param project: The project string identifier
    :param dataset: The validation dataset identifier
    :param site: The site to merge validation results for

    :raises:  oauth2client.client.HttpAccessTokenRefreshError,
              googleapiclient.errors.HttpError
    """
    result_table = site + consts.VALIDATION_TABLE_SUFFIX

    LOGGER.debug("Unifying sparse records for %s.%s.%s",
                 project,
                 dataset,
                 result_table
                )

    for validation_field in consts.VALIDATION_FIELDS:
        query = consts.MERGE_UNIFY_SITE_RECORDS.format(
            project=project,
            dataset=dataset,
            table=result_table,
            field=validation_field
        )

        try:
            results = bq_utils.query(query, batch=True)
        except (oauth2client.client.HttpAccessTokenRefreshError,
                googleapiclient.errors.HttpError):
            LOGGER.exception(
                "Encountered an exception when merging records for:\t%s", site
            )
            raise

    LOGGER.info("Unified sparse records for:  %s.%s.%s", project, dataset, result_table)

    return results


def change_nulls_to_missing_value(project, dataset, site):
    """
    Change existing null values to indicate the match item was/is missing.

    After merging records and deleting sparse records, it is possible for null
    values to exist if the item did not exist in either the concept table or
    the pii table.  To eliminate this possibility, all null values will be
    updated to the constant missing value.

    :param project: The project string identifier
    :param dataset: The validation dataset identifier
    :param site: The site to delete sparse validation results for

    :raises:  oauth2client.client.HttpAccessTokenRefreshError,
              googleapiclient.errors.HttpError
    """
    result_table = site + consts.VALIDATION_TABLE_SUFFIX

    LOGGER.debug("Setting null fields to %s in %s.%s.%s",
                 consts.MISSING,
                 project,
                 dataset,
                 result_table
                )

    query = consts.SELECT_SET_MISSING_VALUE.format(
        project=project,
        dataset=dataset,
        table=result_table,
        field_one=consts.VALIDATION_FIELDS[0],
        field_two=consts.VALIDATION_FIELDS[1],
        field_three=consts.VALIDATION_FIELDS[2],
        field_four=consts.VALIDATION_FIELDS[3],
        field_five=consts.VALIDATION_FIELDS[4],
        field_six=consts.VALIDATION_FIELDS[5],
        field_seven=consts.VALIDATION_FIELDS[6],
        field_eight=consts.VALIDATION_FIELDS[7],
        field_nine=consts.VALIDATION_FIELDS[8],
        field_ten=consts.VALIDATION_FIELDS[9],
        field_eleven=consts.VALIDATION_FIELDS[10],
        field_twelve=consts.VALIDATION_FIELDS[11],
        value=consts.MISSING,
        person_id=consts.PERSON_ID_FIELD,
        algorithm=consts.ALGORITHM_FIELD
    )

    try:
        results = bq_utils.query(
            query,
            destination_table_id=result_table,
            destination_dataset_id=dataset,
            write_disposition=consts.WRITE_TRUNCATE,
            batch=True
        )
    except (oauth2client.client.HttpAccessTokenRefreshError,
            googleapiclient.errors.HttpError):
        LOGGER.exception(
            "Encountered an exception when filling null records for:\t%s", site
        )
        raise

    LOGGER.info(
        "Successfully set null fields to %s in %s.%s.%s",
        consts.MISSING,
        project,
        dataset,
        result_table
    )

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

    fields = [consts.PERSON_ID_FIELD, consts.FIRST_NAME_FIELD,
              consts.LAST_NAME_FIELD, consts.BIRTH_DATE_FIELD, consts.SEX_FIELD,
              consts.ADDRESS_MATCH_FIELD, consts.PHONE_NUMBER_FIELD,
              consts.EMAIL_FIELD, consts.ALGORITHM_FIELD
             ]

    fields_str = ','.join(fields) + '\n'

    # sets up a file stream to write to the bucket
    report_file = StringIO.StringIO()
    report_file.write(fields_str)

    # write to the report file
    read_errors = 0
    for site in hpo_list:
        result_table = site + consts.VALIDATION_TABLE_SUFFIX
        query_string = consts.VALIDATION_RESULTS_VALUES.format(
            project=project,
            dataset=dataset,
            table=result_table
        )

        try:
            results = bq_utils.query(query_string, batch=True)
        except (oauth2client.client.HttpAccessTokenRefreshError,
                googleapiclient.errors.HttpError):
            LOGGER.exception("Encountered an exception when selecting site records")
            report_file.write("Unable to report id validation match records "
                              "for site:\t%s.\n", site)
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

    LOGGER.debug("Wrote validation report csv:  %s", bucket + filename)
    return report_result, read_errors
