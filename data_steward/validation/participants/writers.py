#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
A module to write participant identity matching table data.
"""
# Python imports
import logging
import StringIO

# Third party imports
import oauth2client

# Project imports
import bq_utils
import constants.validation.participants.identity_match as consts
import gcs_utils

LOGGER = logging.getLogger(__name__)


def append_to_result_table(
        site,
        match_values,
        project,
        dataset,
        field_name
    ):
    """
    Append items in match_values to the table generated from site name.

    :param site:  string identifier for the hpo site.
    :param match_values:  dictionary of person_ids and match values for a field
    :param project:  the project BigQuery project name
    :param dataset:  name of the dataset containing the table to append to
    :param field_name:  name of the field to insert values into

    :return: query results value
    """
    result_table = site + consts.VALIDATION_TABLE_SUFFIX

    # create initial values list for insertion
    values_list = []
    for key, value in match_values.iteritems():
        value_str = '(' + str(key) + ', \'' + value + '\', \'' + consts.YES + '\')'
        values_list.append(value_str)

    values_str = ', '.join(values_list)

    query = consts.INSERT_MATCH_VALUES.format(
        project=project,
        dataset=dataset,
        table=result_table,
        field=field_name,
        values=values_str,
        id_field=consts.PERSON_ID_FIELD,
        algorithm_field=consts.ALGORITHM_FIELD
    )

    LOGGER.debug("Inserting match values for site: %s\t\tfield: %s", site, field_name)

    try:
        results = bq_utils.query(query, batch=True)
    except oauth2client.client.HttpAccessTokenRefreshError:
        LOGGER.exception("Encountered an excpetion when inserting records")
        raise

    LOGGER.info("Inserted match values for site:  %s\t\tfield:  %s", site, field_name)

    return results

def remove_sparse_records(project, dataset, site):
    """
    Remove sparsely populated records.

    The sparse records were perviously merged into a unified record.  This
    removes the sparse data records.

    :param project: The project string identifier
    :param dataset: The validation dataset identifier
    :param site: The site to delete sparse validation results for
    """
    result_table = site + consts.VALIDATION_TABLE_SUFFIX

    LOGGER.debug("Removing lingering sparse records from %s.%s.%s",
                 project,
                 dataset,
                 result_table
                )

    query = consts.MERGE_DELETE_SPARSE_RECORDS.format(
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
        field_eleven=consts.VALIDATION_FIELDS[10]
    )

    try:
        results = bq_utils.query(query, batch=True)
    except oauth2client.client.HttpAccessTokenRefreshError:
        LOGGER.exception("Encountered an excpetion when removing sparse records")
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
        except oauth2client.client.HttpAccessTokenRefreshError:
            LOGGER.exception("Encountered an excpetion when merging records")
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
    """
    result_table = site + consts.VALIDATION_TABLE_SUFFIX

    LOGGER.debug("Setting null fields to %s in %s.%s.%s",
                 consts.MISSING,
                 project,
                 dataset,
                 result_table
                )

    query = consts.MERGE_SET_MISSING_FIELDS.format(
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
        value=consts.MISSING
    )

    try:
        results = bq_utils.query(query, batch=True)
    except oauth2client.client.HttpAccessTokenRefreshError:
        LOGGER.exception("Encountered an excpetion when filling null records")
        raise

    LOGGER.info(
        "Set null fields to %s in %s.%s.%s",
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
    fields = [consts.PERSON_ID_FIELD, consts.FIRST_NAME_FIELD,
              consts.LAST_NAME_FIELD, consts.BIRTH_DATE_FIELD,
              consts.ADDRESS_MATCH_FIELD, consts.PHONE_NUMBER_FIELD, consts.EMAIL_FIELD,
              consts.ALGORITHM_FIELD
             ]

    fields_str = ','.join(fields) + '\n'

    # sets up a file stream to write to the bucket
    report_file = StringIO.StringIO()
    report_file.write(fields_str)

    # write to the report file
    for site in hpo_list:
        result_table = site + consts.VALIDATION_TABLE_SUFFIX
        query_string = consts.VALIDATION_RESULTS_VALUES.format(
            project=project,
            dataset=dataset,
            table=result_table
        )

        try:
            results = bq_utils.query(query_string, batch=True)
        except oauth2client.client.HttpAccessTokenRefreshError:
            LOGGER.exception("Encountered an excpetion when selecting site records")

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
    return report_result
