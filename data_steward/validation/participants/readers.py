#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
A module to perform reading operations for validation data.
"""
# Python imports
from datetime import datetime, timezone
import logging

# Third party imports

# Project imports
import bq_utils
import resources as rc
from constants.validation.participants import readers as consts

LOGGER = logging.getLogger(__name__)


def _get_field_type(table_name, column_name):
    """
    Get the defined field type for the table.

    :param table_name:  name of the table containing the field
    :param column_name: name of the column to get the type of

    :return:  The defined type for the field in the table.
    """
    field_type = None
    for field in rc.fields_for(table_name):
        if field.get('name', '') == column_name:
            field_type = field.get('type')

    return field_type


def _get_string(value, field_type=None):
    """
    Get the string value of a field based on its return value type and defiend type.

    :param value: The value to return as a string.
    :param field_type: The defined field type.

    :return: The resulting string value or None.
    """

    time_types = [consts.DATE_TYPE, consts.DATETIME_TYPE, consts.TIMESTAMP_TYPE]
    result = None
    if isinstance(value, bytes):
        result = value.decode('utf-8', 'ignore')
    elif value is None:
        pass
    elif isinstance(value, float) and field_type.lower() in time_types:
        result = datetime.fromtimestamp(value, tz=timezone.utc)
        result = result.strftime(consts.DATE_FORMAT)
    else:
        result = str(value)

    return result


def create_match_values_table(project, rdr_dataset, destination_dataset):
    """
    Get the desired matching values from the combined observation table.

    This retrieves all possible matching values from the observation table
    used in participant matching.  Returned query data is limited to the
    person_id, observation_concept_id, and the value_as_string fields.

    :param project:  The project name to query
    :param rdr_dataset:  The rdr dataset name to query
    :param destination_dataset:  The dataset to write the result table to

    :return: The name of the interim table
    """
    query_string = consts.ALL_PPI_OBSERVATION_VALUES.format(
        project=project,
        dataset=rdr_dataset,
        table=consts.OBSERVATION_TABLE,
        pii_list=','.join(consts.PII_CODES_LIST))

    LOGGER.info("Participant validation ran the query\n%s", query_string)
    results = bq_utils.query(query_string,
                             destination_dataset_id=destination_dataset,
                             destination_table_id=consts.ID_MATCH_TABLE,
                             write_disposition='WRITE_TRUNCATE',
                             batch=True)

    query_job_id = results['jobReference']['jobId']
    incomplete_jobs = bq_utils.wait_on_jobs([query_job_id])
    if incomplete_jobs != []:
        raise bq_utils.BigQueryJobWaitError(incomplete_jobs)

    return consts.ID_MATCH_TABLE


def get_ehr_person_values(project, dataset, table_name, column_name):
    """
    Get the desired matching values from the combined observation table.

    This retrieves all possible matching values from the observation table
    used in participant matching.  Returned query data is limited to the
    person_id, observation_concept_id, and the value_as_string fields.

    :param project: The name of the project to query for ehr values
    :param dataset:  The name of the dataset to query for ehr values
    :param table_name:  The name of the table to query for ehr values
    :param column_name:  String name of the field to return from the table

    :return:  A dictionary with observation_source_concept_id as the key.
        The value is a dictionary of person_ids with the associated value
        of the concept_id.
        For example:
        {person_id_1:  "email_address", person_id_2: "email_address"}
    :raises:  oauth2client.client.HttpAccessTokenRefreshError,
              googleapiclient.errors.HttpError
    """
    query_string = consts.EHR_PERSON_VALUES.format(
        project=project,
        dataset=dataset,
        table=table_name,
        field=column_name,
    )

    LOGGER.info("Participant validation ran the query\n%s", query_string)
    results = bq_utils.query(query_string)
    row_results = bq_utils.large_response_to_rowlist(results)

    field_type = _get_field_type(table_name, column_name)

    result_dict = {}
    for item in row_results:
        person_id = item.get(consts.PERSON_ID_FIELD)
        value = item.get(column_name)
        value = _get_string(value, field_type)

        exists = result_dict.get(person_id)
        if exists is None:
            result_dict[person_id] = value
        else:
            pass

    return result_dict


def get_rdr_match_values(project, dataset, table_name, concept_id):
    """
    Get the desired matching values from the combined observation table.

    This retrieves all possible matching values from the observation table
    used in participant matching.  Returned query data is limited to the
    person_id, observation_concept_id, and the value_as_string fields.

    :param project: The name of the project to query for rdr values
    :param dataset:  The name of the dataset to query for rdr values.  In this
        module, it is likely the validation dataset
    :param table_name:  The name of the table to query for rdr values.  In
        this module, it is likely the validation dataset
    :param concept_id: the id of the concept to verify from the RDR data

    :return:  A dictionary with observation_source_concept_id as the key.
        The value is a dictionary of person_ids with the associated value
        of the concept_id.
        For example:
        {person_id_1:  "email_address", person_id_2: "email_address"}
        {person_id_1: "first_name", person_id_2: "first_name"}
        {person_id_1: "last_name", person_id_2: "last_name"}
    :raises:  oauth2client.client.HttpAccessTokenRefreshError,
              googleapiclient.errors.HttpError
    """
    query_string = consts.PPI_OBSERVATION_VALUES.format(project=project,
                                                        dataset=dataset,
                                                        table=table_name,
                                                        field_value=concept_id)

    LOGGER.info("Participant validation ran the query\n%s", query_string)
    results = bq_utils.query(query_string)
    row_results = bq_utils.large_response_to_rowlist(results)

    field_type = _get_field_type(table_name, 'observation_source_concept_id')

    result_dict = {}
    for item in row_results:
        person_id = item.get(consts.PERSON_ID_FIELD)
        value = item.get(consts.STRING_VALUE_FIELD)
        value = _get_string(value, field_type)

        exists = result_dict.get(person_id)
        if exists is None:
            result_dict[person_id] = value
        else:
            pass

    return result_dict


def get_hpo_site_names():
    """
    Return a list of hpo site ids.

    :return:  A list of string hpo site ids
    """
    hpo_ids = []
    for site in bq_utils.get_hpo_info():
        hpo_ids.append(site[consts.HPO_ID])
    return hpo_ids


def get_pii_values(project, pii_dataset, hpo, table, field):
    """
    Get values from the site's PII table.

    :param project: The name of the project to query for pii values
    :param pii_dataset:  The name of the dataset to query for pii values.
    :param hpo:  hpo string to use when identifying table names for lookup.
    :param table:  The name of the table suffix to query for pii values.
    :param field:  The field name to look up values for

    :return:  A list of tuples with the first tuple element as the person_id
    and the second tuple element as the phone number.
    [(1, '5558675309'), (48, '5558004600'), (99, '5551002000')]
    :raises:  oauth2client.client.HttpAccessTokenRefreshError,
              googleapiclient.errors.HttpError
    """
    query_string = consts.PII_VALUES.format(project=project,
                                            dataset=pii_dataset,
                                            hpo_site_str=hpo,
                                            field=field,
                                            table_suffix=table)

    LOGGER.info("Participant validation ran the query\n%s", query_string)

    results = bq_utils.query(query_string)
    row_results = bq_utils.large_response_to_rowlist(results)

    field_type = _get_field_type(table, field)

    result_list = []
    for item in row_results:
        person_id = item.get(consts.PERSON_ID_FIELD)
        value = item.get(field)

        value = _get_string(value, field_type)
        result_list.append((person_id, value))

    return result_list


def get_location_pii(project, rdr_dataset, pii_dataset, hpo, table, field):
    """
    Get the actual value for a location field.

    ;param project:  The project name
    :param rdr_dataset:  The dataset to get actual location info from
    :param pii_dataset:  The dataset with a location_id.  location_id comes from
        a pii_address table.
    :param hpo:  site identifier used to prefix table names
    :param table:  table name to retrieve pii values for
    :param field:  The actual field to retrieve a value for:  either address_one,
        address_two, city, state, or zip.

    :return:  a list of [(person_id, value)] tuples.
    :raises:  oauth2client.client.HttpAccessTokenRefreshError,
              googleapiclient.errors.HttpError
    """
    location_ids = get_pii_values(project, pii_dataset, hpo, table,
                                  consts.LOCATION_ID_FIELD)

    location_id_list = []
    location_id_dict = {}
    for location_id in location_ids:
        location_id_list.append(location_id[1])
        location_id_dict[int(location_id[1])] = location_id[0]

    if not location_id_list:
        LOGGER.info("No location information found for site:\t%s", hpo)
        return []

    location_id_str = ', '.join(location_id_list)
    query_string = consts.PII_LOCATION_VALUES.format(project=project,
                                                     dataset=rdr_dataset,
                                                     field=field,
                                                     id_list=location_id_str)

    LOGGER.info("Participant validation ran the query\n%s", query_string)

    results = bq_utils.query(query_string)
    row_results = bq_utils.large_response_to_rowlist(results)

    field_type = _get_field_type(table, field)

    result_list = []
    for item in row_results:
        location_id = item.get(consts.LOCATION_ID_FIELD)
        value = item.get(field)

        value = _get_string(value, field_type)

        person_id = location_id_dict.get(location_id, '')
        result_list.append((person_id, value))

    return result_list
