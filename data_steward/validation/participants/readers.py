#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
A module to perform reading operations for validation data.
"""
# Python imports
import logging

# Third party imports

# Project imports
import bq_utils
import resources as rc
import constants.validation.participants.identity_match as consts

LOGGER = logging.getLogger(__name__)


def _get_utf8_string(value):
    result = None
    try:
        result = value.encode('utf-8', 'ignore')
    except AttributeError:
        if value is None:
            LOGGER.debug("Value '%s' can not be utf-8 encoded", value)
        elif isinstance(value, int):
            result = str(value)

    return result


def create_match_values_table(project, rdr_dataset, destination_dataset):
    """
    Get the desired matching values from the combined observation table.

    This retrieves all possible matching values from the observation table
    used in participant matching.  Returned query data is limited to the
    person_id, observation_concept_id, and the value_as_string fields.

    :param date_string: a string formatted date as YYYYMMDD that will be used
        to identify which dataset to use as a lookup

    :return: The name of the interim table
    """
    query_string = consts.ALL_PPI_OBSERVATION_VALUES.format(
        project=project,
        dataset=rdr_dataset,
        table=consts.OBSERVATION_TABLE
    )

    LOGGER.debug("Participant validation ran the query\n%s", query_string)
    results = bq_utils.query(
        query_string,
        destination_dataset_id=destination_dataset,
        destination_table_id=consts.ID_MATCH_TABLE,
        write_disposition='WRITE_TRUNCATE'
    )

    query_job_id = results['jobReference']['jobId']
    incomplete_jobs = bq_utils.wait_on_jobs([query_job_id])
    if incomplete_jobs != []:
        raise bq_utils.BigQueryJobWaitError(incomplete_jobs)

    return consts.ID_MATCH_TABLE


def get_ehr_match_values(project, dataset, table_name, concept_id, id_list):
    """
    Get the desired matching values from the combined observation table.

    This retrieves all possible matching values from the observation table
    used in participant matching.  Returned query data is limited to the
    person_id, observation_concept_id, and the value_as_string fields.

    :param concept_id: the id of the concept to verify from the RDR data

    :return:  A dictionary with observation_source_concept_id as the key.
        The value is a dictionary of person_ids with the associated value
        of the concept_id.
        For example:
        {person_id_1:  "email_address", person_id_2: "email_address"}
        {person_id_1: "first_name", person_id_2: "first_name"}
        {person_id_1: "middle_name", }
        {person_id_1: "last_name", person_id_2: "last_name"}
    """
    query_string = consts.EHR_OBSERVATION_VALUES.format(
        project=project,
        data_set=dataset,
        table=table_name,
        field_value=concept_id,
        person_id_csv=id_list
    )

    LOGGER.debug("Participant validation ran the query\n%s", query_string)
    results = bq_utils.query(query_string)
    row_results = bq_utils.large_response_to_rowlist(results)

    result_dict = {}
    for item in row_results:
        person_id = item.get(consts.PERSON_ID_FIELD)
        value = item.get(consts.STRING_VALUE_FIELD)
        value = _get_utf8_string(value)

        exists = result_dict.get(person_id)
        if exists is None:
            result_dict[person_id] = value
        else:
            if exists == value:
                pass
            else:
                LOGGER.error("Trying to reset value for person_id\t%s.")

    return result_dict


def get_rdr_match_values(project, dataset, table_name, concept_id):
    """
    Get the desired matching values from the combined observation table.

    This retrieves all possible matching values from the observation table
    used in participant matching.  Returned query data is limited to the
    person_id, observation_concept_id, and the value_as_string fields.

    :param concept_id: the id of the concept to verify from the RDR data

    :return:  A dictionary with observation_source_concept_id as the key.
        The value is a dictionary of person_ids with the associated value
        of the concept_id.
        For example:
        {person_id_1:  "email_address", person_id_2: "email_address"}
        {person_id_1: "first_name", person_id_2: "first_name"}
        {person_id_1: "middle_name", }
        {person_id_1: "last_name", person_id_2: "last_name"}
    """
    query_string = consts.PPI_OBSERVATION_VALUES.format(
        project=project,
        dataset=dataset,
        table=table_name,
        field_value=concept_id
    )

    LOGGER.debug("Participant validation ran the query\n%s", query_string)
    results = bq_utils.query(query_string)
    row_results = bq_utils.large_response_to_rowlist(results)

    result_dict = {}
    for item in row_results:
        person_id = item.get(consts.PERSON_ID_FIELD)
        value = item.get(consts.STRING_VALUE_FIELD)
        value = _get_utf8_string(value)

        exists = result_dict.get(person_id)
        if exists is None:
            result_dict[person_id] = value
        else:
            if exists == value:
                pass
            else:
                LOGGER.error("Trying to reset value for person_id\t%s.")

    return result_dict


def get_hpo_site_names():
    """
    Return a list of hpo site ids.

    :return:  A list of string hpo site ids
    """
    hpo_ids = []
    for site in rc.hpo_csv():
        hpo_ids.append(site[consts.HPO_ID])
    return hpo_ids


def get_pii_values(project, pii_dataset, hpo, table, field):
    """
    Get values from the site's PII table.

    :param hpo:  hpo string to use when identifying table names for lookup.

    :return:  A list of tuples with the first tuple element as the person_id
    and the second tuple element as the phone number.
    [(1, '5558675309'), (48, '5558004600'), (99, '5551002000')]
    """
    query_string = consts.PII_VALUES.format(
        project=project,
        dataset=pii_dataset,
        hpo_site_str=hpo,
        field=field,
        table_suffix=table
    )

    LOGGER.debug("Participant validation ran the query\n%s", query_string)

    results = bq_utils.query(query_string)
    row_results = bq_utils.large_response_to_rowlist(results)

    result_list = []
    for item in row_results:
        person_id = item.get(consts.PERSON_ID_FIELD)
        value = item.get(field)

        value = _get_utf8_string(value)
        result_list.append((person_id, value))

    return result_list

def get_location_pii(project, rdr_dataset, pii_dataset, hpo, table, field):
    """
    Get the actual value for a location field.

    ;param project:  The project name
    :param rdr_dataset:  The dataset to get actual location info from
    :param pii_dataset:  The dataset with a location_id.  location_id comes from
        a pii_address table.
    :param field:  The actual field to retrieve a value for:  either address_one,
        address_two, city, state, or zip.

    :return:  a list of (person_id, value) tuples.
    """
    location_ids = get_pii_values(
        project,
        pii_dataset,
        hpo,
        table,
        consts.LOCATION_ID_FIELD
    )

    location_id_list = []
    location_id_dict = {}
    for location_id in location_ids:
        location_id_list.append(location_id[1])
        location_id_dict[int(location_id[1])] = location_id[0]


    location_id_str = ', '.join(location_id_list)
    query_string = consts.PII_LOCATION_VALUES.format(
        project=project,
        data_set=rdr_dataset,
        field=field,
        id_list=location_id_str
    )

    LOGGER.debug("Participant validation ran the query\n%s", query_string)

    results = bq_utils.query(query_string)
    row_results = bq_utils.large_response_to_rowlist(results)

    result_list = []
    for item in row_results:
        location_id = item.get(consts.LOCATION_ID_FIELD)
        value = item.get(field)

        value = _get_utf8_string(value)

        person_id = location_id_dict.get(location_id, '')
        result_list.append((person_id, value))

    return result_list
