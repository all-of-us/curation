#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
A module to perform initial participant identity matching based on PII data.

Compares site PII data to values from the RDR, looking to identify discrepancies.
"""
# Python imports
import json
import logging

# Third party imports
from dateutil.parser import parse

# Project imports
import bq_utils
import resources as rc
import constants.validation.participants.identity_match as consts
import constants.bq_utils as bq_consts

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


def _set_observation_match_values_table(project, rdr_dataset, destination_dataset):
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


def _get_ehr_observation_match_values(project, dataset, table_name, concept_id, id_list):
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


def _get_ppi_observation_match_values(project, dataset, table_name, concept_id):
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


def _get_hpo_site_names():
    hpo_ids = []
    for site in rc.hpo_csv():
        hpo_ids.append(site[consts.HPO_ID])
    return hpo_ids


def _get_pii_values(project, pii_dataset, hpo, table, field):
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

def _get_location_pii(project, rdr_dataset, pii_dataset, hpo, table, field):
    location_ids = _get_pii_values(
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


def _clean_street(street):
    """
    Helper function to return normalized street addresses.

    :param street:  string to clean.
    :return:  a normalized alphanumeric string with all alphabetic characters
        lower cased, leading and trailing white space stripped, abbreviations
        expanded, and punctuation removed or an empty string.
    """
    if street is None:
        return ''
    elif not isinstance(street, str):
        street = str(street)

    clean_street = ''
    street = street.lower()
    street = street.replace('#', ' # ')  # ensure hash is found by itself
    # replace all punctuation with a space, excpet for # which is sometimes used
    # for number
    for char in street:
        if char.isalnum() or char == '#':
            clean_street += char
        else:
            clean_street += ' '

    # for each part of the address, see if it exists in the list of known
    # abbreviations.  if so, expand the abbreviation
    # TODO ensure 50A and 50 A are recognized as the same
    for part in clean_street.split():
        expansion = consts.ADDRESS_ABBREVIATIONS.get(part)
        if expansion:
            clean_street = clean_street.replace(part, expansion)

    # removes possible multiple spaces.
    clean_street = ' '.join(clean_street.split())
    return clean_street


def _clean_state(state):
    """
    Helper function to return state abbreviations with lowercase characters and no whitespace.

    Verifies the state code is a two character state, commonwealth, territory,
    or military state code and returns the code lower cased.  If the code is
    not a valid abbreviation, an empty string is returned.

    :param state:  string to clean.
    :return:  a two character string with all alphabetic characters lower cased
        and all whitespace removed or empty string.
    """
    if state is None:
        return ''
    elif not isinstance(state, str):
        state = str(state)

    clean_state = state.strip()
    clean_state = clean_state.lower()
    return clean_state if clean_state in consts.STATE_ABBREVIATIONS else ''


def _clean_zip(code):
    """
    Helper function to return 5 character zip codes only.

    :param code:  string to clean and format as a zip code
    :return: a five character digit string to compare as a zip code
    """
    if code is None:
        return ''
    elif not isinstance(code, str):
        code = str(code)

    clean_code = ''
    code = code.strip()

    # ensure hyphenated part is ignored
    code = code.split('-')[0]
    code = code.split(' ')[0]

    # perform zero padding upto 5 chars
    code = code.zfill(5)

    for char in code:
        if char.isdigit():
            clean_code += char

    return clean_code


def _clean_phone(number):
    """
    Helper function to return only character digits.

    :param number:  string to clean.
    :return:  a string with everything that is not a digit removed.
    """
    if number is None:
        return ''
    elif not isinstance(number, str):
        number = str(number)

    clean_number = ''
    for char in number:
        if char.isdigit():
            clean_number += char
    return clean_number


def _clean_email(email):
    """
    Helper function to return emails with lowercase characters and no whitespace.

    :param email:  string to clean.
    :return:  a string with all alphabetic characters lower cased and all
        whitespace removed.
    """
    if email is None:
        return ''
    elif not isinstance(email, str):
        email = str(email)

    clean_email = email.strip()
    return clean_email.lower()


def _clean_name(name):
    """
    Helper function to return names with lowercase alphabetic characters only.

    :param name:  string to clean.
    :return:  a string with everything that is not an alphabetic character
        removed and all characters are lower cased.
    """
    if name is None:
        return ''
    elif not isinstance(name, str):
        name = str(name)

    clean_name = ''
    for char in name:
        if char.isalpha():
            clean_name += char
    return clean_name.lower()


def _compare_address_lists(list_one, list_two):
    """
    Counts the number of elements in list one that are not in list two.

    :param list_one: list of strings to check for existence in list_two
    :param list_two: list of strings to use for existence check of
        list_one parts
    :return: the count of items in list_one that are missing from list_two
    """
    diff = 0
    # TODO ensure 7 and 7th are identified as the same
    for part in list_one:
        if part not in list_two:
            diff += 1
    return diff


def _compare_name_fields(project, rdr_dataset, pii_dataset, hpo, concept_id, pii_field):
    """
    For an hpo, compare all first, middle, and last name fields to omop settings.

    This compares a site's name field values found in their uploaded PII
    tables with the values in the OMOP observation table.

    :param hpo: string name of hop to search.
    :return: a set of person_ids from the hpo PII name table that had name
        comparison performed and the updated match_values dictionary.
    """
    person_ids = set()
    match_values = {}

    rdr_names = _get_ppi_observation_match_values(
        project, rdr_dataset, consts.ID_MATCH_TABLE, concept_id
    )

    pii_names = _get_pii_values(project, pii_dataset, hpo, consts.PII_NAME_TABLE, pii_field)

    for person_id, pii_name in pii_names:
        person_ids.add(person_id)
        rdr_name = rdr_names.get(person_id)

        if rdr_name is None or pii_name is None:
            match_str = consts.MISSING
        else:
            pii_name = _clean_name(pii_name)
            rdr_name = _clean_name(rdr_name)
            match_str = consts.MATCH if rdr_name == pii_name else consts.MISMATCH

        match_values[person_id] = match_str

    return person_ids, match_values


def _compare_email_addresses(project, rdr_dataset, pii_dataset, hpo, concept_id, pii_field):
    """
    Compare email addresses from hpo PII table and OMOP observation table.

    :param hpo:  hpo site name.  used to query the correct pii table.
    :param email_addresses: dictionary of email addresses where person_id is
        the key and email address is the value
    :return: a set of person_ids for which email addresses were compared for the
        site. and the updated match_value dictionary.
    """
    person_ids = set()
    match_values = {}

    email_addresses = _get_ppi_observation_match_values(
        project, rdr_dataset, consts.ID_MATCH_TABLE, concept_id
    )

    pii_emails = _get_pii_values(project, pii_dataset, hpo, consts.PII_EMAIL_TABLE, pii_field)

    for person_id, pii_email in pii_emails:
        person_ids.add(person_id)
        rdr_email = email_addresses.get(person_id)

        if rdr_email is None or pii_email is None:
            match_str = consts.MISSING
        else:
            rdr_email = _clean_email(rdr_email)
            pii_email = _clean_email(pii_email)
            match_str = consts.MATCH if rdr_email == pii_email else consts.MISMATCH

        match_values[person_id] = match_str

    return person_ids, match_values


def _compare_phone_numbers(project, rdr_dataset, pii_dataset, hpo, concept_id, pii_field):
    """
    Compare the digit based phone numbers from PII and Observation tables.

    :param hpo:  hpo site name
    :param phone_numbers:  dictionary of rdr phone numbers where person_id is
        the key and phone numbers are values
    :return: A set of person_ids for which phone numbers were compared and
        the updated match_values dictionary.
    """
    person_ids = set()
    match_values = {}

    phone_numbers = _get_ppi_observation_match_values(
        project, rdr_dataset, consts.ID_MATCH_TABLE, concept_id
    )

    pii_phone_numbers = _get_pii_values(
        project,
        pii_dataset,
        hpo,
        consts.PII_PHONE_TABLE,
        pii_field
    )

    for person_id, pii_number in pii_phone_numbers:
        person_ids.add(person_id)
        rdr_phone = phone_numbers.get(person_id)

        if rdr_phone is None or pii_number is None:
            match_str = consts.MISSING
        else:
            rdr_phone = _clean_phone(rdr_phone)
            pii_number = _clean_phone(pii_number)
            match_str = consts.MATCH if rdr_phone == pii_number else consts.MISMATCH

        match_values[person_id] = match_str

    return person_ids, match_values


def _compare_cities(project, validation_dataset, rdr_dataset, pii_dataset, hpo, concept_id, pii_field):
    """
    Compare email addresses from hpo PII table and OMOP observation table.

    :param hpo:  hpo site name.  used to query the correct pii table.
    :param email_addresses: dictionary of email addresses where person_id is
        the key and email address is the value
    :return: a set of person_ids for which email addresses were compared for the
        site. and the updated match_value dictionary.
    """
    person_ids = set()
    match_values = {}

    cities = _get_ppi_observation_match_values(
        project, validation_dataset, consts.ID_MATCH_TABLE, concept_id
    )

    pii_cities = _get_location_pii(
        project,
        rdr_dataset,
        pii_dataset,
        hpo,
        consts.PII_ADDRESS_TABLE,
        pii_field
    )

    for person_id, pii_city in pii_cities:
        person_ids.add(person_id)
        rdr_city = cities.get(person_id)

        if rdr_city is None or pii_city is None:
            match_str = consts.MISSING
        else:
            rdr_city = _clean_name(rdr_city)
            pii_city = _clean_name(pii_city)
            match_str = consts.MATCH if rdr_city == pii_city else consts.MISMATCH

        match_values[person_id] = match_str

    return person_ids, match_values


def _compare_states(project, validation_dataset, rdr_dataset, pii_dataset, hpo, concept_id, pii_field):
    """
    Compare email addresses from hpo PII table and OMOP observation table.

    :param hpo:  hpo site name.  used to query the correct pii table.
    :param email_addresses: dictionary of email addresses where person_id is
        the key and email address is the value
    :return: a set of person_ids for which email addresses were compared for the
        site. and the updated match_value dictionary.
    """
    person_ids = set()
    match_values = {}

    states = _get_ppi_observation_match_values(
        project, validation_dataset, consts.ID_MATCH_TABLE, concept_id
    )

    pii_states = _get_location_pii(
        project,
        rdr_dataset,
        pii_dataset,
        hpo,
        consts.PII_ADDRESS_TABLE,
        pii_field
    )

    for person_id, pii_state in pii_states:
        person_ids.add(person_id)
        rdr_state = states.get(person_id)

        if rdr_state is None or pii_state is None:
            match_str = consts.MISSING
        else:
            rdr_state = _clean_state(rdr_state)
            pii_state = _clean_state(pii_state)
            match_str = consts.MATCH if rdr_state == pii_state else consts.MISMATCH

        match_values[person_id] = match_str

    return person_ids, match_values


def _compare_zip_codes(project, validation_dataset, rdr_dataset, pii_dataset, hpo, concept_id, pii_field):
    """
    Compare email addresses from hpo PII table and OMOP observation table.

    :param hpo:  hpo site name.  used to query the correct pii table.
    :param email_addresses: dictionary of email addresses where person_id is
        the key and email address is the value
    :return: a set of person_ids for which email addresses were compared for the
        site. and the updated match_value dictionary.
    """
    person_ids = set()
    match_values = {}

    zip_codes = _get_ppi_observation_match_values(
        project, validation_dataset, consts.ID_MATCH_TABLE, concept_id
    )

    pii_zip_codes = _get_location_pii(
        project,
        rdr_dataset,
        pii_dataset,
        hpo,
        consts.PII_ADDRESS_TABLE,
        pii_field
    )

    for person_id, pii_zip_code in pii_zip_codes:
        person_ids.add(person_id)
        rdr_zip = zip_codes.get(person_id)

        if rdr_zip is None or pii_zip_code is None:
            match_str = consts.MISSING
        else:
            rdr_zip = _clean_zip(rdr_zip)
            pii_zip = _clean_zip(pii_zip_code)
            match_str = consts.MATCH if rdr_zip == pii_zip else consts.MISMATCH

        match_values[person_id] = match_str

    return person_ids, match_values


def _compare_street_addresses(
        project,
        validation_dataset,
        rdr_dataset,
        pii_dataset,
        hpo,
        concept_id_one,
        concept_id_two,
        field_one,
        field_two
    ):
    """
    Compare the components of the standard address field.

    Individually compares the address one, address two, city, state, and zip
    fields of an address.  Compares address one and address two as distinct
    fields and if they do not match, then combines the fields and compares as
    a single field.  Both are either set as a match or not match.

    :param hpo:  hpo site name used to download pii from the site's pii table
    :return: a set of person_ids for which site pii addresses were compared and
        updated match_values dictionary.
    """
    person_ids = set()
    address_one_match_values = {}
    address_two_match_values = {}

    rdr_address_ones = _get_ppi_observation_match_values(
        project, validation_dataset, consts.ID_MATCH_TABLE, concept_id_one
    )

    rdr_address_twos = _get_ppi_observation_match_values(
        project, validation_dataset, consts.ID_MATCH_TABLE, concept_id_two
    )

    pii_street_ones = _get_location_pii(
        project, rdr_dataset, pii_dataset, hpo, consts.PII_ADDRESS_TABLE, field_one
    )
    pii_street_twos = _get_location_pii(
        project, rdr_dataset, pii_dataset, hpo, consts.PII_ADDRESS_TABLE, field_two
    )

    pii_street_addresses = {}
    for person_id, street in pii_street_ones:
        pii_street_addresses[person_id] = [person_id, street]

    for person_id, street in pii_street_twos:
        current_value = pii_street_addresses.get(person_id, [])

        if current_value == []:
            current_value = [person_id, '', street]
        else:
            current_value.append(street)

        pii_street_addresses[person_id] = current_value

    for person_id, addresses in pii_street_addresses.iteritems():
        person_ids.add(person_id)

        pii_addr_one = addresses[1]
        pii_addr_two = addresses[2]

        rdr_addr_one = _clean_street(rdr_address_ones.get(person_id))
        pii_addr_one = _clean_street(pii_addr_one)
        rdr_addr_two = _clean_street(rdr_address_twos.get(person_id))
        pii_addr_two = _clean_street(pii_addr_two)

        # easy case, fields 1 and 2 from both sources match exactly
        if rdr_addr_one == pii_addr_one and rdr_addr_two == pii_addr_two:
            address_one_match_values[person_id] = consts.MATCH
            address_two_match_values[person_id] = consts.MATCH
        else:
            # convert two fields to one field and store as a list of strings
            full_rdr_street = rdr_addr_one + ' ' + rdr_addr_two
            full_pii_street = pii_addr_one + ' ' + pii_addr_two
            full_rdr_street_list = full_rdr_street.split()
            full_pii_street_list = full_pii_street.split()

            # check top see if each item in one list is in the other list  and
            # set match results from that
            missing_rdr = _compare_address_lists(full_rdr_street_list, full_pii_street_list)
            missing_pii = _compare_address_lists(full_pii_street_list, full_rdr_street_list)

            if (missing_rdr + missing_pii) > 0:
                address_one_match_values[person_id] = consts.MISMATCH
                address_two_match_values[person_id] = consts.MISMATCH
            else:
                address_one_match_values[person_id] = consts.MATCH
                address_two_match_values[person_id] = consts.MATCH

    return person_ids, address_one_match_values, address_two_match_values


def _compare_birth_dates(
        project,
        validation_dataset,
        rdr_dataset,
        person_id_set,
        concept_id_pii,
        concept_id_ehr
    ):
    """
    Compare birth dates for people.

    Converts birthdates and birth_datetimes to calendar objects.  Converts
    the calendar objects back to strings with the same format and compares
    these strings.

    :param person_id_set: set of person_ids gathered from PII tables.
    :param rdr_birthdates: dictionary of rdr birth datetimes where person_id is
        the key and a birth datetime string is the value
    :param ehr_birthdates: dictionary of birth datetimes where person_id is
        the key and a birth datetime string is the value
    :return: updated match_values dictionary
    """
    match_values = {}

    pii_birthdates = _get_ppi_observation_match_values(
        project, validation_dataset, consts.ID_MATCH_TABLE, concept_id_pii
    )

    person_id_list = [str(person_id) for person_id in person_id_set]

    ehr_birthdates = _get_ehr_observation_match_values(
        project,
        rdr_dataset,
        consts.OBSERVATION_TABLE,
        concept_id_ehr,
        ', '.join(person_id_list)
    )

    # compare birth_datetime from ppi info to ehr info and record results.
    for person_id in person_id_set:
        rdr_birthdate = pii_birthdates.get(person_id)
        ehr_birthdate = ehr_birthdates.get(person_id)

        if rdr_birthdate is None or ehr_birthdate is None:
            match_values[person_id] = consts.MISSING
        elif isinstance(rdr_birthdate, str) and isinstance(ehr_birthdate, str):
            # convert values to datetime objects
            rdr_date = parse(rdr_birthdate)
            ehr_date = parse(ehr_birthdate)
            # convert datetime objects to Year/month/day strings and compare
            rdr_string = rdr_date.strftime(consts.DATE)
            ehr_string = ehr_date.strftime(consts.DATE)

            match_str = consts.MATCH if rdr_string == ehr_string else consts.MISMATCH
            match_values[person_id] = match_str
        else:
            match_values[person_id] = consts.MISMATCH

    return match_values


def _update_known_person_ids(person_ids, hpo, person_id_set):
    current_value = person_ids.get(hpo, set())
    current_value.update(person_id_set)
    person_ids[hpo] = current_value
    return person_ids


def  _append_to_result_table(
        site,
        match_values,
        project,
        validation_dataset,
        field_name
    ):
    result_table = site + consts.VALIDATION_TABLE_SUFFIX

    # create initial values list for insertion
    values_list = []
    for key, value in match_values.iteritems():
        values_list.append('(' + str(key) + ', \'' + value + '\')')

    values_str = ', '.join(values_list)

    query = consts.INSERT_MATCH_VALUES.format(
        project=project,
        dataset=validation_dataset,
        table=result_table,
        field=field_name,
        values=values_str,
        id_field=consts.PERSON_ID_FIELD
    )

    LOGGER.debug("Inserting match values for site: %s\t\tfield: %s", site, field_name)

    results = bq_utils.query(query, batch=True)

    return results

def _remove_sparse_records(project, dataset, site):
    """
    Remove sparsely populated records.

    The sparse records were perviously merged into a unified record.  This
    removes the sparse data records.

    :param project: The project string identifier
    :param dataset: The validation dataset identifier
    :param site: The site to delete sparse validation results for
    """
    result_table = site + consts.VALIDATION_TABLE_SUFFIX

    LOGGER.debug("Removing lingering sparse records from %s.%s.%s", project, dataset, result_table)

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

    results = bq_utils.query(query, batch=True)
    return results


def _merge_fields_into_single_record(project, dataset, site):
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

    LOGGER.debug("Unifying sparse records for %s.%s.%s", project, dataset, result_table)

    for validation_field in consts.VALIDATION_FIELDS:
        query = consts.MERGE_UNIFY_SITE_RECORDS.format(
            project=project,
            dataset=dataset,
            table=result_table,
            field=validation_field
        )

        results = bq_utils.query(query, batch=True)
    return results


def _change_nulls_to_missing_value(project, dataset, site):
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

    LOGGER.debug("Setting null fields to %s in %s.%s.%s", consts.MISSING, project, dataset, result_table)

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

    results = bq_utils.query(query, batch=True)
    return results



def match_participants(project, rdr_dataset, pii_dataset, dest_dataset_id):
    """
    Entry point for performing participant matching of PPI, EHR, and PII data.

    :param project: a string representing the project name

    :return: results of the field comparison for each hpo
    """
    # create new dataset for the intermediate tables and results
    dataset_result = bq_utils.create_dataset(dataset_id=dest_dataset_id,
                                             description='provenance info',
                                             overwrite_existing=True)

    validation_dataset = dataset_result.get(bq_consts.DATASET_REF, {})
    validation_dataset = validation_dataset.get(bq_consts.DATASET_ID, '')

    # create intermediate observation table in new dataset
    _set_observation_match_values_table(project, rdr_dataset, dest_dataset_id)

    hpo_sites = ['lrwb_test_table']
#    hpo_sites = _get_hpo_site_names()

    field_list = []
    with open('resources/fields/identity_match.json') as json_file:
        field_list = json.load(json_file)

    for site_name in hpo_sites:
        bq_utils.create_table(
            site_name + consts.VALIDATION_TABLE_SUFFIX,
            field_list,
            drop_existing=True,
            dataset_id=validation_dataset
        )

    results = {}
    person_ids = {}

    # validate first names
    for site in hpo_sites:
        person_id_set, match_values = _compare_name_fields(
            project,
            validation_dataset,
            pii_dataset,
            site,
            consts.OBS_PII_NAME_FIRST,
            consts.FIRST_NAME_FIELD
        )
        person_ids = _update_known_person_ids(person_ids, site, person_id_set)

        _append_to_result_table(
            site,
            match_values,
            project,
            validation_dataset,
            consts.FIRST_NAME_FIELD
        )

    # validate last names
    for site in hpo_sites:
        person_id_set, match_values = _compare_name_fields(
            project,
            validation_dataset,
            pii_dataset,
            site,
            consts.OBS_PII_NAME_LAST,
            consts.LAST_NAME_FIELD
        )
        person_ids = _update_known_person_ids(person_ids, site, person_id_set)
        # write last name matches for hpo to table
        _append_to_result_table(
            site,
            match_values,
            project,
            validation_dataset,
            consts.LAST_NAME_FIELD
        )

    # validate middle names
    for site in hpo_sites:
        person_id_set, match_values = _compare_name_fields(
            project,
            validation_dataset,
            pii_dataset,
            site,
            consts.OBS_PII_NAME_MIDDLE,
            consts.MIDDLE_NAME_FIELD
        )
        person_ids = _update_known_person_ids(person_ids, site, person_id_set)
        # write middle name matches for hpo to table
        _append_to_result_table(
            site,
            match_values,
            project,
            validation_dataset,
            consts.MIDDLE_NAME_FIELD
        )

    # validate zip codes
    for site in hpo_sites:
        person_id_set, match_values = _compare_zip_codes(
            project,
            validation_dataset,
            rdr_dataset,
            pii_dataset,
            site,
            consts.OBS_PII_STREET_ADDRESS_ZIP,
            consts.ZIP_CODE_FIELD
        )
        person_ids = _update_known_person_ids(person_ids, site, person_id_set)
        # write zip codes matces for hpo to table
        _append_to_result_table(
            site,
            match_values,
            project,
            validation_dataset,
            consts.ZIP_CODE_FIELD
        )

    # validate city
    for site in hpo_sites:
        person_id_set, match_values = _compare_cities(
            project,
            validation_dataset,
            rdr_dataset,
            pii_dataset,
            site,
            consts.OBS_PII_STREET_ADDRESS_CITY,
            consts.CITY_FIELD
        )
        person_ids = _update_known_person_ids(person_ids, site, person_id_set)
        # write city matches for hpo to table
        _append_to_result_table(
            site,
            match_values,
            project,
            validation_dataset,
            consts.CITY_FIELD
        )

    # validate state
    for site in hpo_sites:
        person_id_set, match_values = _compare_states(
            project,
            validation_dataset,
            rdr_dataset,
            pii_dataset,
            site,
            consts.OBS_PII_STREET_ADDRESS_STATE,
            consts.STATE_FIELD
        )
        person_ids = _update_known_person_ids(person_ids, site, person_id_set)
        # write state matches for hpo to table
        _append_to_result_table(
            site,
            match_values,
            project,
            validation_dataset,
            consts.STATE_FIELD
        )

    # validate street addresses
    for site in hpo_sites:
        person_id_set, address_one_match_values, address_two_match_values = _compare_street_addresses(
            project,
            validation_dataset,
            rdr_dataset,
            pii_dataset,
            site,
            consts.OBS_PII_STREET_ADDRESS_ONE,
            consts.OBS_PII_STREET_ADDRESS_TWO,
            consts.ADDRESS_ONE_FIELD,
            consts.ADDRESS_TWO_FIELD
        )
        person_ids = _update_known_person_ids(person_ids, site, person_id_set)
        # write street address matches for hpo to table
        _append_to_result_table(
            site,
            address_one_match_values,
            project,
            validation_dataset,
            consts.ADDRESS_ONE_FIELD
        )
        _append_to_result_table(
            site,
            address_two_match_values,
            project,
            validation_dataset,
            consts.ADDRESS_TWO_FIELD
        )

    # validate email addresses
    for site in hpo_sites:
        person_id_set, match_values = _compare_email_addresses(
            project,
            validation_dataset,
            pii_dataset,
            site,
            consts.OBS_PII_EMAIL_ADDRESS,
            consts.EMAIL_FIELD
        )
        person_ids = _update_known_person_ids(person_ids, site, person_id_set)
        # write email matches for hpo to table
        _append_to_result_table(
            site,
            match_values,
            project,
            validation_dataset,
            consts.EMAIL_FIELD
        )

    # validate phone numbers
    for site in hpo_sites:
        person_id_set, match_values = _compare_phone_numbers(
            project,
            validation_dataset,
            pii_dataset,
            site,
            consts.OBS_PII_PHONE,
            consts.PHONE_NUMBER_FIELD
        )
        person_ids = _update_known_person_ids(person_ids, site, person_id_set)
        # write phone number matches for hpo to table
        _append_to_result_table(
            site,
            match_values,
            project,
            validation_dataset,
            consts.PHONE_NUMBER_FIELD
        )

    # validate birth dates
    for site, participants in person_ids.iteritems():
        match_values = _compare_birth_dates(
            project,
            validation_dataset,
            rdr_dataset,
            participants,
            consts.OBS_PII_BIRTH_DATETIME,
            consts.OBS_EHR_BIRTH_DATETIME
        )
        # write birthday match for hpo to table
        _append_to_result_table(
            site,
            match_values,
            project,
            validation_dataset,
            consts.BIRTH_DATE_FIELD
        )

    # generate single clean record for each participant at each site
    for site in hpo_sites:
        _merge_fields_into_single_record(project, validation_dataset, site)
        _remove_sparse_records(project, validation_dataset, site)
        _change_nulls_to_missing_value(project, validation_dataset, site)

    # TODO:  generate hpo site reports

    # TODO:  generate aggregate site report

    return results


if __name__ == '__main__':
    RDR_DATASET = 'lrwb_combined20190415'
    PII_DATASET = 'lrwb_pii_tables'
    PROJECT = 'aou-res-curation-test'
    DEST_DATASET_ID = 'temp_dataset_id20190415'
    match_participants(PROJECT, RDR_DATASET, PII_DATASET, DEST_DATASET_ID)
