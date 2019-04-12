"""
A module to perform initial participant identity matching based on PII data.

Compares site PII data to values from the RDR, looking to identify discrepancies.
"""
import logging

from dateutil.parser import parse

import bq_utils
import resources as rc
import constants.validation.participants.identity_match as consts

LOGGER = logging.getLogger(__name__)


def _get_utf8_string(value):
    result = ''
    try:
        result = value.encode('utf-8', 'ignore')
    except AttributeError:
        LOGGER.debug("Value '{}' can not be utf-8 encoded", value)

    return result


def _get_all_observation_match_values(project, date_string, destination_dataset):
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
        date_string=date_string,
        table=consts.OBSERVATION_TABLE

    )

    LOGGER.debug("Participant validation ran the query\n%s", query_string)
    results = bq_utils.query(
        query_string,
        destination_dataset_id=destination_dataset,
        destination_table_id=consts.ID_MATCH_TABLE,
        write_disposition='WRITE_TRUNCATE'
    )

    return consts.ID_MATCH_TABLE


def _get_ehr_observation_match_values(table_name, date_string, concept_id):
    """
    Get the desired matching values from the combined observation table.

    This retrieves all possible matching values from the observation table
    used in participant matching.  Returned query data is limited to the
    person_id, observation_concept_id, and the value_as_string fields.

    :param date_string: a string formatted date as YYYYMMDD that will be used
        to identify which dataset to use as a lookup
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
    query_string = consts.EHR_OBSERVATION_VALUES.format(date_string=date_string,
                                                        table=table_name,
                                                        field_value=concept_id)

    LOGGER.debug("Participant validation ran the query\n%s", query_string)
    results = bq_utils.query(query_string)
    row_results = bq_utils.response_to_large_rowset(results)

    result_dict = {}
    for item in row_results:
        person_id = item.get(consts.PERSON_ID)
        value = item.get(consts.STRING_VALUE)
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


def _get_ppi_observation_match_values(table_name, date_string, concept_id):
    """
    Get the desired matching values from the combined observation table.

    This retrieves all possible matching values from the observation table
    used in participant matching.  Returned query data is limited to the
    person_id, observation_concept_id, and the value_as_string fields.

    :param date_string: a string formatted date as YYYYMMDD that will be used
        to identify which dataset to use as a lookup
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
    query_string = consts.PPI_OBSERVATION_VALUES.format(date_string=date_string,
                                                        table=table_name,
                                                        field_value=concept_id)

    LOGGER.debug("Participant validation ran the query\n%s", query_string)
    results = bq_utils.query(query_string)
    row_results = bq_utils.response_to_large_rowset(results)

    result_dict = {}
    for item in row_results:
        person_id = item.get(consts.PERSON_ID)
        value = item.get(consts.STRING_VALUE)
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


def _get_pii_values(hpo, date_string, table, field):
    """
    Get values from the site's PII table.

    :param hpo:  hpo string to use when identifying table names for lookup.

    :return:  A list of tuples with the first tuple element as the person_id
    and the second tuple element as the phone number.
    [(1, '5558675309'), (48, '5558004600'), (99, '5551002000')]
    """
    query_string = consts.PII_VALUES.format(date_string=date_string,
                                            hpo_site_str=hpo,
                                            field=field,
                                            table_suffix=table)
    LOGGER.debug("Participant validation ran the query\n%s", query_string)
    results = bq_utils.query(query_string)
    row_results = bq_utils.large_response_to_rows(results)

    result_list = []
    for item in row_results:
        person_id = item.get(consts.PERSON_ID)
        value = item.get(field)

        value = _get_utf8_string(value)

        result_list.append((person_id, value))

    return result_list

def _get_location_pii(hpo, date_string, table, field):
    location_ids = _get_pii_values(hpo, date_string, table, consts.LOCATION_ID_FIELD)

    location_id_list = []
    location_id_dict = {}
    for location_id in location_ids:
        location_id_list.append(location_id[1])
        location_id_dict[location_id[1]] = location_id[0]

    location_id_str = ', '.join(location_id_list)
    query_string = consts.PII_LOCATION_VALUES.format(
        date_string=date_string,
        field=field,
        id_list=location_id_str
    )

    LOGGER.debug("Participant validation ran the query\n%s", query_string)
    results = bq_utils.query(query_string)
    row_results = bq_utils.large_response_to_rows(results)

    result_list = []
    for item in row_results:
        location_id = item.get(consts.LOCATION_ID_FIELD)
        value = item.get(field)

        value = _get_utf8_string(value)

        person_id = location_id_dict.get(location_id, '')
        result_list.append((person_id, value))

    return result_list


def _get_pii_addresses(hpo):
    """
    Get email addresses from the site's PII table.

    :param hpo:  hpo string to use when identifying table names for lookup.

    :return:  A list of tuples with the first tuple element as the person_id,
    the second tuple element as the address line one, the third tuple element is
    address line 2, the fourth tuple element is city, the fifth tuple element is
    state, and the sixth tuple element is zip code.
    [(1, 'street 1', '', 'nashville', 'tn', 33333),
     (2, 'street_8', 'street_10', 'yuma', 'az', 22222)]
    """
    return []


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


def _compare_name_fields(hpo, date_string, concept_id, pii_field):
    """
    For an hpo, compare all first, middle, and last name fields to omop settings.

    This compares a site's name field values found in their uploaded PII
    tables with the values in the OMOP observation table.

    :param hpo: string name of hop to search.
    :param date_string: a string formatted date as YYYYMMDD that will be used
        to identify which dataset to use as a lookup
    :return: a set of person_ids from the hpo PII name table that had name
        comparison performed and the updated match_values dictionary.
    """
    person_ids = set()
    match_values = {}

    obs_names = _get_ppi_observation_match_values(
        consts.ID_MATCH_TABLE, date_string, concept_id
    )

    for person_id, name in _get_pii_values(hpo, date_string, consts.PII_NAME_TABLE, pii_field):
        person_ids.add(person_id)
        name = _clean_name(name)
        rdr_name = _clean_name(obs_names.get(person_id))

        match_str = consts.MATCH if rdr_name == name else consts.MISMATCH
        match_values[person_id] = match_str

    return person_ids, match_values


def _compare_email_addresses(hpo, date_string, concept_id, pii_field):
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
        consts.ID_MATCH_TABLE, date_string, concept_id
    )

    for person_id, email in _get_pii_values(hpo, date_string, consts.PII_EMAIL_TABLE, pii_field):
        person_ids.add(person_id)

        rdr_email = _clean_email(email_addresses.get(person_id))
        pii_email = _clean_email(email)

        match_str = consts.MATCH if rdr_email == pii_email else consts.MISMATCH
        match_values[person_id] = match_str

    return person_ids, match_values


def _compare_phone_numbers(hpo, date_string, concept_id, pii_field):
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
        consts.ID_MATCH_TABLE, date_string, concept_id
    )

    for person_id, number in _get_pii_values(hpo, date_string, consts.PII_PHONE_TABLE, pii_field):
        person_ids.add(person_id)

        rdr_phone = _clean_phone(phone_numbers.get(person_id))
        pii_number = _clean_phone(number)

        match_str = consts.MATCH if rdr_phone == pii_number else consts.MISMATCH
        match_values[person_id] = match_str

    return person_ids, match_values


def _compare_cities(hpo, date_string, concept_id, pii_field):
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
        consts.ID_MATCH_TABLE, date_string, concept_id
    )

    for person_id, city in _get_location_pii(hpo, date_string, consts.PII_ADDRESS_TABLE, pii_field):
        person_ids.add(person_id)

        rdr_city = _clean_name(cities.get(person_id))
        pii_city = _clean_name(city)

        match_str = consts.MATCH if rdr_city == pii_city else consts.MISMATCH
        match_values[person_id] = match_str

    return person_ids, match_values


def _compare_states(hpo, date_string, concept_id, pii_field):
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
        consts.ID_MATCH_TABLE, date_string, concept_id
    )

    for person_id, state in _get_location_pii(hpo, date_string, consts.PII_ADDRESS_TABLE, pii_field):
        person_ids.add(person_id)

        rdr_state = _clean_state(states.get(person_id))
        pii_state = _clean_state(state)

        match_str = consts.MATCH if rdr_state == pii_state else consts.MISMATCH
        match_values[person_id] = match_str

    return person_ids, match_values


def _compare_zip_codes(hpo, date_string, concept_id, pii_field):
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
        consts.ID_MATCH_TABLE, date_string, concept_id
    )

    for person_id, zip_code in _get_location_pii(hpo, date_string, consts.PII_ADDRESS_TABLE, pii_field):
        person_ids.add(person_id)

        rdr_zip = _clean_zip(zip_codes.get(person_id))
        pii_zip = _clean_zip(zip_code)

        match_str = consts.MATCH if rdr_zip == pii_zip else consts.MISMATCH
        match_values[person_id] = match_str

    return person_ids, match_values


def _compare_street_addresses(
        hpo,
        date_string,
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
        consts.ID_MATCH_TABLE, date_string, concept_id_one
    )

    rdr_address_twos = _get_ppi_observation_match_values(
        consts.ID_MATCH_TABLE, date_string, concept_id_two
    )

    pii_street_ones = _get_location_pii(hpo, date_string, consts.PII_ADDRESS_TABLE, field_one)
    pii_street_twos = _get_location_pii(hpo, date_string, consts.PII_ADDRESS_TABLE, field_two)

    pii_street_addresses = {}
    for person_id, street in pii_street_ones:
        pii_street_addresses[person_id] = [person_id, street]

    for person_id, street in pii_street_twos:
        current_value = pii_street_addresses.get(person_id, [])
        pii_street_addresses[person_id] = current_value.append(street)

    for person_id, pii_addr_one, pii_addr_two in pii_street_addresses:
        person_ids.add(person_id)

        rdr_addr_one = _clean_street(streets_one.get(person_id))
        pii_addr_one = _clean_street(pii_addr_one)
        rdr_addr_two = _clean_street(streets_two.get(person_id))
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
        hpo,
        date_string,
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
        consts.ID_MATCH_TABLE, date_string, concept_id_pii
    )

    ehr_birthdates = _get_ehr_observation_match_values(
        consts.OBSERVATION_TABLE, date_string, concept_id_ehr, ', '.join(list(person_id_set))
    )

    # compare birth_datetime from ppi info to ehr info and record results.
    for person_id in person_id_set:
        rdr_birthdate = rdr_birthdates.get(person_id)
        ehr_birthdate = ehr_birthdates.get(person_id)

        if (rdr_birthdate is None and ehr_birthdate is not None) \
                or (ehr_birthdate is None and rdr_birthdate is not None):
            match_values[person_id] = consts.MISMATCH
            return match_values
        elif rdr_birthdate is None and ehr_birthdate is None:
            match_values[person_id] = consts.MATCH
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
    person_ids[hpo] = current_value.update(person_id_set)


def match_participants(project, date_string):
    """
    Entry point for performing participant matching of PPI, EHR, and PII data.

    :param project: a string representing the project name
    :param date_string: a string formatted date as YYYYMMDD that will be used
        to identify which dataset to use as a lookup

    :return: results of the field comparison for each hpo
    """
    # create new dataset for the intermediate tables and results
    dest_dataset_id = 'temp_dataset_id'
    dataset_result = bq_utils.create_dataset(dataset_id=dest_dataset_id,
                                             description='provenance info')

    # create intermediate table in new dataset
    rdr_values = _get_all_observation_match_values(project, date_string, dest_dataset_id)

    hpo_sites = _get_hpo_site_names()

    # TODO:  create tables for data types for each hpo

    results = {}
    person_ids = {}

    # validate first names
    for site in hpo_sites:
        person_id_set, match_values = _compare_name_fields(
            site, date_string, consts.OBS_PII_NAME_FIRST, consts.FIRST_NAME
        )
        person_ids = _update_known_person_ids(person_ids, site, person_id_set)
        # write first name matches for hpo to table

    # validate last names
    for site in hpo_sites:
        person_id_set, match_values = _compare_name_fields(
            site, date_string, consts.OBS_PII_NAME_LAST, consts.LAST_NAME
        )
        person_ids = _update_known_person_ids(person_ids, site, person_id_set)
        # write last name matches for hpo to table

    # validate middle names
    for site in hpo_sites:
        person_id_set, match_values = _compare_name_fields(
            site, date_string, consts.OBS_PII_NAME_MIDDLE, consts.MIDDLE_NAME
        )
        person_ids = _update_known_person_ids(person_ids, site, person_id_set)
        # write middle name matches for hpo to table

    # validate email addresses
    for site in hpo_sites:
        person_id_set, match_values = _compare_email_addresses(
            site, date_string, consts.OBS_PII_EMAIL_ADDRESS, consts.EMAIL_FIELD
        )
        person_ids = _update_known_person_ids(person_ids, site, person_id_set)
        # write email matches for hpo to table

    # validate phone numbers
    for site in hpo_sites:
        person_id_set, match_values = _compare_phone_numbers(
            site, date_string, consts.OBS_PII_PHONE, consts.PHONE_NUMBER_FIELD
        )
        person_ids = _update_known_person_ids(person_ids, site, person_id_set)
        # write phone number matches for hpo to table

    # validate zip codes
    for site in hpo_sites:
        person_id_set, match_values = _compare_zip_codes(
            site, date_string, consts.OBS_PII_STREET_ADDRESS_ZIP, consts.ZIP_CODE_FIELD
        )
        person_ids = _update_known_person_ids(person_ids, site, person_id_set)
        # write zip codes matces for hpo to table

    # validate city
    for site in hpo_sites:
        person_id_set, match_values = _compare_cities(
            site, date_string, consts.OBS_PII_STREET_ADDRESS_CITY, consts.CITY_FIELD
        )
        person_ids = _update_known_person_ids(person_ids, site, person_id_set)
        # write city matches for hpo to table

    # validate state
    for site in hpo_sites:
        person_id_set, match_values = _compare_states(
            site, date_string, consts.OBS_PII_STREET_ADDRESS_STATE, consts.STATE_FIELD
        )
        person_ids = _update_known_person_ids(person_ids, site, person_id_set)
        # write state matches for hpo to table

    # validate street addresses
    for site in hpo_sites:
        person_id_set, match_values = _compare_street_addresses(
            site,
            date_string,
            consts.OBS_PII_STREET_ADDRESS_ONE,
            consts.OBS_PII_STREET_ADDRESS_TWO,
            consts.ADDRESS_ONE_FIELD,
            consts.ADDRESS_TWO_FIELD
        )
        person_ids = _update_known_person_ids(person_ids, site, person_id_set)
        # write street address matches for hpo to table

    # validate birth dates
    for site, participants in person_ids.iteritems():
        #TODO:  Continue working on this comparison method.
        match_values = _compare_birth_dates(site, date_string, participants, consts.OBS_PII_BIRTH_DATETIME,
        consts.OBS_EHR_BIRTH_DATETIME)
        # write birthday match for hpo to table

    # TODO:  generate hpo site reports

    # TODO:  generate aggregate site report

    return results


if __name__ == '__main__':
    DATASET = '20190326'
#    PROJECT = 'aou-res-curation-prod'
    PROJECT = 'aou-res-curation-test'
    match_participants(PROJECT, DATASET)
#    bq_utils.create_dataset(dataset_id='lrwb_validation_test1', description='provenance info')
