"""
A module to perform initial participant identity matching based on PII data.

Compares site PII data to values from the RDR, looking to identify discrepancies.
"""
from datetime import datetime
from dateutil.parser import parse
import logging

#import bq_utils
import validation.participants.consts as consts

def _get_observation_match_values(dataset):
    """
    Get the desired matching values from the combined observation table.

    This retrieves all possible matching values from the observation table
    used in participant matching.  Returned query data is limited to the
    person_id, observation_concept_id, and the value_as_string fields.

    :param dataset:  The string name of the dataset to use when matching participants

    :return:  A dictionary with observation_concept_id as keys.  Each value is
        a dictionary of person_ids with the associated value of the concept_id.
        For example:
        { 1585260: {person_id_1:  "email_address", person_id_2: "email_address"},
          1585596: {person_id_1: "first_name", person_id_2: "first_name"},
          1585597: {person_id_1: "middle_name", },
          1585598: {person_id_1: "last_name", person_id_2: "last_name"},
          ...,
        }
    """
    query_string = consts.PPI_OBSERVATION_VALUES.format(dataset=dataset,
                                                        table='observation')
    print(query_string)
#    results = bq_utils.query(query_string)
    return {}

def _get_hpo_site_names():
    # This will have to do until I can talk with other engineers.
    return []

def _get_pii_names(hpo):
    """
    Get names from the site's PII table.

    :param hpo: hpo string to use when identifying table names for lookup.

    :return:  A list of tuples with the first tuple element as the pserson_id
    and the second tuple element as the first name, the third tuple element
    is the middle name, the fourth tuple element is the last name.  For example,
    [(1, 'suzie', '', 'q'), (48, 'johnny', 'B', 'Good'), (99, 'roy', 'g', 'biv')]
    """
    return []

def _get_pii_emails(hpo):
    """
    Get email addresses from the site's PII table.

    :param hpo:  hpo string to use when identifying table names for lookup.

    :return:  A list of tuples with the first tuple element as the person_id
    and the second tuple element as the email address.
    [(1, 'anon@gmail.com'), (99, '')]
    """
    return []

def _get_pii_phone_numbers(hpo):
    """
    Get email addresses from the site's PII table.

    :param hpo:  hpo string to use when identifying table names for lookup.

    :return:  A list of tuples with the first tuple element as the person_id
    and the second tuple element as the phone number.
    [(1, '5558675309'), (48, '5558004600'), (99, '5551002000')]
    """
    return []

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

def _record_match_value(results, person_id, field, match):
    """
    Helper function to record the match status of a field for a person_id.

    :param results: a dictionary to store results for each site participant.
        it is expected to be of the form {person_id: {field_1: value_1, field_2: value_2},}
    :param person_id: integer id value.  should be unique, meaning all data
        associated with this person_id should belong to the same person
    :param field: field name for which the match is being recorded
    :param match: value to set as match or mismatch
    :return: the updated results dictionary
    """
    try:
        results[person_id][field] = match
    except KeyError:
        # key error will be caught if the person_id doesn't exist in the result
        # set yet.
        results[person_id] = {field: match}
    return results

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

    if len(code) > 5 and code[5] in ['-', ' ']:
        code = code[0:5]

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
    for part in list_one:
        if part not in list_two:
            diff += 1
    return diff

def _compare_name_fields(hpo, first_names, middle_names, last_names, match_values):
    """
    For an hpo, compare all first, middle, and last name fields to omop settings.

    This compares a site's name field values found in their uploaded PII
    tables with the values in the OMOP observation table.

    :param hpo: string name of hop to search.
    :param first_names:  dictionary of first names where person_id is the key
        and name is the value
    :param middle_names: dictionary of middle names where person_id is the key
        and name is the value
    :param last_names: dictionary of last names where person_id is the key
        and name is the value
    :param match_values: current result set dictionary to update with new
        match information
    :return: a set of person_ids from the hpo PII name table that had name
        comparison performed and the updated match_values dictionary.
    """
    person_ids = set()

    for person_id, first, middle, last in _get_pii_names(hpo):
        person_ids.add(person_id)
        first = _clean_name(first)
        middle = _clean_name(middle)
        last = _clean_name(last)
        rdr_first = _clean_name(first_names.get(person_id))
        rdr_middle = _clean_name(middle_names.get(person_id))
        rdr_last = _clean_name(last_names.get(person_id))

        match_str = consts.MATCH if rdr_first == first else consts.MISMATCH
        _record_match_value(match_values, person_id, consts.FIRST_NAME, match_str)

        match_str = consts.MATCH if rdr_middle == middle else consts.MISMATCH
        _record_match_value(match_values, person_id, consts.MIDDLE_NAME, match_str)

        match_str = consts.MATCH if rdr_last == last else consts.MISMATCH
        _record_match_value(match_values, person_id, consts.LAST_NAME, match_str)

    return person_ids, match_values

def _compare_email_addresses(hpo, email_addresses, match_values):
    """
    Compare email addresses from hpo PII table and OMOP observation table.

    :param hpo:  hpo site name.  used to query the correct pii table.
    :param email_addresses: dictionary of email addresses where person_id is
        the key and email address is the value
    :param match_values: current result set dictionary to update with new
        match information
    :return: a set of person_ids for which email addresses were compared for the
        site. and the updated match_value dictionary.
    """
    person_ids = set()

    for person_id, email in _get_pii_emails(hpo):
        person_ids.add(person_id)

        rdr_email = _clean_email(email_addresses.get(person_id))
        pii_email = _clean_email(email)

        match_str = consts.MATCH if rdr_email == pii_email else consts.MISMATCH
        _record_match_value(match_values, person_id, consts.EMAIL, match_str)

    return person_ids, match_values

def _compare_phone_numbers(hpo, phone_numbers, match_values):
    """
    Compare the digit based phone numbers from PII and Observation tables.

    :param hpo:  hpo site name
    :param phone_numbers:  dictionary of rdr phone numbers where person_id is
        the key and phone numbers are values
    :param match_values: current result set dictionary to update with new
        match information
    :return: A set of person_ids for which phone numbers were compared and
        the updated match_values dictionary.
    """
    person_ids = set()

    for person_id, number in _get_pii_phone_numbers(hpo):
        person_ids.add(person_id)

        rdr_phone = _clean_phone(phone_numbers.get(person_id))
        pii_number = _clean_phone(number)

        match_str = consts.MATCH if rdr_phone == pii_number else consts.MISMATCH
        _record_match_value(match_values, person_id, consts.CONTACT_PHONE, match_str)

    return person_ids, match_values

def _compare_address_fields(hpo, streets_one, streets_two, cities, states, zips, match_values):
    """
    Compare the components of the standard address field.

    Individually compares the address one, address two, city, state, and zip
    fields of an address.  Compares address one and address two as distinct
    fields and if they do not match, then combines the fields and compares as
    a single field.  Both are either set as a match or not match.

    :param hpo:  hpo site name used to download pii from the site's pii table
    :param streets_one: dictionary of rdr street address one values where
        person_id is the key and the address is the value
    :param streets_two: dictionary of rdr street address two values where
        person_id is the key and the address is the value
    :param cities: dictionary of city values where person_id is the key
        and city names are the values
    :param states: dictionary of state values where person_id is the key and
        state postal abbreviations are the values
    :param zips: dictionary of zip codes where person_id is the key and U.S.
        postal codes are the values
    :param match_values: current result set dictionary to update with new
        match information
    :return: a set of person_ids for which site pii addresses were compared and
        updated match_values dictionary.
    """
    person_ids = set()
    for person_id, addr_1, addr_2, city, state, zip_code in _get_pii_addresses(hpo):
        person_ids.add(person_id)

        rdr_addr_one = _clean_street(streets_one.get(person_id))
        addr_1 = _clean_street(addr_1)
        rdr_addr_two = _clean_street(streets_two.get(person_id))
        addr_2 = _clean_street(addr_2)

        # easy case, fields 1 and 2 from both sources match exactly
        if rdr_addr_one == addr_1 and rdr_addr_two == addr_2:
            _record_match_value(match_values, person_id, consts.STREET_ONE, consts.MATCH)
            _record_match_value(match_values, person_id, consts.STREET_TWO, consts.MATCH)
        else:
            # convert two fields to one field and store as a list of strings
            full_rdr_street = rdr_addr_one  + ' ' + rdr_addr_two
            full_pii_street = addr_1  + ' ' + addr_2
            full_rdr_street_list = full_rdr_street.split()
            full_pii_street_list = full_pii_street.split()

            # check top see if each item in one list is in the other list  and
            # set match results from that
            missing_rdr = _compare_address_lists(full_rdr_street_list, full_pii_street_list)
            missing_pii = _compare_address_lists(full_pii_street_list, full_rdr_street_list)

            if (missing_rdr + missing_pii) > 0:
                _record_match_value(match_values, person_id, consts.STREET_ONE, consts.MISMATCH)
                _record_match_value(match_values, person_id, consts.STREET_TWO, consts.MISMATCH)
            else:
                _record_match_value(match_values, person_id, consts.STREET_ONE, consts.MATCH)
                _record_match_value(match_values, person_id, consts.STREET_TWO, consts.MATCH)

        rdr_city = _clean_name(cities.get(person_id))
        city = _clean_name(city)
        match_str = consts.MATCH if rdr_city == city else consts.MISMATCH
        _record_match_value(match_values, person_id, consts.CITY, match_str)

        rdr_state = _clean_state(states.get(person_id))
        state = _clean_state(state)
        match_str = consts.MATCH if rdr_state == state else consts.MISMATCH
        _record_match_value(match_values, person_id, consts.STATE, match_str)

        rdr_zip = _clean_zip(zips.get(person_id))
        zip_code = _clean_zip(zip_code)
        match_str = consts.MATCH if rdr_zip == zip_code else consts.MISMATCH
        _record_match_value(match_values, person_id, consts.ZIP, match_str)

    return person_ids, match_values

def _compare_birth_dates(person_id_set, rdr_birthdates, ehr_birthdates, match_values):
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
    :param match_values: current result set dictionary to update with new
        match information
    :return: updated match_values dictionary
    """
    # compare birth_datetime from observation to observation and record results.
    for person_id in person_id_set:
        rdr_birthdate = rdr_birthdates.get(person_id)
        ehr_birthdate = ehr_birthdates.get(person_id)

        if (rdr_birthdate is None and ehr_birthdate is not None) \
            or (ehr_birthdate is None and rdr_birthdate is not None):
            _record_match_value(match_values, person_id, consts.BIRTHDATE, consts.MISMATCH)
            return match_values
        elif rdr_birthdate is None and ehr_birthdate is None:
            _record_match_value(match_values, person_id, consts.BIRTHDATE, consts.MATCH)
        elif isinstance(rdr_birthdate, str) and isinstance(ehr_birthdate, str):
           # convert values to datetime objects
            rdr_date = parse(rdr_birthdate)
            ehr_date = parse(ehr_birthdate)
            # convert datetime objects to Year/month/day strings and compare
            rdr_string = rdr_date.strftime(consts.DATE)
            ehr_string = ehr_date.strftime(consts.DATE)

            match_str = consts.MATCH if rdr_string == ehr_string else consts.MISMATCH
            _record_match_value(match_values, person_id, consts.BIRTHDATE, match_str)
        else:
            _record_match_value(match_values, person_id, consts.BIRTHDATE, consts.MISMATCH)

    return match_values

def _validate_hpo_pii(hpo, rdr_values, match_values):
    """
    For an hpo, validate the information submitted from their PII tables.

    Orders the comparisons.

    :param hpo: hpo site string
    :param rdr_values: dictionary of values pulled from OMOP tables
    :param match_values: current result set dictionary to update with new
        match information
    :return: updated match_values dictionary
    """
    person_ids = set()

    first_names = rdr_values.get(consts.OBS_PII_NAME_FIRST)
    middle_names = rdr_values.get(consts.OBS_PII_NAME_MIDDLE)
    last_names = rdr_values.get(consts.OBS_PII_NAME_LAST)
    person_id_set, match_values = _compare_name_fields(hpo, first_names, middle_names, last_names, match_values)
    person_ids.update(person_id_set)

    email_addresses = rdr_values.get(consts.OBS_PII_EMAIL_ADDRESS)
    person_id_set, match_values = _compare_email_addresses(hpo, email_addresses, match_values)
    person_ids.update(person_id_set)

    phone_numbers = rdr_values.get(consts.OBS_PII_PHONE)
    person_id_set, match_values = _compare_phone_numbers(hpo, phone_numbers, match_values)
    person_ids.update(person_id_set)

    streets_one = rdr_values.get(consts.OBS_PII_STREET_ADDRESS_ONE)
    streets_two = rdr_values.get(consts.OBS_PII_STREET_ADDRESS_TWO)
    cities = rdr_values.get(consts.OBS_PII_STREET_ADDRESS_CITY)
    states = rdr_values.get(consts.OBS_PII_STREET_ADDRESS_STATE)
    zips = rdr_values.get(consts.OBS_PII_STREET_ADDRESS_ZIP)
    person_id_set, match_values = _compare_address_fields(hpo, streets_one, streets_two, cities, states, zips, match_values)
    person_ids.update(person_id_set)


    rdr_birthdate = rdr_values.get(consts.OBS_PII_BIRTH_DATETIME)
    ehr_birthdate = rdr_values.get(consts.OBS_EHR_BIRTH_DATETIME)
    match_values = _compare_birth_dates(person_ids, rdr_birthdate, ehr_birthdate, match_values)

    return match_values


def match_participants(dataset):
    """
    Entry point for performing participant matching of PPI, EHR, and PII data.

    :param dataset:  The string name of the dataset to use when matching participants
    :return: results of the field comparison for each hpo
    """
    #TODO:  implement stubs to get data from BigQuery
    rdr_values = _get_observation_match_values(dataset)

    # TODO: get list of hpos.  what is the easiest way to get this data?  Read BigQuery tables
    # or do we maintain a list somewhere?
    hpo_sites = _get_hpo_site_names()

    results = {}
    for site in hpo_sites:
        match_values = _validate_hpo_pii(site, rdr_values, {})
        results.update(match_values)
        # TODO:  generate hpo site report

    # TODO:  generate aggregate site report

    return results


if __name__ == '__main__':
    DATASET = 'combined20190211'
    match_participants(DATASET)
