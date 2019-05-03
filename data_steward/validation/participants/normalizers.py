#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
A module to perform various normalizing functions for participant validation data.
"""
# Python imports
import logging

# Third party imports

# Project imports
import constants.validation.participants.normalizers as consts

LOGGER = logging.getLogger(__name__)


def normalize_city_name(city):
    """
    Helper function to return names with lowercase alphabetic characters only.

    :param city:  string to normalize.
    :return:  a string with everything that is not an alphabetic character
        removed and all characters are lower cased.
    """
    if city is None:
        return ''
    elif not isinstance(city, str):
        city = str(city)

    normalized_city = ''
    city = city.lower()
    for char in city:
        if char.isalnum() or char.isspace():
            normalized_city += char

    for part in normalized_city.split():
        expansion = consts.CITY_ABBREVIATIONS.get(part)
        if expansion:
            normalized_city = normalized_city.replace(part, expansion)

    normalized_city = ' '.join(normalized_city.split())
    return normalized_city


def _get_numeric_part_only(part):
    """
    Clean common alphabetic endings from numbers.

    This removes commonly used st, th, nd, and rd endings that
    may follow digits.

    :param part:  The string to remove st, th, nd, and rd endings from, if
        they exist following digits.

    :return:  The digits only, or None.
    """
    match = consts.COMPILED_NUMERIC_ENDINGS_REGEX.match(part)

    if match:
        return part[0:-2]

    return None


def _get_alpha_numeric_parts(part):
    """
    return the split digit and numeric components as a single string.

    :param part:  The string component being checked for digits followed by
        alphabetic characters.

    :return:  The string digits and alphabetic characters split by a space, if
        they exist, or None.
    """
    match = consts.COMPILED_ALPHA_NUMERIC.match(part)

    if match:
        digits = ''
        alphas = ''
        for char in part:
            if char.isalpha():
                alphas += char
            elif char.isdigit():
                digits += char
        return ' '.join([digits, alphas])

    return None


def normalize_street(street):
    """
    Helper function to return normalized street addresses.

    :param street:  string to normalize.
    :return:  a normalized alphanumeric string with all alphabetic characters
        lower cased, leading and trailing white space stripped, abbreviations
        expanded, and punctuation removed or an empty string.
    """
    if street is None:
        return ''
    elif not isinstance(street, str):
        street = str(street)

    normalized_street = ''
    street = street.lower()
    # replace all punctuation with a space
    for char in street:
        if char.isalnum():
            normalized_street += char
        else:
            normalized_street += ' '

    # for each part of the address, see if it exists in the list of known
    # abbreviations.  if so, expand the abbreviation
    for part in normalized_street.split():
        expansion = consts.ADDRESS_ABBREVIATIONS.get(part)
        # expand recognized abbreviations
        if expansion:
            normalized_street = normalized_street.replace(part, expansion)
            part = expansion

        # normalize 7 and 7th as the same
        number = _get_numeric_part_only(part)
        if number:
            normalized_street = normalized_street.replace(part, number)
            part = number

        # normalize 50A and 50 A as the same
        alpha_num = _get_alpha_numeric_parts(part)
        if alpha_num:
            normalized_street = normalized_street.replace(part, alpha_num)
            part = alpha_num

    # removes possible multiple spaces.
    normalized_street = ' '.join(normalized_street.split())
    return normalized_street


def normalize_state(state):
    """
    Helper function to return state abbreviations with lowercase characters and no whitespace.

    Verifies the state code is a two character state, commonwealth, territory,
    or military state code and returns the code lower cased.  If the code is
    not a valid abbreviation, an empty string is returned.

    :param state:  string to normalize.
    :return:  a two character string with all alphabetic characters lower cased
        and all whitespace removed or empty string.
    """
    if state is None:
        return ''
    elif not isinstance(state, str):
        state = str(state)

    normalized_state = state.strip()
    normalized_state = normalized_state.lower()
    return normalized_state if normalized_state in consts.STATE_ABBREVIATIONS else ''


def normalize_zip(code):
    """
    Helper function to return 5 character zip codes only.

    :param code:  string to normalize and format as a zip code
    :return: a five character digit string to compare as a zip code
    """
    if code is None:
        return ''
    elif not isinstance(code, str):
        code = str(code)

    normalized_code = ''
    code = code.strip()

    # ensure hyphenated part is ignored
    code = code.split('-')[0]
    code = code.split(' ')[0]

    # perform zero padding upto 5 chars
    code = code.zfill(5)

    for char in code:
        if char.isdigit():
            normalized_code += char

    return normalized_code


def normalize_phone(number):
    """
    Helper function to return only character digits.

    :param number:  string to normalize.
    :return:  a string with everything that is not a digit removed.
    """
    if number is None:
        return ''
    elif not isinstance(number, str):
        number = str(number)

    normalized_number = ''
    for char in number:
        if char.isdigit():
            normalized_number += char
    return normalized_number


def normalize_email(email):
    """
    Helper function to return emails with lowercase characters and no whitespace.

    :param email:  string to normalize.
    :return:  a string with all alphabetic characters lower cased and all
        whitespace removed.
    """
    if email is None:
        return ''
    elif not isinstance(email, str):
        email = str(email)

    normalized_email = email.strip()
    return normalized_email.lower() if consts.AT in normalized_email else ''


def normalize_name(name):
    """
    Helper function to return names with lowercase alphabetic characters only.

    :param name:  string to normalize.
    :return:  a string with everything that is not an alphabetic character
        removed and all characters are lower cased.
    """
    if name is None:
        return ''
    elif not isinstance(name, str):
        name = str(name)

    normalized_name = ''
    for char in name:
        if char.isalpha():
            normalized_name += char
    return normalized_name.lower()
