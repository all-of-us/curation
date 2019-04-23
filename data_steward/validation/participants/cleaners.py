#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
A module to perform various normalizing functions for participant validation data.
"""
# Python imports
import logging

# Third party imports

# Project imports
import constants.validation.participants.identity_match as consts

LOGGER = logging.getLogger(__name__)


def clean_street(street):
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

    cleaned_street = ''
    street = street.lower()
    street = street.replace('#', ' # ')  # ensure hash is found by itself
    # replace all punctuation with a space, excpet for # which is sometimes used
    # for number
    for char in street:
        if char.isalnum() or char == '#':
            cleaned_street += char
        else:
            cleaned_street += ' '

    # for each part of the address, see if it exists in the list of known
    # abbreviations.  if so, expand the abbreviation
    # TODO ensure 50A and 50 A are recognized as the same
    for part in cleaned_street.split():
        expansion = consts.ADDRESS_ABBREVIATIONS.get(part)
        if expansion:
            cleaned_street = cleaned_street.replace(part, expansion)

    # removes possible multiple spaces.
    cleaned_street = ' '.join(cleaned_street.split())
    return cleaned_street


def clean_state(state):
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

    cleaned_state = state.strip()
    cleaned_state = cleaned_state.lower()
    return cleaned_state if cleaned_state in consts.STATE_ABBREVIATIONS else ''


def clean_zip(code):
    """
    Helper function to return 5 character zip codes only.

    :param code:  string to clean and format as a zip code
    :return: a five character digit string to compare as a zip code
    """
    if code is None:
        return ''
    elif not isinstance(code, str):
        code = str(code)

    cleaned_code = ''
    code = code.strip()

    # ensure hyphenated part is ignored
    code = code.split('-')[0]
    code = code.split(' ')[0]

    # perform zero padding upto 5 chars
    code = code.zfill(5)

    for char in code:
        if char.isdigit():
            cleaned_code += char

    return cleaned_code


def clean_phone(number):
    """
    Helper function to return only character digits.

    :param number:  string to clean.
    :return:  a string with everything that is not a digit removed.
    """
    if number is None:
        return ''
    elif not isinstance(number, str):
        number = str(number)

    cleaned_number = ''
    for char in number:
        if char.isdigit():
            cleaned_number += char
    return cleaned_number


def clean_email(email):
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

    cleaned_email = email.strip()
    return cleaned_email.lower()


def clean_name(name):
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

    cleaned_name = ''
    for char in name:
        if char.isalpha():
            cleaned_name += char
    return cleaned_name.lower()
