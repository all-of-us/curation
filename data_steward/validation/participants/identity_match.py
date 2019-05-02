#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
A module to perform initial participant identity matching based on PII data.

Compares site PII data to values from the RDR, looking to identify discrepancies.
"""
# Python imports
from datetime import datetime
import json
import logging
import os
import re

# Third party imports
from dateutil.parser import parse

# Project imports
import bq_utils
import gcs_utils
import constants.bq_utils as bq_consts
import constants.validation.participants.identity_match as consts
import validation.participants.normalizers as normalizer
import validation.participants.readers as readers
import validation.participants.writers as writers

LOGGER = logging.getLogger(__name__)


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


def _compare_name_fields(
        project,
        rdr_dataset,
        pii_dataset,
        hpo,
        concept_id,
        pii_field
    ):
    """
    For an hpo, compare all first, middle, and last name fields to omop settings.

    This compares a site's name field values found in their uploaded PII
    tables with the values in the OMOP observation table.

    :param hpo: string name of hop to search.
    :return: a match_values dictionary.
    """
    match_values = {}

    rdr_names = readers.get_rdr_match_values(
        project, rdr_dataset, consts.ID_MATCH_TABLE, concept_id
    )

    pii_names = readers.get_pii_values(
        project,
        pii_dataset,
        hpo,
        consts.PII_NAME_TABLE,
        pii_field
    )

    for person_id, pii_name in pii_names:
        rdr_name = rdr_names.get(person_id)

        if rdr_name is None or pii_name is None:
            match_str = consts.MISSING
        else:
            pii_name = normalizer.normalize_name(pii_name)
            rdr_name = normalizer.normalize_name(rdr_name)
            match_str = consts.MATCH if rdr_name == pii_name else consts.MISMATCH

        match_values[person_id] = match_str

    return match_values


def _compare_email_addresses(
        project,
        rdr_dataset,
        pii_dataset,
        hpo,
        concept_id,
        pii_field
    ):
    """
    Compare email addresses from hpo PII table and OMOP observation table.

    :param hpo:  hpo site name.  used to query the correct pii table.
    :param email_addresses: dictionary of email addresses where person_id is
        the key and email address is the value
    :return: a match_value dictionary.
    """
    match_values = {}

    email_addresses = readers.get_rdr_match_values(
        project, rdr_dataset, consts.ID_MATCH_TABLE, concept_id
    )

    pii_emails = readers.get_pii_values(
        project,
        pii_dataset,
        hpo,
        consts.PII_EMAIL_TABLE,
        pii_field
    )

    for person_id, pii_email in pii_emails:
        rdr_email = email_addresses.get(person_id)

        if rdr_email is None or pii_email is None:
            match_str = consts.MISSING
        else:
            rdr_email = normalizer.normalize_email(rdr_email)
            pii_email = normalizer.normalize_email(pii_email)
            match_str = consts.MATCH if rdr_email == pii_email else consts.MISMATCH

        match_values[person_id] = match_str

    return match_values


def _compare_phone_numbers(
        project,
        rdr_dataset,
        pii_dataset,
        hpo,
        concept_id,
        pii_field
    ):
    """
    Compare the digit based phone numbers from PII and Observation tables.

    :param hpo:  hpo site name
    :param phone_numbers:  dictionary of rdr phone numbers where person_id is
        the key and phone numbers are values
    :return: A match_values dictionary.
    """
    match_values = {}

    phone_numbers = readers.get_rdr_match_values(
        project, rdr_dataset, consts.ID_MATCH_TABLE, concept_id
    )

    pii_phone_numbers = readers.get_pii_values(
        project,
        pii_dataset,
        hpo,
        consts.PII_PHONE_TABLE,
        pii_field
    )

    for person_id, pii_number in pii_phone_numbers:
        rdr_phone = phone_numbers.get(person_id)

        if rdr_phone is None or pii_number is None:
            match_str = consts.MISSING
        else:
            rdr_phone = normalizer.normalize_phone(rdr_phone)
            pii_number = normalizer.normalize_phone(pii_number)
            match_str = consts.MATCH if rdr_phone == pii_number else consts.MISMATCH

        match_values[person_id] = match_str

    return match_values


def _compare_cities(
        project,
        validation_dataset,
        rdr_dataset,
        pii_dataset,
        hpo,
        concept_id,
        pii_field
    ):
    """
    Compare email addresses from hpo PII table and OMOP observation table.

    :param hpo:  hpo site name.  used to query the correct pii table.
    :param email_addresses: dictionary of email addresses where person_id is
        the key and email address is the value
    :return: a match_value dictionary.
    """
    match_values = {}

    cities = readers.get_rdr_match_values(
        project, validation_dataset, consts.ID_MATCH_TABLE, concept_id
    )

    pii_cities = readers.get_location_pii(
        project,
        rdr_dataset,
        pii_dataset,
        hpo,
        consts.PII_ADDRESS_TABLE,
        pii_field
    )

    for person_id, pii_city in pii_cities:
        rdr_city = cities.get(person_id)

        if rdr_city is None or pii_city is None:
            match_str = consts.MISSING
        else:
            rdr_city = normalizer.normalize_city_name(rdr_city)
            pii_city = normalizer.normalize_city_name(pii_city)
            match_str = consts.MATCH if rdr_city == pii_city else consts.MISMATCH

        match_values[person_id] = match_str

    return match_values


def _compare_states(
        project,
        validation_dataset,
        rdr_dataset,
        pii_dataset,
        hpo,
        concept_id,
        pii_field
    ):
    """
    Compare email addresses from hpo PII table and OMOP observation table.

    :param hpo:  hpo site name.  used to query the correct pii table.
    :param email_addresses: dictionary of email addresses where person_id is
        the key and email address is the value
    :return: a match_value dictionary.
    """
    match_values = {}

    states = readers.get_rdr_match_values(
        project, validation_dataset, consts.ID_MATCH_TABLE, concept_id
    )

    pii_states = readers.get_location_pii(
        project,
        rdr_dataset,
        pii_dataset,
        hpo,
        consts.PII_ADDRESS_TABLE,
        pii_field
    )

    for person_id, pii_state in pii_states:
        rdr_state = states.get(person_id)

        if rdr_state is None or pii_state is None:
            match_str = consts.MISSING
        else:
            rdr_state = normalizer.normalize_state(rdr_state)
            pii_state = normalizer.normalize_state(pii_state)
            match_str = consts.MATCH if rdr_state == pii_state else consts.MISMATCH

        match_values[person_id] = match_str

    return match_values


def _compare_zip_codes(
        project,
        validation_dataset,
        rdr_dataset,
        pii_dataset,
        hpo,
        concept_id,
        pii_field
    ):
    """
    Compare email addresses from hpo PII table and OMOP observation table.

    :param hpo:  hpo site name.  used to query the correct pii table.
    :param email_addresses: dictionary of email addresses where person_id is
        the key and email address is the value
    :return: a match_value dictionary.
    """
    match_values = {}

    zip_codes = readers.get_rdr_match_values(
        project, validation_dataset, consts.ID_MATCH_TABLE, concept_id
    )

    pii_zip_codes = readers.get_location_pii(
        project,
        rdr_dataset,
        pii_dataset,
        hpo,
        consts.PII_ADDRESS_TABLE,
        pii_field
    )

    for person_id, pii_zip_code in pii_zip_codes:
        rdr_zip = zip_codes.get(person_id)

        if rdr_zip is None or pii_zip_code is None:
            match_str = consts.MISSING
        else:
            rdr_zip = normalizer.normalize_zip(rdr_zip)
            pii_zip = normalizer.normalize_zip(pii_zip_code)
            match_str = consts.MATCH if rdr_zip == pii_zip else consts.MISMATCH

        match_values[person_id] = match_str

    return match_values


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
    :return: a match_values dictionary.
    """
    address_one_match_values = {}
    address_two_match_values = {}

    rdr_address_ones = readers.get_rdr_match_values(
        project, validation_dataset, consts.ID_MATCH_TABLE, concept_id_one
    )

    rdr_address_twos = readers.get_rdr_match_values(
        project, validation_dataset, consts.ID_MATCH_TABLE, concept_id_two
    )

    pii_street_ones = readers.get_location_pii(
        project, rdr_dataset, pii_dataset, hpo, consts.PII_ADDRESS_TABLE, field_one
    )
    pii_street_twos = readers.get_location_pii(
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

        pii_addr_one = addresses[1]
        pii_addr_two = addresses[2]

        rdr_addr_one = normalizer.normalize_street(rdr_address_ones.get(person_id))
        pii_addr_one = normalizer.normalize_street(pii_addr_one)
        rdr_addr_two = normalizer.normalize_street(rdr_address_twos.get(person_id))
        pii_addr_two = normalizer.normalize_street(pii_addr_two)

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

    return address_one_match_values, address_two_match_values


def _compare_genders(
        project,
        validation_dataset,
        ehr_dataset,
        site,
        concept_id_pii
    ):
    """
    Compare genders for people.

    Converts birthdates and birth_datetimes to calendar objects.  Converts
    the calendar objects back to strings with the same format and compares
    these strings.

    :param rdr_birthdates: dictionary of rdr birth datetimes where person_id is
        the key and a birth datetime string is the value
    :param ehr_birthdates: dictionary of birth datetimes where person_id is
        the key and a birth datetime string is the value
    :return: updated match_values dictionary
    """
    match_values = {}

    pii_genders = readers.get_rdr_match_values(
        project, validation_dataset, consts.ID_MATCH_TABLE, concept_id_pii
    )

    ehr_genders = readers.get_ehr_person_values(
        project,
        ehr_dataset,
        site + consts.EHR_PERSON_TABLE_SUFFIX,
        consts.GENDER_FIELD
    )

    # compare gender from ppi info to ehr info and record results.
    for person_id, ehr_gender in ehr_genders.iteritems():
        rdr_gender = pii_genders.get(person_id)
        ehr_gender = consts.SEX_CONCEPT_IDS.get(ehr_gender, '')

        rdr_gender = rdr_gender.lower()
        ehr_gender = ehr_gender.lower()

        if rdr_gender is None or ehr_gender is None:
            match_str = consts.MISSING
        else:
            match_str = consts.MATCH if rdr_gender == ehr_gender else consts.MISMATCH

        match_values[person_id] = match_str

    return match_values


def _compare_birth_dates(
        project,
        validation_dataset,
        ehr_dataset,
        site,
        concept_id_pii
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

    pii_birthdates = readers.get_rdr_match_values(
        project, validation_dataset, consts.ID_MATCH_TABLE, concept_id_pii
    )

    ehr_birthdates = readers.get_ehr_person_values(
        project,
        ehr_dataset,
        site + consts.EHR_PERSON_TABLE_SUFFIX,
        consts.BIRTH_DATETIME_FIELD
    )

    # compare birth_datetime from ppi info to ehr info and record results.
    for person_id, ehr_birthdate in ehr_birthdates.iteritems():
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


def _get_date_string(dataset):
    date_string = dataset[-8:]

    if re.match(consts.DRC_DATE_REGEX, date_string):
        return date_string

    return datetime.now().strftime(consts.DRC_DATE_FORMAT)


def match_participants(project, rdr_dataset, ehr_dataset, dest_dataset_id):
    """
    Entry point for performing participant matching of PPI, EHR, and PII data.

    :param project: a string representing the project name

    :return: results of the field comparison for each hpo
    """
    date_string = _get_date_string(rdr_dataset)

    if not re.match(consts.DRC_DATE_REGEX, dest_dataset_id[-8:]):
        dest_dataset_id += date_string

    # create new dataset for the intermediate tables and results
    dataset_result = bq_utils.create_dataset(
        dataset_id=dest_dataset_id,
        description=consts.DESTINATION_DATASET_DESCRIPTION.format(
            version='',
            rdr_dataset=rdr_dataset,
            ehr_dataset=ehr_dataset
        ),
        overwrite_existing=True)

    validation_dataset = dataset_result.get(bq_consts.DATASET_REF, {})
    validation_dataset = validation_dataset.get(bq_consts.DATASET_ID, '')

    # create intermediate observation table in new dataset
    readers.create_match_values_table(project, rdr_dataset, dest_dataset_id)

    hpo_sites = readers.get_hpo_site_names()

    field_list = []
    #TODO:  create a proper config file to store this path
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

    # validate first names
    for site in hpo_sites:
        match_values = _compare_name_fields(
            project,
            validation_dataset,
            ehr_dataset,
            site,
            consts.OBS_PII_NAME_FIRST,
            consts.FIRST_NAME_FIELD
        )

        writers.append_to_result_table(
            site,
            match_values,
            project,
            validation_dataset,
            consts.FIRST_NAME_FIELD
        )

    # validate last names
    for site in hpo_sites:
        match_values = _compare_name_fields(
            project,
            validation_dataset,
            ehr_dataset,
            site,
            consts.OBS_PII_NAME_LAST,
            consts.LAST_NAME_FIELD
        )
        # write last name matches for hpo to table
        writers.append_to_result_table(
            site,
            match_values,
            project,
            validation_dataset,
            consts.LAST_NAME_FIELD
        )

    # validate middle names
    for site in hpo_sites:
        match_values = _compare_name_fields(
            project,
            validation_dataset,
            ehr_dataset,
            site,
            consts.OBS_PII_NAME_MIDDLE,
            consts.MIDDLE_NAME_FIELD
        )
        # write middle name matches for hpo to table
        writers.append_to_result_table(
            site,
            match_values,
            project,
            validation_dataset,
            consts.MIDDLE_NAME_FIELD
        )

    # validate zip codes
    for site in hpo_sites:
        match_values = _compare_zip_codes(
            project,
            validation_dataset,
            rdr_dataset,
            ehr_dataset,
            site,
            consts.OBS_PII_STREET_ADDRESS_ZIP,
            consts.ZIP_CODE_FIELD
        )
        # write zip codes matces for hpo to table
        writers.append_to_result_table(
            site,
            match_values,
            project,
            validation_dataset,
            consts.ZIP_CODE_FIELD
        )

    # validate city
    for site in hpo_sites:
        match_values = _compare_cities(
            project,
            validation_dataset,
            rdr_dataset,
            ehr_dataset,
            site,
            consts.OBS_PII_STREET_ADDRESS_CITY,
            consts.CITY_FIELD
        )
        # write city matches for hpo to table
        writers.append_to_result_table(
            site,
            match_values,
            project,
            validation_dataset,
            consts.CITY_FIELD
        )

    # validate state
    for site in hpo_sites:
        match_values = _compare_states(
            project,
            validation_dataset,
            rdr_dataset,
            ehr_dataset,
            site,
            consts.OBS_PII_STREET_ADDRESS_STATE,
            consts.STATE_FIELD
        )
        # write state matches for hpo to table
        writers.append_to_result_table(
            site,
            match_values,
            project,
            validation_dataset,
            consts.STATE_FIELD
        )

    # validate street addresses
    for site in hpo_sites:
        address_one_matches, address_two_matches = _compare_street_addresses(
            project,
            validation_dataset,
            rdr_dataset,
            ehr_dataset,
            site,
            consts.OBS_PII_STREET_ADDRESS_ONE,
            consts.OBS_PII_STREET_ADDRESS_TWO,
            consts.ADDRESS_ONE_FIELD,
            consts.ADDRESS_TWO_FIELD
        )
        # write street address matches for hpo to table
        writers.append_to_result_table(
            site,
            address_one_matches,
            project,
            validation_dataset,
            consts.ADDRESS_ONE_FIELD
        )
        writers.append_to_result_table(
            site,
            address_two_matches,
            project,
            validation_dataset,
            consts.ADDRESS_TWO_FIELD
        )

    # validate email addresses
    for site in hpo_sites:
        match_values = _compare_email_addresses(
            project,
            validation_dataset,
            ehr_dataset,
            site,
            consts.OBS_PII_EMAIL_ADDRESS,
            consts.EMAIL_FIELD
        )
        # write email matches for hpo to table
        writers.append_to_result_table(
            site,
            match_values,
            project,
            validation_dataset,
            consts.EMAIL_FIELD
        )

    # validate phone numbers
    for site in hpo_sites:
        match_values = _compare_phone_numbers(
            project,
            validation_dataset,
            ehr_dataset,
            site,
            consts.OBS_PII_PHONE,
            consts.PHONE_NUMBER_FIELD
        )
        # write phone number matches for hpo to table
        writers.append_to_result_table(
            site,
            match_values,
            project,
            validation_dataset,
            consts.PHONE_NUMBER_FIELD
        )

    # validate genders
    for site in hpo_sites:
        match_values = _compare_genders(
            project,
            validation_dataset,
            ehr_dataset,
            site,
            consts.OBS_PII_SEX
        )
        # write birthday match for hpo to table
        writers.append_to_result_table(
            site,
            match_values,
            project,
            validation_dataset,
            consts.SEX_FIELD
        )

    # validate birth dates
    for site in hpo_sites:
        match_values = _compare_birth_dates(
            project,
            validation_dataset,
            ehr_dataset,
            site,
            consts.OBS_PII_BIRTH_DATETIME
        )
        # write birthday match for hpo to table
        writers.append_to_result_table(
            site,
            match_values,
            project,
            validation_dataset,
            consts.BIRTH_DATE_FIELD
        )

    # generate single clean record for each participant at each site
    for site in hpo_sites:
        writers.merge_fields_into_single_record(project, validation_dataset, site)
        writers.remove_sparse_records(project, validation_dataset, site)
        writers.change_nulls_to_missing_value(project, validation_dataset, site)

    # generate hpo site reports
    for site in hpo_sites:
        bucket = gcs_utils.get_hpo_bucket(site)
        filename = os.path.join(
            consts.REPORT_DIRECTORY.format(date=date_string),
            consts.REPORT_TITLE
        )
        writers.create_site_validation_report(
            project, validation_dataset, [site], bucket, filename
        )

    # generate aggregate site report
    bucket = gcs_utils.get_drc_bucket()
    filename = os.path.join(validation_dataset, consts.REPORT_TITLE)
    writers.create_site_validation_report(project, validation_dataset, hpo_sites, bucket, filename)

    return results


if __name__ == '__main__':
    RDR_DATASET = 'lrwb_combined20190415'
    PII_DATASET = 'lrwb_pii_tables'
    PROJECT = 'aou-res-curation-test'
    DEST_DATASET_ID = 'temp_dataset_id'
    match_participants(PROJECT, RDR_DATASET, PII_DATASET, DEST_DATASET_ID)
