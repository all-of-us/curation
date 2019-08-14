#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
A module to perform initial participant identity matching based on PII data.

Compares site PII data to values from the RDR, looking to identify discrepancies.
"""
# Python imports
from datetime import datetime
import logging
import os
import re

# Third party imports
from dateutil.parser import parse
import googleapiclient
import oauth2client

# Project imports
import bq_utils
import gcs_utils
import constants.bq_utils as bq_consts
import constants.validation.participants.identity_match as consts
import constants.validation.participants.writers as writer_consts
import resources
import validation.participants.normalizers as normalizer
import validation.participants.readers as readers
import validation.participants.writers as writers

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s')
LOGGER = logging.getLogger(__name__)
# LOGGER.setLevel(logging.DEBUG)


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

    :param project:  project to search for the datasets
    :param rdr_dataset:  contains datasets from the rdr group
    :param pii_dataset:  dataset created from submitted hpo sites.  the pii tables
    :param hpo: string identifier of hpo
    :param concept_id:  integer value of concept id for concept in the rdr_dataset
    :param pii_field:  string value of field name with data matching the
        concept_id.  used to extract the correct values from the pii tables

    :return: a match_values dictionary.
    """
    match_values = {}

    rdr_names = readers.get_rdr_match_values(
        project, rdr_dataset, consts.ID_MATCH_TABLE, concept_id
    )

    try:
        pii_names = readers.get_pii_values(
            project,
            pii_dataset,
            hpo,
            consts.PII_NAME_TABLE,
            pii_field
        )
    except (oauth2client.client.HttpAccessTokenRefreshError,
            googleapiclient.errors.HttpError) as exception:
        LOGGER.exception(
            "Unable to read PII for: %s\tdata field:\t%s", hpo, pii_field
        )
        return match_values, exception

    for person_id, pii_name in pii_names:
        rdr_name = rdr_names.get(person_id)

        if rdr_name is None or pii_name is None:
            match_str = consts.MISSING
        else:
            pii_name = normalizer.normalize_name(pii_name)
            rdr_name = normalizer.normalize_name(rdr_name)
            match_str = consts.MATCH if rdr_name == pii_name else consts.MISMATCH

        match_values[person_id] = match_str

    return match_values, None


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

    :param project:  project to search for the datasets
    :param rdr_dataset:  contains datasets from the rdr group
    :param pii_dataset:  dataset created from submitted hpo sites.  the pii tables
    :param hpo: string identifier of hpo
    :param concept_id:  integer value of concept id for concept in the rdr_dataset
    :param pii_field:  string value of field name with data matching the
        concept_id.  used to extract the correct values from the pii tables

    :return: a match_value dictionary.
    """
    match_values = {}

    email_addresses = readers.get_rdr_match_values(
        project, rdr_dataset, consts.ID_MATCH_TABLE, concept_id
    )

    try:
        pii_emails = readers.get_pii_values(
            project,
            pii_dataset,
            hpo,
            consts.PII_EMAIL_TABLE,
            pii_field
        )
    except (oauth2client.client.HttpAccessTokenRefreshError,
            googleapiclient.errors.HttpError) as exception:
        LOGGER.exception(
            "Unable to read PII for: %s\tdata field:\t%s", hpo, pii_field
        )
        return match_values, exception

    for person_id, pii_email in pii_emails:
        rdr_email = email_addresses.get(person_id)

        if rdr_email is None or pii_email is None:
            match_str = consts.MISSING
        else:
            rdr_email = normalizer.normalize_email(rdr_email)
            pii_email = normalizer.normalize_email(pii_email)
            match_str = consts.MATCH if rdr_email == pii_email else consts.MISMATCH

        match_values[person_id] = match_str

    return match_values, None


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

    :param project:  project to search for the datasets
    :param rdr_dataset:  contains datasets from the rdr group
    :param pii_dataset:  dataset created from submitted hpo sites.  the pii tables
    :param hpo: string identifier of hpo
    :param concept_id:  integer value of concept id for concept in the rdr_dataset
    :param pii_field:  string value of field name with data matching the
        concept_id.  used to extract the correct values from the pii tables

    :return: A match_values dictionary.
    """
    match_values = {}

    phone_numbers = readers.get_rdr_match_values(
        project, rdr_dataset, consts.ID_MATCH_TABLE, concept_id
    )

    try:
        pii_phone_numbers = readers.get_pii_values(
            project,
            pii_dataset,
            hpo,
            consts.PII_PHONE_TABLE,
            pii_field
        )
    except (oauth2client.client.HttpAccessTokenRefreshError,
            googleapiclient.errors.HttpError) as exception:
        LOGGER.exception(
            "Unable to read PII for: %s\tdata field:\t%s", hpo, pii_field
        )
        return match_values, exception

    for person_id, pii_number in pii_phone_numbers:
        rdr_phone = phone_numbers.get(person_id)

        if rdr_phone is None or pii_number is None:
            match_str = consts.MISSING
        else:
            rdr_phone = normalizer.normalize_phone(rdr_phone)
            pii_number = normalizer.normalize_phone(pii_number)
            match_str = consts.MATCH if rdr_phone == pii_number else consts.MISMATCH

        match_values[person_id] = match_str

    return match_values, None


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
    Compare city information from hpo PII table and OMOP observation table.

    :param project:  project to search for the datasets
    :param validation_dataset:  the auto generated match validation dataset
        created in this module.  queried to get the location value to identify
        a location field
    :param rdr_dataset:  contains datasets from the rdr group
    :param pii_dataset:  dataset created from submitted hpo sites.  the pii tables
    :param hpo: string identifier of hpo
    :param concept_id:  integer value of concept id for concept in the rdr_dataset
    :param pii_field:  string value of field name with data matching the
        concept_id.  used to extract the correct values from the pii tables

    :return: a match_value dictionary.
    """
    match_values = {}

    cities = readers.get_rdr_match_values(
        project, validation_dataset, consts.ID_MATCH_TABLE, concept_id
    )

    try:
        pii_cities = readers.get_location_pii(
            project,
            rdr_dataset,
            pii_dataset,
            hpo,
            consts.PII_ADDRESS_TABLE,
            pii_field
        )
    except (oauth2client.client.HttpAccessTokenRefreshError,
            googleapiclient.errors.HttpError) as exception:
        LOGGER.exception(
            "Unable to read PII for: %s\tdata field:\t%s", hpo, pii_field
        )
        return match_values, exception

    for person_id, pii_city in pii_cities:
        rdr_city = cities.get(person_id)

        if rdr_city is None or pii_city is None:
            match_str = consts.MISSING
        else:
            rdr_city = normalizer.normalize_city_name(rdr_city)
            pii_city = normalizer.normalize_city_name(pii_city)
            match_str = consts.MATCH if rdr_city == pii_city else consts.MISMATCH

        match_values[person_id] = match_str

    return match_values, None


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

    :param project:  project to search for the datasets
    :param validation_dataset:  the auto generated match validation dataset
        created in this module.  queried to get the location value to identify
        a location field
    :param rdr_dataset:  contains datasets from the rdr group
    :param pii_dataset:  dataset created from submitted hpo sites.  the pii tables
    :param hpo: string identifier of hpo
    :param concept_id:  integer value of concept id for concept in the rdr_dataset
    :param pii_field:  string value of field name with data matching the
        concept_id.  used to extract the correct values from the pii tables

    :return: a match_value dictionary.
    """
    match_values = {}

    states = readers.get_rdr_match_values(
        project, validation_dataset, consts.ID_MATCH_TABLE, concept_id
    )

    try:
        pii_states = readers.get_location_pii(
            project,
            rdr_dataset,
            pii_dataset,
            hpo,
            consts.PII_ADDRESS_TABLE,
            pii_field
        )
    except (oauth2client.client.HttpAccessTokenRefreshError,
            googleapiclient.errors.HttpError) as exception:
        LOGGER.exception(
            "Unable to read PII for: %s\tdata field:\t%s", hpo, pii_field
        )
        return match_values, exception

    for person_id, pii_state in pii_states:
        rdr_state = states.get(person_id)

        if rdr_state is None or pii_state is None:
            match_str = consts.MISSING
        else:
            rdr_state = normalizer.normalize_state(rdr_state)
            pii_state = normalizer.normalize_state(pii_state)
            match_str = consts.MATCH if rdr_state == pii_state else consts.MISMATCH

        match_values[person_id] = match_str

    return match_values, None


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

    :param project:  project to search for the datasets
    :param validation_dataset:  the auto generated match validation dataset
        created in this module.  queried to get the location value to identify
        a location field
    :param rdr_dataset:  contains datasets from the rdr group
    :param pii_dataset:  dataset created from submitted hpo sites.  the pii tables
    :param hpo: string identifier of hpo
    :param concept_id:  integer value of concept id for concept in the rdr_dataset
    :param pii_field:  string value of field name with data matching the
        concept_id.  used to extract the correct values from the pii tables

    :return: a match_value dictionary.
    """
    match_values = {}

    zip_codes = readers.get_rdr_match_values(
        project, validation_dataset, consts.ID_MATCH_TABLE, concept_id
    )

    try:
        pii_zip_codes = readers.get_location_pii(
            project,
            rdr_dataset,
            pii_dataset,
            hpo,
            consts.PII_ADDRESS_TABLE,
            pii_field
        )
    except (oauth2client.client.HttpAccessTokenRefreshError,
            googleapiclient.errors.HttpError) as exception:
        LOGGER.exception(
            "Unable to read PII for: %s\tdata field:\t%s", hpo, pii_field
        )
        return match_values, exception

    for person_id, pii_zip_code in pii_zip_codes:
        rdr_zip = zip_codes.get(person_id)

        if rdr_zip is None or pii_zip_code is None:
            match_str = consts.MISSING
        else:
            rdr_zip = normalizer.normalize_zip(rdr_zip)
            pii_zip = normalizer.normalize_zip(pii_zip_code)
            match_str = consts.MATCH if rdr_zip == pii_zip else consts.MISMATCH

        match_values[person_id] = match_str

    return match_values, None


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

    :param project:  project to search for the datasets
    :param validation_dataset:  the auto generated match validation dataset
        created in this module.  queried to get the location value to identify
        a location field
    :param rdr_dataset:  contains datasets from the rdr group
    :param pii_dataset:  dataset created from submitted hpo sites.  the pii tables
    :param hpo: string identifier of hpo
    :param concept_id_one:  integer value of concept id for concept in the rdr_dataset
    :param concept_id_two:  integer value of concept id for concept in the rdr_dataset
    :param field_one:  string value of field name with data matching the
        concept_id.  used to extract the correct values from the pii tables
    :param field_two:  string value of field name with data matching the
        concept_id.  used to extract the correct values from the pii tables

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

    try:
        pii_street_ones = readers.get_location_pii(
            project, rdr_dataset, pii_dataset, hpo, consts.PII_ADDRESS_TABLE, field_one
        )
    except (oauth2client.client.HttpAccessTokenRefreshError,
            googleapiclient.errors.HttpError) as exception:
        LOGGER.exception(
            "Unable to read PII for: %s\tdata field:\t%s", hpo, field_one
        )
        return address_one_match_values, address_two_match_values, exception
    try:
        pii_street_twos = readers.get_location_pii(
            project, rdr_dataset, pii_dataset, hpo, consts.PII_ADDRESS_TABLE, field_two
        )
    except (oauth2client.client.HttpAccessTokenRefreshError,
            googleapiclient.errors.HttpError) as exception:
        LOGGER.exception(
            "Unable to read PII for: %s\tdata field:\t%s", hpo, field_two
        )
        return address_one_match_values, address_two_match_values, exception

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

    return address_one_match_values, address_two_match_values, None


def _compare_genders(
        project,
        validation_dataset,
        pii_dataset,
        hpo,
        concept_id_pii
    ):
    """
    Compare genders for people.

    Converts birthdates and birth_datetimes to calendar objects.  Converts
    the calendar objects back to strings with the same format and compares
    these strings.

    :param project:  project to search for the datasets
    :param validation_dataset:  the auto generated match validation dataset
        created in this module.  queried to get the gender value
    :param pii_dataset:  dataset created from submitted hpo sites.  the pii tables
    :param hpo: string identifier of hpo
    :param concept_id_pii:  integer value of concept id for concept in the rdr_dataset

    :return: updated match_values dictionary
    """
    match_values = {}

    pii_genders = readers.get_rdr_match_values(
        project, validation_dataset, consts.ID_MATCH_TABLE, concept_id_pii
    )

    try:
        ehr_genders = readers.get_ehr_person_values(
            project,
            pii_dataset,
            hpo + consts.EHR_PERSON_TABLE_SUFFIX,
            consts.GENDER_FIELD
        )
    except (oauth2client.client.HttpAccessTokenRefreshError,
            googleapiclient.errors.HttpError) as exception:
        LOGGER.exception(
            "Unable to read PII for: %s\tdata field:\t%s", hpo, consts.GENDER_FIELD
        )
        return match_values, exception

    # compare gender from ppi info to ehr info and record results.
    for person_id, ehr_gender in ehr_genders.iteritems():
        rdr_gender = pii_genders.get(person_id, '')
        ehr_gender = consts.SEX_CONCEPT_IDS.get(ehr_gender, '')

        if rdr_gender is None or ehr_gender is None:
            match_str = consts.MISSING
        else:
            rdr_gender = rdr_gender.lower()
            ehr_gender = ehr_gender.lower()
            match_str = consts.MATCH if rdr_gender == ehr_gender else consts.MISMATCH

        match_values[person_id] = match_str

    return match_values, None


def _compare_birth_dates(
        project,
        validation_dataset,
        pii_dataset,
        site,
        concept_id_pii
    ):
    """
    Compare birth dates for people.

    Converts birthdates and birth_datetimes to calendar objects.  Converts
    the calendar objects back to strings with the same format and compares
    these strings.

    :param project:  project to search for the datasets
    :param validation_dataset:  the auto generated match validation dataset
        created in this module.  queried to get the gender value
    :param pii_dataset:  dataset created from submitted hpo sites.  the pii tables
    :param site: string identifier of hpo
    :param concept_id_pii:  integer value of concept id for concept in the rdr_dataset

    :return: updated match_values dictionary
    """
    match_values = {}

    pii_birthdates = readers.get_rdr_match_values(
        project, validation_dataset, consts.ID_MATCH_TABLE, concept_id_pii
    )

    try:
        ehr_birthdates = readers.get_ehr_person_values(
            project,
            pii_dataset,
            site + consts.EHR_PERSON_TABLE_SUFFIX,
            consts.BIRTH_DATETIME_FIELD
        )
    except (oauth2client.client.HttpAccessTokenRefreshError,
            googleapiclient.errors.HttpError) as exception:
        LOGGER.exception(
            "Unable to read PII for: %s\tdata field:\t%s", site, consts.BIRTH_DATETIME_FIELD
        )
        return match_values, exception

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

    return match_values, None


def _get_date_string(dataset):
    """
    Helper function to return an 8 digit date string.

    If the dataset parameter ends with an 8 digit string, that string is used
    as the date in YYYYMMDD format.  If not, the current date is returned in
    YYYYMMDD format.

    :param dataset:  string representing a dataset name

    :return:  an 8 digit string of the date to use
    """
    date_string = dataset[-8:]

    if re.match(consts.DRC_DATE_REGEX, date_string):
        return date_string

    return datetime.now().strftime(consts.DRC_DATE_FORMAT)


def write_results_to_site_buckets(project, validation_dataset=None):
    """
    Write the results of participant matching to each site's bucket.

    :param project: a string representing the project name
    :param validation_dataset:  the identifier for the match values
        destination dataset

    :return: None
    :raises:  RuntimeError if validation_dataset is not defined.
    """
    LOGGER.info('Writing to site buckets')
    if validation_dataset is None:
        LOGGER.error('Validation_dataset name is not defined.')
        raise RuntimeError('validation_dataset name cannot be None.')

    date_string = _get_date_string(validation_dataset)
    hpo_sites = readers.get_hpo_site_names()
    # generate hpo site reports
    for site in hpo_sites:
        bucket = gcs_utils.get_hpo_bucket(site)
        filename = os.path.join(
            consts.REPORT_DIRECTORY.format(date=date_string),
            consts.REPORT_TITLE
        )
        _, errors = writers.create_site_validation_report(
            project, validation_dataset, [site], bucket, filename
        )

        if errors > 0:
            LOGGER.error("Encountered %d read errors when writing %s site report",
                         errors,
                         site
                        )


def convert_all_match_list_to_dict(match_values_list):
    """

    :param match_values_list:
    :return:
    """
    match_values_dict = {}
    for person_id, match_list in match_values_list.iteritems():
        match_values_dict[person_id] = {}
        match_values_dict[person_id][consts.PERSON_FIELD] = person_id
        match_values_dict[person_id][consts.SITE_FIELD] = match_list[0]
        match_values_dict[person_id][consts.FIRST_NAME_FIELD] = match_list[2]
        match_values_dict[person_id][consts.LAST_NAME_FIELD] = match_list[3]
        match_values_dict[person_id][consts.MIDDLE_NAME_FIELD] = match_list[4]
        match_values_dict[person_id][consts.ZIP_CODE_FIELD] = match_list[5]
        match_values_dict[person_id][consts.CITY_FIELD] = match_list[6]
        match_values_dict[person_id][consts.STATE_FIELD] = match_list[7]
        match_values_dict[person_id][consts.ADDRESS_ONE_FIELD] = match_list[8]
        match_values_dict[person_id][consts.ADDRESS_TWO_FIELD] = match_list[9]
        match_values_dict[person_id][consts.EMAIL_FIELD] = match_list[10]
        match_values_dict[person_id][consts.PHONE_NUMBER_FIELD] = match_list[11]
        match_values_dict[person_id][consts.SEX_FIELD] = match_list[12]
        match_values_dict[person_id][consts.BIRTH_DATE_FIELD] = match_list[13]
        match_values_dict[person_id][consts.ALGORITHM_FIELD] = match_list[14]
    return match_values_dict


def write_results_to_drc_bucket(project, validation_dataset=None):
    """
    Write the results of participant matching to the drc bucket.

    :param project: a string representing the project name
    :param validation_dataset:  the identifier for the match values
        destination dataset

    :return: None
    :raises:  RuntimeError if validation_dataset is not defined.
    """
    LOGGER.info('Writing to the DRC bucket')
    if validation_dataset is None:
        LOGGER.error('Validation_dataset name is not defined.')
        raise RuntimeError('validation_dataset name cannot be None.')

    date_string = _get_date_string(validation_dataset)
    hpo_sites = readers.get_hpo_site_names()
    # generate aggregate site report
    bucket = gcs_utils.get_drc_bucket()
    filename = os.path.join(validation_dataset,
                            consts.REPORT_DIRECTORY.format(date=date_string),
                            consts.REPORT_TITLE
                           )
    _, errors = writers.create_site_validation_report(
        project, validation_dataset, hpo_sites, bucket, filename
    )

    if errors > 0:
        LOGGER.error("Encountered %d read errors when writing drc report",
                     errors
                    )


def match_participants(
        project,
        rdr_dataset,
        ehr_dataset,
        dest_dataset_id):
    """
    Entry point for performing participant matching of PPI, EHR, and PII data.

    :param project: a string representing the project name
    :param rdr_dataset:  the dataset created from the results given to us by
        the rdr team
    :param ehr_dataset:  the dataset containing the pii information for
        comparisons
    :param dest_dataset_id:  the desired identifier for the match values
        destination dataset

    :return: results of the field comparison for each hpo
    """
    LOGGER.info('Calling match_participants with:\n'
                'project:\t%s\n'
                'rdr_dataset:\t%s\n'
                'ehr_dataset:\t%s\n'
                'dest_dataset_id:\t%s\n',
                project, rdr_dataset, ehr_dataset, dest_dataset_id)

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
    LOGGER.info('Created new validation results dataset:\t%s', validation_dataset)

    # create intermediate observation table in new dataset
    readers.create_match_values_table(project, rdr_dataset, dest_dataset_id)

    hpo_sites = readers.get_hpo_site_names()

    # TODO:  create a proper config file to store this path
    field_list = resources.fields_for('identity_match')

    read_errors = 0
    write_errors = 0
    results = {}

    # validate first names
    for site in hpo_sites:
        LOGGER.info('Creating site table for %s', site)
        bq_utils.create_table(
            site + consts.VALIDATION_TABLE_SUFFIX,
            field_list,
            drop_existing=True,
            dataset_id=validation_dataset
        )
        LOGGER.info('Site table created for %s', site)
        all_match_value_headers = []
        site_match_values = {}
        all_match_value_headers.append(consts.FIRST_NAME_FIELD)
        LOGGER.info('Processing site: %s for field %s', site, all_match_value_headers[-1])
        match_values, exc = _compare_name_fields(
            project,
            validation_dataset,
            ehr_dataset,
            site,
            consts.OBS_PII_NAME_FIRST,
            consts.FIRST_NAME_FIELD
        )

        if exc is not None:
            read_errors += 1
        else:
            for person_id in match_values:
                if person_id not in site_match_values:
                    site_match_values[person_id] = [site, person_id]
                else:
                    site_match_values[person_id].append(match_values[person_id])
        LOGGER.info('Validated site: %s for field %s', site, all_match_value_headers[-1])

        # validate last names
        all_match_value_headers.append(consts.LAST_NAME_FIELD)
        LOGGER.info('Processing site: %s for field %s', site, all_match_value_headers[-1])
        match_values, exc = _compare_name_fields(
            project,
            validation_dataset,
            ehr_dataset,
            site,
            consts.OBS_PII_NAME_LAST,
            consts.LAST_NAME_FIELD
        )

        if exc is not None:
            read_errors += 1
        else:
            # write last name matches for hpo to table
            for person_id in match_values:
                if person_id not in site_match_values:
                    # assuming every person_id is only associated with one site
                    # TODO raise fatal error otherwise
                    site_match_values[person_id] = [site, person_id] + \
                                                  [consts.MISSING
                                                   for _
                                                   in range(len(all_match_value_headers) - 1)]
                else:
                    site_match_values[person_id].append(match_values[person_id])
            # len should be 4
            for person_id in site_match_values:
                if len(site_match_values[person_id]) < len(all_match_value_headers) + 2:
                    site_match_values[person_id].append(consts.MISSING)
        LOGGER.info('Validated site: %s for field %s', site, all_match_value_headers[-1])

        # validate middle names
        all_match_value_headers.append(consts.MIDDLE_NAME_FIELD)
        LOGGER.info('Processing site: %s for field %s', site, all_match_value_headers[-1])
        match_values, exc = _compare_name_fields(
            project,
            validation_dataset,
            ehr_dataset,
            site,
            consts.OBS_PII_NAME_MIDDLE,
            consts.MIDDLE_NAME_FIELD
        )

        if exc is not None:
            read_errors += 1
        else:
            # write middle name matches for hpo to table
            for person_id in match_values:
                if person_id not in site_match_values:
                    site_match_values[person_id] = [site, person_id] + \
                                                  [consts.MISSING
                                                   for _
                                                   in range(len(all_match_value_headers)-1)]
                else:
                    site_match_values[person_id].append(match_values[person_id])
            # len should be 5
            for person_id in site_match_values:
                if len(site_match_values[person_id]) < len(all_match_value_headers) + 2:
                    site_match_values[person_id].append(consts.MISSING)
        LOGGER.info('Validated site: %s for field %s', site, all_match_value_headers[-1])

        # validate zip codes
        all_match_value_headers.append(consts.ZIP_CODE_FIELD)
        LOGGER.info('Processing site: %s for field %s', site, all_match_value_headers[-1])
        match_values, exc = _compare_zip_codes(
            project,
            validation_dataset,
            rdr_dataset,
            ehr_dataset,
            site,
            consts.OBS_PII_STREET_ADDRESS_ZIP,
            consts.ZIP_CODE_FIELD
        )

        if exc is not None:
            read_errors += 1
        else:
            # write zip codes matces for hpo to table
            for person_id in match_values:
                if person_id not in site_match_values:
                    site_match_values[person_id] = [site, person_id] + \
                                                  [consts.MISSING
                                                   for _
                                                   in range(len(all_match_value_headers) - 1)]
                else:
                    site_match_values[person_id].append(match_values[person_id])
            # len should be 6
            for person_id in site_match_values:
                if len(site_match_values[person_id]) < len(all_match_value_headers) + 2:
                    site_match_values[person_id].append(consts.MISSING)
        LOGGER.info('Validated site: %s for field %s', site, all_match_value_headers[-1])

        # validate city
        all_match_value_headers.append(consts.CITY_FIELD)
        LOGGER.info('Processing site: %s for field %s', site, all_match_value_headers[-1])
        match_values, exc = _compare_cities(
            project,
            validation_dataset,
            rdr_dataset,
            ehr_dataset,
            site,
            consts.OBS_PII_STREET_ADDRESS_CITY,
            consts.CITY_FIELD
        )

        if exc is not None:
            read_errors += 1
        else:
            # write city matches for hpo to table
            for person_id in match_values:
                if person_id not in site_match_values:
                    site_match_values[person_id] = [site, person_id] + \
                                                  [consts.MISSING
                                                   for _
                                                   in range(len(all_match_value_headers) - 1)]
                else:
                    site_match_values[person_id].append(match_values[person_id])
            # len should be 7
            for person_id in site_match_values:
                if len(site_match_values[person_id]) < len(all_match_value_headers) + 2:
                    site_match_values[person_id].append(consts.MISSING)
        LOGGER.info('Validated site: %s for field %s', site, all_match_value_headers[-1])

        # validate state
        all_match_value_headers.append(consts.STATE_FIELD)
        LOGGER.info('Processing site: %s for field %s', site, all_match_value_headers[-1])
        match_values, exc = _compare_states(
            project,
            validation_dataset,
            rdr_dataset,
            ehr_dataset,
            site,
            consts.OBS_PII_STREET_ADDRESS_STATE,
            consts.STATE_FIELD
        )

        if exc is not None:
            read_errors += 1
        else:
            # write state matches for hpo to table
            for person_id in match_values:
                if person_id not in site_match_values:
                    site_match_values[person_id] = [site, person_id] + \
                                                  [consts.MISSING
                                                   for _
                                                   in range(len(all_match_value_headers) - 1)]
                else:
                    site_match_values[person_id].append(match_values[person_id])
            # len should be 8
            for person_id in site_match_values:
                if len(site_match_values[person_id]) < len(all_match_value_headers) + 2:
                    site_match_values[person_id].append(consts.MISSING)
        LOGGER.info('Validated site: %s for field %s', site, all_match_value_headers[-1])

        # validate street addresses
        all_match_value_headers.append(consts.ADDRESS_ONE_FIELD)
        all_match_value_headers.append(consts.ADDRESS_TWO_FIELD)
        LOGGER.info('Processing site: %s for field %s', site, all_match_value_headers[-1])
        address_one_matches, address_two_matches, exc = _compare_street_addresses(
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

        if exc is not None:
            read_errors += 1
        else:
            # write street address matches for hpo to table
            for person_id in address_one_matches:
                if person_id not in site_match_values:
                    site_match_values[person_id] = [site, person_id] + \
                                                  [consts.MISSING
                                                   for _
                                                   in range(len(all_match_value_headers) - 1)]
                else:
                    site_match_values[person_id].append(address_one_matches[person_id])
            # len should be 9
            for person_id in site_match_values:
                if len(site_match_values[person_id]) < len(all_match_value_headers) + 2:
                    site_match_values[person_id].append(consts.MISSING)

            for person_id in address_two_matches:
                if person_id not in site_match_values:
                    site_match_values[person_id] = [site, person_id] + \
                                                  [consts.MISSING
                                                   for _
                                                   in range(len(all_match_value_headers) - 1)]
                else:
                    site_match_values[person_id].append(address_two_matches[person_id])
            # len should be 10
            for person_id in site_match_values:
                if len(site_match_values[person_id]) < len(all_match_value_headers) + 2:
                    site_match_values[person_id].append(consts.MISSING)
        LOGGER.info('Validated site: %s for field %s', site, all_match_value_headers[-1])

        # validate email addresses
        all_match_value_headers.append(consts.EMAIL_FIELD)
        LOGGER.info('Processing site: %s for field %s', site, all_match_value_headers[-1])
        match_values, exc = _compare_email_addresses(
            project,
            validation_dataset,
            ehr_dataset,
            site,
            consts.OBS_PII_EMAIL_ADDRESS,
            consts.EMAIL_FIELD
        )

        if exc is not None:
            read_errors += 1
        else:
            # write email matches for hpo to table
            for person_id in match_values:
                if person_id not in site_match_values:
                    site_match_values[person_id] = [site, person_id] + \
                                                  [consts.MISSING
                                                   for _
                                                   in range(len(all_match_value_headers) - 1)]
                else:
                    site_match_values[person_id].append(match_values[person_id])
            # len should be 11
            for person_id in site_match_values:
                if len(site_match_values[person_id]) < len(all_match_value_headers) + 2:
                    site_match_values[person_id].append(consts.MISSING)
        LOGGER.info('Validated site: %s for field %s', site, all_match_value_headers[-1])

        # validate phone numbers
        all_match_value_headers.append(consts.PHONE_NUMBER_FIELD)
        LOGGER.info('Processing site: %s for field %s', site, all_match_value_headers[-1])
        match_values, exc = _compare_phone_numbers(
            project,
            validation_dataset,
            ehr_dataset,
            site,
            consts.OBS_PII_PHONE,
            consts.PHONE_NUMBER_FIELD
        )

        if exc is not None:
            read_errors += 1
        else:
            # write phone number matches for hpo to table
            for person_id in match_values:
                if person_id not in site_match_values:
                    site_match_values[person_id] = [site, person_id] + \
                                                  [consts.MISSING
                                                   for _
                                                   in range(len(all_match_value_headers) - 1)]
                else:
                    site_match_values[person_id].append(match_values[person_id])
            # len should be 12
            for person_id in site_match_values:
                if len(site_match_values[person_id]) < len(all_match_value_headers) + 2:
                    site_match_values[person_id].append(consts.MISSING)
        LOGGER.info('Validated site: %s for field %s', site, all_match_value_headers[-1])

        # validate genders
        all_match_value_headers.append(consts.SEX_FIELD)
        LOGGER.info('Processing site: %s for field %s', site, all_match_value_headers[-1])
        match_values, exc = _compare_genders(
            project,
            validation_dataset,
            ehr_dataset,
            site,
            consts.OBS_PII_SEX
        )

        if exc is not None:
            read_errors += 1
        else:
            # write birthday match for hpo to table
            for person_id in match_values:
                if person_id not in site_match_values:
                    site_match_values[person_id] = [site, person_id] + \
                                                  [consts.MISSING
                                                   for _
                                                   in range(len(all_match_value_headers) - 1)]
                else:
                    site_match_values[person_id].append(match_values[person_id])
            # len should be 13
            for person_id in site_match_values:
                if len(site_match_values[person_id]) < len(all_match_value_headers) + 2:
                    site_match_values[person_id].append(consts.MISSING)
        LOGGER.info('Validated site: %s for field %s', site, all_match_value_headers[-1])

        # validate birth dates
        all_match_value_headers.append(consts.BIRTH_DATE_FIELD)
        LOGGER.info('Processing site: %s for field %s', site, all_match_value_headers[-1])
        match_values, exc = _compare_birth_dates(
            project,
            validation_dataset,
            ehr_dataset,
            site,
            consts.OBS_PII_BIRTH_DATETIME
        )

        if exc is not None:
            read_errors += 1
        else:
            # write birthday match for hpo to table
            for person_id in match_values:
                if person_id not in site_match_values:
                    site_match_values[person_id] = [site, person_id] + \
                                                  [consts.MISSING
                                                   for _
                                                   in range(len(all_match_value_headers) - 1)]
                else:
                    site_match_values[person_id].append(match_values[person_id])
            # len should be 14
            for person_id in site_match_values:
                if len(site_match_values[person_id]) < len(all_match_value_headers) + 2:
                    site_match_values[person_id].append(consts.MISSING)
            for person_id in site_match_values:
                site_match_values[person_id].append(writer_consts.YES)
        LOGGER.info('Validated site: %s for field %s', site, all_match_value_headers[-1])
        site_match_values_dict = convert_all_match_list_to_dict(site_match_values)

        # generate aggregate site report
        bucket = gcs_utils.get_drc_bucket()
        file_path = os.path.join(validation_dataset,
                                 consts.REPORT_DIRECTORY.format(date=date_string))
        file_name = consts.REPORT_TITLE
        reports = writers.load_site_validation_report(
            project, validation_dataset, site_match_values_dict, site, bucket, file_path, file_name
        )
        results[site] = reports

    LOGGER.info("Finished creating validation tables")

    if read_errors > 0:
        LOGGER.error("Encountered %d read errors creating validation dataset:\t%s",
                     read_errors,
                     validation_dataset)

    if write_errors > 0:
        LOGGER.error("Encountered %d write errors creating validation dataset:\t%s",
                     write_errors,
                     validation_dataset)

    return results, read_errors + write_errors


if __name__ == '__main__':
    RDR_DATASET = ''   # the combined dataset
    PII_DATASET = ''         # the ehr dataset
    PROJECT = ''       # the project identifier
    DEST_DATASET_ID = ''     # desired name of the validation dataset
    match_participants(
        PROJECT, RDR_DATASET, PII_DATASET, DEST_DATASET_ID
    )
