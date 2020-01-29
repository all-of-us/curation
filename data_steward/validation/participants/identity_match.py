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
from constants import bq_utils as bq_consts
from constants.validation.participants import identity_match as consts
import resources
from validation.participants import normalizers as normalizer
from validation.participants import readers as readers
from validation.participants import writers as writers

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


def _compare_name_fields(project, rdr_dataset, pii_dataset, hpo, concept_id,
                         pii_field, pii_tables):
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
    table_name = hpo + consts.PII_NAME_TABLE

    if table_name in pii_tables:
        rdr_names = readers.get_rdr_match_values(project, rdr_dataset,
                                                 consts.ID_MATCH_TABLE,
                                                 concept_id)

        try:
            pii_names = readers.get_pii_values(project, pii_dataset, hpo,
                                               consts.PII_NAME_TABLE, pii_field)
        except (oauth2client.client.HttpAccessTokenRefreshError,
                googleapiclient.errors.HttpError):
            LOGGER.exception("Unable to read PII for: %s\tdata field:\t%s", hpo,
                             pii_field)
            raise

        for person_id, pii_name in pii_names:
            rdr_name = rdr_names.get(person_id)

            if rdr_name is None or pii_name is None:
                match_str = consts.MISSING
            else:
                pii_name = normalizer.normalize_name(pii_name)
                rdr_name = normalizer.normalize_name(rdr_name)
                match_str = consts.MATCH if rdr_name == pii_name else consts.MISMATCH

            match_values[person_id] = match_str
    else:
        raise RuntimeError('Table {} doesnt exist.'.format(table_name))

    return match_values


def _compare_email_addresses(project, rdr_dataset, pii_dataset, hpo, concept_id,
                             pii_field, pii_tables):
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
    table_name = hpo + consts.PII_EMAIL_TABLE

    if table_name in pii_tables:
        email_addresses = readers.get_rdr_match_values(project, rdr_dataset,
                                                       consts.ID_MATCH_TABLE,
                                                       concept_id)

        try:
            pii_emails = readers.get_pii_values(project, pii_dataset, hpo,
                                                consts.PII_EMAIL_TABLE,
                                                pii_field)
        except (oauth2client.client.HttpAccessTokenRefreshError,
                googleapiclient.errors.HttpError):
            LOGGER.exception("Unable to read PII for: %s\tdata field:\t%s", hpo,
                             pii_field)
            raise

        for person_id, pii_email in pii_emails:
            rdr_email = email_addresses.get(person_id)

            if rdr_email is None or pii_email is None:
                match_str = consts.MISSING
            else:
                rdr_email = normalizer.normalize_email(rdr_email)
                pii_email = normalizer.normalize_email(pii_email)
                match_str = consts.MATCH if rdr_email == pii_email else consts.MISMATCH

            match_values[person_id] = match_str
    else:
        raise RuntimeError('Table {} doesnt exist.'.format(table_name))

    return match_values


def _compare_phone_numbers(project, rdr_dataset, pii_dataset, hpo, concept_id,
                           pii_field, pii_tables):
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
    table_name = hpo + consts.PII_PHONE_TABLE

    if table_name in pii_tables:
        phone_numbers = readers.get_rdr_match_values(project, rdr_dataset,
                                                     consts.ID_MATCH_TABLE,
                                                     concept_id)

        try:
            pii_phone_numbers = readers.get_pii_values(project, pii_dataset,
                                                       hpo,
                                                       consts.PII_PHONE_TABLE,
                                                       pii_field)
        except (oauth2client.client.HttpAccessTokenRefreshError,
                googleapiclient.errors.HttpError):
            LOGGER.exception("Unable to read PII for: %s\tdata field:\t%s", hpo,
                             pii_field)
            raise

        for person_id, pii_number in pii_phone_numbers:
            rdr_phone = phone_numbers.get(person_id)

            if rdr_phone is None or pii_number is None:
                match_str = consts.MISSING
            else:
                rdr_phone = normalizer.normalize_phone(rdr_phone)
                pii_number = normalizer.normalize_phone(pii_number)
                match_str = consts.MATCH if rdr_phone == pii_number else consts.MISMATCH

            match_values[person_id] = match_str
    else:
        raise RuntimeError('Table {} doesnt exist.'.format(table_name))

    return match_values


def _compare_cities(project, validation_dataset, rdr_dataset, pii_dataset, hpo,
                    concept_id, pii_field, pii_tables):
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
    table_name = hpo + consts.PII_ADDRESS_TABLE

    if table_name in pii_tables:
        cities = readers.get_rdr_match_values(project, validation_dataset,
                                              consts.ID_MATCH_TABLE, concept_id)

        try:
            pii_cities = readers.get_location_pii(project, rdr_dataset,
                                                  pii_dataset, hpo,
                                                  consts.PII_ADDRESS_TABLE,
                                                  pii_field)
        except (oauth2client.client.HttpAccessTokenRefreshError,
                googleapiclient.errors.HttpError):
            LOGGER.exception("Unable to read PII for: %s\tdata field:\t%s", hpo,
                             pii_field)
            raise

        for person_id, pii_city in pii_cities:
            rdr_city = cities.get(person_id)

            if rdr_city is None or pii_city is None:
                match_str = consts.MISSING
            else:
                rdr_city = normalizer.normalize_city_name(rdr_city)
                pii_city = normalizer.normalize_city_name(pii_city)
                match_str = consts.MATCH if rdr_city == pii_city else consts.MISMATCH

            match_values[person_id] = match_str
    else:
        raise RuntimeError('Table {} doesnt exist.'.format(table_name))

    return match_values


def _compare_states(project, validation_dataset, rdr_dataset, pii_dataset, hpo,
                    concept_id, pii_field, pii_tables):
    """
    Compare state addresses from hpo PII table and OMOP observation table.

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
    table_name = hpo + consts.PII_ADDRESS_TABLE

    if table_name in pii_tables:
        states = readers.get_rdr_match_values(project, validation_dataset,
                                              consts.ID_MATCH_TABLE, concept_id)

        try:
            pii_states = readers.get_location_pii(project, rdr_dataset,
                                                  pii_dataset, hpo,
                                                  consts.PII_ADDRESS_TABLE,
                                                  pii_field)
        except (oauth2client.client.HttpAccessTokenRefreshError,
                googleapiclient.errors.HttpError):
            LOGGER.exception("Unable to read PII for: %s\tdata field:\t%s", hpo,
                             pii_field)
            raise

        for person_id, pii_state in pii_states:
            rdr_state = states.get(person_id)

            if rdr_state is None or pii_state is None:
                match_str = consts.MISSING
            else:
                rdr_state = normalizer.normalize_state(rdr_state)
                pii_state = normalizer.normalize_state(pii_state)
                match_str = consts.MATCH if rdr_state == pii_state else consts.MISMATCH

            match_values[person_id] = match_str
    else:
        raise RuntimeError('Table {} doesnt exist.'.format(table_name))

    return match_values


def _compare_zip_codes(project, validation_dataset, rdr_dataset, pii_dataset,
                       hpo, concept_id, pii_field, pii_tables):
    """
    Compare zip codes from hpo PII table and OMOP observation table.

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
    table_name = hpo + consts.PII_ADDRESS_TABLE

    if table_name in pii_tables:
        zip_codes = readers.get_rdr_match_values(project, validation_dataset,
                                                 consts.ID_MATCH_TABLE,
                                                 concept_id)

        try:
            pii_zip_codes = readers.get_location_pii(project, rdr_dataset,
                                                     pii_dataset, hpo,
                                                     consts.PII_ADDRESS_TABLE,
                                                     pii_field)
        except (oauth2client.client.HttpAccessTokenRefreshError,
                googleapiclient.errors.HttpError):
            LOGGER.exception("Unable to read PII for: %s\tdata field:\t%s", hpo,
                             pii_field)
            raise

        for person_id, pii_zip_code in pii_zip_codes:
            rdr_zip = zip_codes.get(person_id)

            if rdr_zip is None or pii_zip_code is None:
                match_str = consts.MISSING
            else:
                rdr_zip = normalizer.normalize_zip(rdr_zip)
                pii_zip = normalizer.normalize_zip(pii_zip_code)
                match_str = consts.MATCH if rdr_zip == pii_zip else consts.MISMATCH

            match_values[person_id] = match_str
    else:
        raise RuntimeError('Table {} doesnt exist.'.format(table_name))

    return match_values


def _compare_street_addresses(project, validation_dataset, rdr_dataset,
                              pii_dataset, hpo, concept_id_one, concept_id_two,
                              field_one, field_two, pii_tables):
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
    table_name = hpo + consts.PII_ADDRESS_TABLE

    if table_name in pii_tables:
        rdr_address_ones = readers.get_rdr_match_values(project,
                                                        validation_dataset,
                                                        consts.ID_MATCH_TABLE,
                                                        concept_id_one)

        rdr_address_twos = readers.get_rdr_match_values(project,
                                                        validation_dataset,
                                                        consts.ID_MATCH_TABLE,
                                                        concept_id_two)

        try:
            pii_street_ones = readers.get_location_pii(project, rdr_dataset,
                                                       pii_dataset, hpo,
                                                       consts.PII_ADDRESS_TABLE,
                                                       field_one)
        except (oauth2client.client.HttpAccessTokenRefreshError,
                googleapiclient.errors.HttpError):
            LOGGER.exception("Unable to read PII for: %s\tdata field:\t%s", hpo,
                             field_one)
            raise
        try:
            pii_street_twos = readers.get_location_pii(project, rdr_dataset,
                                                       pii_dataset, hpo,
                                                       consts.PII_ADDRESS_TABLE,
                                                       field_two)
        except (oauth2client.client.HttpAccessTokenRefreshError,
                googleapiclient.errors.HttpError):
            LOGGER.exception("Unable to read PII for: %s\tdata field:\t%s", hpo,
                             field_two)
            raise

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

        for person_id, addresses in pii_street_addresses.items():

            pii_addr_one = addresses[1]
            pii_addr_two = addresses[2]

            rdr_addr_one = normalizer.normalize_street(
                rdr_address_ones.get(person_id))
            pii_addr_one = normalizer.normalize_street(pii_addr_one)
            rdr_addr_two = normalizer.normalize_street(
                rdr_address_twos.get(person_id))
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
                missing_rdr = _compare_address_lists(full_rdr_street_list,
                                                     full_pii_street_list)
                missing_pii = _compare_address_lists(full_pii_street_list,
                                                     full_rdr_street_list)

                if (missing_rdr + missing_pii) > 0:
                    address_one_match_values[person_id] = consts.MISMATCH
                    address_two_match_values[person_id] = consts.MISMATCH
                else:
                    address_one_match_values[person_id] = consts.MATCH
                    address_two_match_values[person_id] = consts.MATCH
    else:
        raise RuntimeError('Table {} doesnt exist.'.format(table_name))

    return address_one_match_values, address_two_match_values


def _compare_genders(project, validation_dataset, pii_dataset, hpo,
                     concept_id_pii, pii_tables):
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
    table_name = hpo + consts.EHR_PERSON_TABLE_SUFFIX

    if table_name in pii_tables:
        pii_genders = readers.get_rdr_match_values(project, validation_dataset,
                                                   consts.ID_MATCH_TABLE,
                                                   concept_id_pii)

        try:
            ehr_genders = readers.get_ehr_person_values(project, pii_dataset,
                                                        table_name,
                                                        consts.GENDER_FIELD)
        except (oauth2client.client.HttpAccessTokenRefreshError,
                googleapiclient.errors.HttpError):
            LOGGER.exception("Unable to read PII for: %s\tdata field:\t%s", hpo,
                             consts.GENDER_FIELD)
            raise

        # compare gender from ppi info to ehr info and record results.
        for person_id, ehr_gender in ehr_genders.items():
            rdr_gender = pii_genders.get(person_id, '')
            ehr_gender = consts.SEX_CONCEPT_IDS.get(ehr_gender, '')

            if rdr_gender is None or ehr_gender is None:
                match_str = consts.MISSING
            else:
                rdr_gender = rdr_gender.lower()
                ehr_gender = ehr_gender.lower()
                match_str = consts.MATCH if rdr_gender == ehr_gender else consts.MISMATCH

            match_values[person_id] = match_str
    else:
        raise RuntimeError('Table {} doesnt exist.'.format(table_name))

    return match_values


def _compare_birth_dates(project, validation_dataset, pii_dataset, site,
                         concept_id_pii, pii_tables):
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
    table_name = site + consts.EHR_PERSON_TABLE_SUFFIX

    if table_name in pii_tables:
        pii_birthdates = readers.get_rdr_match_values(project,
                                                      validation_dataset,
                                                      consts.ID_MATCH_TABLE,
                                                      concept_id_pii)

        try:
            ehr_birthdates = readers.get_ehr_person_values(
                project, pii_dataset, table_name, consts.BIRTH_DATETIME_FIELD)
        except (oauth2client.client.HttpAccessTokenRefreshError,
                googleapiclient.errors.HttpError):
            LOGGER.exception("Unable to read PII for: %s\tdata field:\t%s",
                             site, consts.BIRTH_DATETIME_FIELD)
            raise

        # compare birth_datetime from ppi info to ehr info and record results.
        for person_id, ehr_birthdate in ehr_birthdates.items():
            rdr_birthdate = pii_birthdates.get(person_id)
            ehr_birthdate = ehr_birthdates.get(person_id)

            if rdr_birthdate is None or ehr_birthdate is None:
                match_values[person_id] = consts.MISSING
            elif isinstance(rdr_birthdate, str) and isinstance(
                    ehr_birthdate, str):
                # convert values to datetime objects
                rdr_date = parse(rdr_birthdate)
                ehr_date = parse(ehr_birthdate)
                # convert datetime objects to Year/month/day strings and compare
                rdr_string = rdr_date.strftime(consts.DATE_FORMAT)
                ehr_string = ehr_date.strftime(consts.DATE_FORMAT)

                match_str = consts.MATCH if rdr_string == ehr_string else consts.MISMATCH
                match_values[person_id] = match_str
            else:
                match_values[person_id] = consts.MISMATCH
    else:
        raise RuntimeError('Table {} doesnt exist.'.format(table_name))

    return match_values


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
            consts.REPORT_TITLE)
        _, errors = writers.create_site_validation_report(
            project, validation_dataset, [site], bucket, filename)

        if errors > 0:
            LOGGER.error(
                "Encountered %d read errors when writing %s site report",
                errors, site)


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
                            consts.REPORT_TITLE)
    _, errors = writers.create_site_validation_report(project,
                                                      validation_dataset,
                                                      hpo_sites, bucket,
                                                      filename)

    if errors > 0:
        LOGGER.error("Encountered %d read errors when writing drc report",
                     errors)


def _add_matches_to_results(results, matches, field):
    for key, value in matches.items():
        individual = results.get(key)

        if individual is None:
            initialization_dict = {}
            # add in missing data to dictionary
            for field_item in consts.VALIDATION_FIELDS:
                initialization_dict[field_item] = consts.MISSING
            individual = initialization_dict

        individual[field] = value
        results[key] = individual

    return results


def match_participants(project, rdr_dataset, ehr_dataset, dest_dataset_id):
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
    LOGGER.info(
        'Calling match_participants with:\n'
        'project:\t%s\n'
        'rdr_dataset:\t%s\n'
        'ehr_dataset:\t%s\n'
        'dest_dataset_id:\t%s\n', project, rdr_dataset, ehr_dataset,
        dest_dataset_id)

    ehr_tables = bq_utils.list_dataset_contents(ehr_dataset)

    date_string = _get_date_string(rdr_dataset)

    if not re.match(consts.DRC_DATE_REGEX, dest_dataset_id[-8:]):
        dest_dataset_id += date_string

    # create new dataset for the intermediate tables and results
    dataset_result = bq_utils.create_dataset(
        dataset_id=dest_dataset_id,
        description=consts.DESTINATION_DATASET_DESCRIPTION.format(
            version='', rdr_dataset=rdr_dataset, ehr_dataset=ehr_dataset),
        overwrite_existing=True)

    validation_dataset = dataset_result.get(bq_consts.DATASET_REF, {})
    validation_dataset = validation_dataset.get(bq_consts.DATASET_ID, '')
    LOGGER.info('Created new validation results dataset:\t%s',
                validation_dataset)

    # create intermediate observation table in new dataset
    readers.create_match_values_table(project, rdr_dataset, dest_dataset_id)

    hpo_sites = readers.get_hpo_site_names()

    #TODO:  create a proper config file to store this path
    field_list = resources.fields_for('identity_match')

    for site_name in hpo_sites:
        bq_utils.create_table(site_name + consts.VALIDATION_TABLE_SUFFIX,
                              field_list,
                              drop_existing=True,
                              dataset_id=validation_dataset)

    read_errors = 0
    write_errors = 0

    # validate first names
    for site in hpo_sites:
        LOGGER.info('Beginning identity validation for site: %s', site)
        results = {}

        try:
            match_values = None
            match_values = _compare_name_fields(project, validation_dataset,
                                                ehr_dataset, site,
                                                consts.OBS_PII_NAME_FIRST,
                                                consts.FIRST_NAME_FIELD,
                                                ehr_tables)
        except (oauth2client.client.HttpAccessTokenRefreshError,
                googleapiclient.errors.HttpError, RuntimeError):
            LOGGER.exception("Could not read data for field: %s at site: %s",
                             consts.FIRST_NAME_FIELD, site)
            read_errors += 1
        else:
            results = _add_matches_to_results(results, match_values,
                                              consts.FIRST_NAME_FIELD)
            LOGGER.info('Validated first names for: %s', site)

        # validate last names
        try:
            match_values = None
            match_values = _compare_name_fields(project, validation_dataset,
                                                ehr_dataset, site,
                                                consts.OBS_PII_NAME_LAST,
                                                consts.LAST_NAME_FIELD,
                                                ehr_tables)
        except (oauth2client.client.HttpAccessTokenRefreshError,
                googleapiclient.errors.HttpError, RuntimeError):
            LOGGER.exception("Could not read data for field: %s at site: %s",
                             consts.LAST_NAME_FIELD, site)
            read_errors += 1
        else:
            results = _add_matches_to_results(results, match_values,
                                              consts.LAST_NAME_FIELD)
            LOGGER.info('Validated last names for: %s', site)

        # validate middle names
        try:
            match_values = None


#            match_values = _compare_name_fields(
#                project,
#                validation_dataset,
#                ehr_dataset,
#                site,
#                consts.OBS_PII_NAME_MIDDLE,
#                consts.MIDDLE_NAME_FIELD,
#                ehr_tables
#            )
        except (oauth2client.client.HttpAccessTokenRefreshError,
                googleapiclient.errors.HttpError, RuntimeError):
            LOGGER.exception("Could not read data for field: %s at site: %s",
                             consts.MIDDLE_NAME_FIELD, site)
            read_errors += 1
        else:
            # write middle name matches for hpo to table
            #            results = _add_matches_to_results(results, match_values, consts.MIDDLE_NAME_FIELD)
            LOGGER.info('Not validating middle names')

        # validate zip codes
        try:
            match_values = None
            match_values = _compare_zip_codes(project, validation_dataset,
                                              rdr_dataset, ehr_dataset, site,
                                              consts.OBS_PII_STREET_ADDRESS_ZIP,
                                              consts.ZIP_CODE_FIELD, ehr_tables)
        except (oauth2client.client.HttpAccessTokenRefreshError,
                googleapiclient.errors.HttpError, RuntimeError):
            LOGGER.exception("Could not read data for field: %s at site: %s",
                             consts.ZIP_CODE_FIELD, site)
            read_errors += 1
        else:
            results = _add_matches_to_results(results, match_values,
                                              consts.ZIP_CODE_FIELD)
            LOGGER.info('Validated zip codes for: %s', site)

        # validate city
        try:
            match_values = None
            match_values = _compare_cities(project, validation_dataset,
                                           rdr_dataset, ehr_dataset, site,
                                           consts.OBS_PII_STREET_ADDRESS_CITY,
                                           consts.CITY_FIELD, ehr_tables)
        except (oauth2client.client.HttpAccessTokenRefreshError,
                googleapiclient.errors.HttpError, RuntimeError):
            LOGGER.exception("Could not read data for field: %s at site: %s",
                             consts.CITY_FIELD, site)
            read_errors += 1
        else:
            results = _add_matches_to_results(results, match_values,
                                              consts.ZIP_CODE_FIELD)
            LOGGER.info('Validated city names for: %s', site)

        # validate state
        try:
            match_values = None
            match_values = _compare_states(project, validation_dataset,
                                           rdr_dataset, ehr_dataset, site,
                                           consts.OBS_PII_STREET_ADDRESS_STATE,
                                           consts.STATE_FIELD, ehr_tables)
        except (oauth2client.client.HttpAccessTokenRefreshError,
                googleapiclient.errors.HttpError, RuntimeError):
            LOGGER.exception("Could not read data for field: %s at site: %s",
                             consts.STATE_FIELD, site)
            read_errors += 1
        else:
            results = _add_matches_to_results(results, match_values,
                                              consts.STATE_FIELD)
            LOGGER.info('Validated states for: %s', site)

        # validate street addresses
        try:
            address_one_matches = None
            address_two_matches = None
            match_values = None
            address_one_matches, address_two_matches = _compare_street_addresses(
                project, validation_dataset, rdr_dataset, ehr_dataset, site,
                consts.OBS_PII_STREET_ADDRESS_ONE,
                consts.OBS_PII_STREET_ADDRESS_TWO, consts.ADDRESS_ONE_FIELD,
                consts.ADDRESS_TWO_FIELD, ehr_tables)
        except (oauth2client.client.HttpAccessTokenRefreshError,
                googleapiclient.errors.HttpError, RuntimeError):
            LOGGER.exception(
                "Could not read data for fields: %s, %s at site: %s",
                consts.ADDRESS_ONE_FIELD, consts.ADDRESS_TWO_FIELD, site)
            read_errors += 1
        else:
            results = _add_matches_to_results(results, address_one_matches,
                                              consts.ADDRESS_ONE_FIELD)
            results = _add_matches_to_results(results, address_two_matches,
                                              consts.ADDRESS_TWO_FIELD)
            LOGGER.info('Validated street addresses for: %s', site)

        # validate email addresses
        try:
            match_values = None
            match_values = _compare_email_addresses(
                project, validation_dataset, ehr_dataset, site,
                consts.OBS_PII_EMAIL_ADDRESS, consts.EMAIL_FIELD, ehr_tables)
        except (oauth2client.client.HttpAccessTokenRefreshError,
                googleapiclient.errors.HttpError, RuntimeError):
            LOGGER.exception("Could not read data for field: %s at site: %s",
                             consts.EMAIL_FIELD, site)
            read_errors += 1
        else:
            results = _add_matches_to_results(results, match_values,
                                              consts.EMAIL_FIELD)
            LOGGER.info('Validated email addresses for: %s', site)

        # validate phone numbers
        try:
            match_values = None
            match_values = _compare_phone_numbers(project, validation_dataset,
                                                  ehr_dataset, site,
                                                  consts.OBS_PII_PHONE,
                                                  consts.PHONE_NUMBER_FIELD,
                                                  ehr_tables)
        except (oauth2client.client.HttpAccessTokenRefreshError,
                googleapiclient.errors.HttpError, RuntimeError):
            LOGGER.exception("Could not read data for field: %s at site: %s",
                             consts.PHONE_NUMBER_FIELD, site)
            read_errors += 1
        else:
            results = _add_matches_to_results(results, match_values,
                                              consts.PHONE_NUMBER_FIELD)
            LOGGER.info('Validated phone numbers for: %s', site)

        # validate genders
        try:
            match_values = None
            match_values = _compare_genders(project, validation_dataset,
                                            ehr_dataset, site,
                                            consts.OBS_PII_SEX, ehr_tables)
        except (oauth2client.client.HttpAccessTokenRefreshError,
                googleapiclient.errors.HttpError, RuntimeError):
            LOGGER.exception("Could not read data for field: %s at site: %s",
                             consts.SEX_FIELD, site)
            read_errors += 1
        else:
            results = _add_matches_to_results(results, match_values,
                                              consts.SEX_FIELD)
            LOGGER.info('Validated genders for: %s', site)

        # validate birth dates
        try:
            match_values = None
            match_values = _compare_birth_dates(project, validation_dataset,
                                                ehr_dataset, site,
                                                consts.OBS_PII_BIRTH_DATETIME,
                                                ehr_tables)
        except (oauth2client.client.HttpAccessTokenRefreshError,
                googleapiclient.errors.HttpError, RuntimeError):
            LOGGER.exception("Could not read data for field: %s at site: %s",
                             consts.BIRTH_DATETIME_FIELD, site)
            read_errors += 1
        else:
            results = _add_matches_to_results(results, match_values,
                                              consts.BIRTH_DATE_FIELD)
            LOGGER.info('Validated birth dates for: %s', site)

        LOGGER.info('Writing results to BQ table')
        # write dictionary to a table
        try:
            writers.write_to_result_table(project, validation_dataset, site,
                                          results)
        except (oauth2client.client.HttpAccessTokenRefreshError,
                googleapiclient.errors.HttpError):
            LOGGER.exception(
                'Did not write site information to validation dataset:  %s',
                site)
            write_errors += 1

        LOGGER.info('Wrote validation results for site: %s', site)

    LOGGER.info("FINISHED: Validation dataset created:  %s", validation_dataset)

    if read_errors > 0:
        LOGGER.error(
            "Encountered %d read errors creating validation dataset:\t%s",
            read_errors, validation_dataset)

    if write_errors > 0:
        LOGGER.error(
            "Encountered %d write errors creating validation dataset:\t%s",
            write_errors, validation_dataset)

    return read_errors + write_errors

if __name__ == '__main__':
    RDR_DATASET = ''  # the combined dataset
    PII_DATASET = ''  # the ehr dataset
    PROJECT = ''  # the project identifier
    DEST_DATASET_ID = ''  # desired name of the validation dataset
    match_participants(PROJECT, RDR_DATASET, PII_DATASET, DEST_DATASET_ID)
