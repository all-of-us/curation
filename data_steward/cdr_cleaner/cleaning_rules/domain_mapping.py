from collections import OrderedDict
import resources
import logging
import re

LOGGER = logging.getLogger(__name__)

SRC_FIELD = 'src_field'
DEST_FIELD = 'dest_field'
SRC_TABLE = 'src_table'
DEST_TABLE = 'dest_table'
SRC_VALUE = 'src_value'
DEST_VALUE = 'dest_value'
TRANSLATION = 'translation'
IS_REROUTED = 'is_rerouted'
REROUTING_CRITERIA = 'rerouting_criteria'

DOMAIN_TABLE_NAMES = [
    'condition_occurrence', 'procedure_occurrence', 'drug_exposure',
    'device_exposure', 'observation', 'measurement'
]

METADATA_DOMAIN = 'Metadata'
NULL_VALUE = 'NULL'
EMPTY_STRING = ''

table_mappings_csv = resources.table_mappings_csv()
field_mappings_csv = resources.field_mappings_csv()
value_mappings_csv = resources.value_mappings_csv()


def exist_domain_mappings(src_table, dest_table):
    """
    This function checks if the rerouting between src_table and dest_table is possible
    :param src_table: the source CDM table
    :param dest_table: the destination CDM table
    :return:
    """
    for t in table_mappings_csv:
        if t[SRC_TABLE] == src_table and t[DEST_TABLE] == dest_table:
            return int(t[IS_REROUTED]) == 1
    return False


def get_rerouting_criteria(src_table, dest_table):
    """
    This function checks if there is a rerouting criteria between src_table and dest_table.
    E.g. From observation to measurement, we reroute the observation records only if there are values associated with such records
    :param src_table: the source CDM table
    :param dest_table: the destination CDM table
    :return:
    """
    for t in table_mappings_csv:
        if t[SRC_TABLE] == src_table and t[DEST_TABLE] == dest_table:
            return re.sub('^\\s+$', EMPTY_STRING, t[REROUTING_CRITERIA])
    return EMPTY_STRING


def get_field_mappings(src_table, dest_table):
    """
    This function retrieves the field mappings between src_table and dest_table.
    :param src_table: the source CDM table
    :param dest_table: the destination CDM table
    :return:
    """
    field_mappings = {}
    for t in field_mappings_csv:
        if t[SRC_TABLE] == src_table and t[DEST_TABLE] == dest_table:
            field_mappings[t[DEST_FIELD]] = t[SRC_FIELD]
    return OrderedDict(sorted(field_mappings.items()))


def value_requires_translation(src_table, dest_table, src_field, dest_field):
    """
    This function checks if the values between src_field and dest_field need to be translated.
    E.g. the value primary_condition for condition_type_concept_id needs to be translated to
    the value primary_procedure for procedure_type_concept_id
    :param src_table: the source CDM table
    :param dest_table: the destination CDM table
    :param src_field: the src_field from which the values need to be translated
    :param dest_field: the dest_field into which the values need to be translated
    :return: a boolean value that indicates if the translation is required
    """
    for t in field_mappings_csv:
        if t[SRC_TABLE] == src_table \
                and t[DEST_TABLE] == dest_table \
                and t[SRC_FIELD] == src_field \
                and t[DEST_FIELD] == dest_field:
            return int(t[TRANSLATION]) == 1
    return False


def get_value_mappings(src_table, dest_table, src_field, dest_field):
    """
    This function translates values between src_field and dest_field.
    E.g. the value primary_condition for condition_type_concept_id needs to be translated to
    the value primary_procedure for procedure_type_concept_id
    :param src_table: the source CDM table
    :param dest_table: the destination CDM table
    :param src_field: the src_field from which the values need to be translated
    :param dest_field: the dest_field into which the values need to be translated
    :return: a list of value mappings between the src_field and dest_field
    """
    value_mappings = {}
    for t in value_mappings_csv:
        if t[SRC_TABLE] == src_table \
                and t[DEST_TABLE] == dest_table \
                and t[SRC_FIELD] == src_field \
                and t[DEST_FIELD] == dest_field:
            value_mappings[t[DEST_VALUE]] = t[SRC_VALUE]

    return value_mappings
