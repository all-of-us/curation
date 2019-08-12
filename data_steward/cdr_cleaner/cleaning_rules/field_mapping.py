from collections import OrderedDict
from domain_mapping import DOMAIN_TABLE_NAMES
from domain_mapping import get_domain
from domain_mapping import get_domain_id_field
from domain_mapping import get_field_mappings
from domain_mapping import exist_domain_mappings
from domain_mapping import NULL_VALUE
import domain_mapping
import resources
import re

NAME_FIELD = 'name'
TYPE_CONCEPT_SUFFIX = '_type_concept_id'
DOMAIN_COMMON_FIELDS = 'common_fields'
DOMAIN_SPECIFIC_FIELDS = 'specific_fields'
DOMAIN_DATE_FIELDS = 'date_fields'

COMMON_DOMAIN_FIELD_SUFFIXES = [
    'person_id',
    'visit_occurrence_id',
    'provider_id',
    '_concept_id',
    '_type_concept_id',
    '_source_value',
    '_source_concept_id'
]

DATE_FIELD_SUFFIXES = [
    '_start_date',
    '_start_datetime',
    '_end_date',
    '_end_datetime',
    '_date',
    '_datetime'
]

ATTRIBUTE_MAPPING_TEMPLATE = '{src_table},{dest_table},{src_field},{dest_field},{translation}'

CDM_TABLE_SCHEMAS = resources.cdm_schemas(False, False)

FIELD_MAPPING_HEADER = 'src_table,dest_table,src_field,dest_field,translation\n'


def generate_field_mappings(_src_table, _dest_table, src_table_fields, dest_table_fields):
    """
    This functions generates a list of field mappings between the src_table and dest_table

    :param _src_table: the source CDM table
    :param _dest_table: the destination CDM table
    :param src_table_fields: the dictionary that contains all the source fields (common fields, date fields and domain specific fields)
    :param dest_table_fields: the dictionary that contains all the destination fields (common fields, date fields and domain specific fields)
    :return: a list of field mappings between _src_table and _dest_table
    """
    _field_mappings = OrderedDict()
    for field_type in dest_table_fields:

        if field_type == DOMAIN_SPECIFIC_FIELDS:
            specific_field_mappings = resolve_specific_field_mappings(_src_table, _dest_table,
                                                                      dest_table_fields[DOMAIN_SPECIFIC_FIELDS])
            _field_mappings.update(specific_field_mappings)

        elif field_type == DOMAIN_DATE_FIELDS:
            date_field_mappings = resolve_date_field_mappings(src_table_fields[DOMAIN_DATE_FIELDS],
                                                              dest_table_fields[DOMAIN_DATE_FIELDS])
            _field_mappings.update(date_field_mappings)

        else:
            common_field_mappings = resolve_common_field_mappings(src_table_fields[DOMAIN_COMMON_FIELDS],
                                                                  dest_table_fields[DOMAIN_COMMON_FIELDS])
            _field_mappings.update(common_field_mappings)

    return _field_mappings


def resolve_common_field_mappings(src_common_fields, dest_common_fields):
    """
    This function generates a list of field mappings for common domain fields.
    :param src_common_fields: a dictionary that contains the source common fields
    :param dest_common_fields: a dictionary that contains the destination common fields
    :return:
    """
    common_field_mappings = OrderedDict()

    for field_suffix in dest_common_fields:
        _dest_field = dest_common_fields[field_suffix]
        _src_field = src_common_fields[field_suffix] if field_suffix in src_common_fields else NULL_VALUE
        common_field_mappings[_dest_field] = _src_field

    return common_field_mappings


def resolve_specific_field_mappings(_src_table, _dest_table, _dest_specific_fields):
    """
    This function generates a list of field mappings between _src_table and _dest_table for the domain specific fields.
    E.g. The fields value_as_number and value_as_concept_id can be mapped between observation and measurement.

    :param _src_table: the source CDM table
    :param _dest_table: the destination CDM table
    :param _dest_specific_fields: an array that contains the specific destination fields
    :return:
    """
    specific_field_mappings = OrderedDict()

    # If the src_table and dest_table are the same, map all the fields onto themselves.
    if _src_table == _dest_table:
        for dest_specific_field in _dest_specific_fields:
            specific_field_mappings[dest_specific_field] = dest_specific_field
    else:

        # Retrieve the field mappings and put them into the dict
        specific_field_mappings.update(get_field_mappings(_src_table, _dest_table))

        # For dest_specific_field that is not defined, map it to NULL
        for dest_specific_field in _dest_specific_fields:
            if dest_specific_field not in specific_field_mappings:
                specific_field_mappings[dest_specific_field] = NULL_VALUE

    return specific_field_mappings


def resolve_date_field_mappings(src_date_fields, dest_date_fields):
    """
    This function generates a list of date field mappings based on simple heuristics.
        1. if numbers of date fields are equal between src_date_fields and dest_date_fields,
        that means both have either two date fields (domain_date, domain_datetime)
        or four date fields (domain_start_date, domain_start_datetime, domain_end_date, domain_end_datetime).
        So we can map the corresponding date fields to each other.

        2. if numbers of date fields are not equal, one must have two fields (domain_date, domain_datetime),
        and the other must have four fields (domain_start_date, domain_start_datetime, domain_end_date, domain_end_datetime).
        We need to map the domain_date to domain_start_date and domain_datetime to domain_start_datetime

    :param src_date_fields: an array that contains the source date fields
    :param dest_date_fields: an array that contains the destination date fields
    :return: a list of date field mappings
    """
    date_field_mappings = OrderedDict()

    if len(src_date_fields) == len(dest_date_fields):
        for src_date_suffix in src_date_fields:
            if src_date_suffix in dest_date_fields:
                _src_field = src_date_fields[src_date_suffix]
                _dest_field = dest_date_fields[src_date_suffix]
                date_field_mappings[_dest_field] = _src_field
    else:
        for dest_date_suffix in dest_date_fields:
            if '_end' in dest_date_suffix:
                src_date_suffix = None
            elif '_start' in dest_date_suffix:
                src_date_suffix = dest_date_suffix[len('_start'):]
            else:
                src_date_suffix = '_start{}'.format(dest_date_suffix)

            _src_field = src_date_fields[src_date_suffix] if src_date_suffix is not None else NULL_VALUE
            _dest_field = dest_date_fields[dest_date_suffix]
            date_field_mappings[_dest_field] = _src_field

    return date_field_mappings


def create_domain_field_dict():
    """
    This function categorizes the CDM table fields and puts them into different 'buckets' of the dictionary.
    The purpose of creating this dictionary is to facilitate the mapping of the fields in the downstream process.
    person_id

    :return: a dictionary that contains CDM table fields
    """
    domain_fields = OrderedDict()
    for domain_table in domain_mapping.DOMAIN_TABLE_NAMES:
        _field_mappings = OrderedDict()
        common_field_mappings = OrderedDict()
        date_field_mappings = OrderedDict()
        specific_fields = []
        domain = get_domain(domain_table)
        domain_id_field = get_domain_id_field(domain_table)

        for field_name in get_domain_fields(domain_table):

            # Added a special check for drug_exposure because the drug_exposure columns don't follow the same pattern
            # E.g. drug_exposure_start_time doesn't follow the pattern {domain}_start_datetime
            if field_name.find(domain_table) != -1:
                field_suffix = re.sub(domain_table, '', field_name)
            else:
                field_suffix = re.sub(domain.lower(), '', field_name)

            # Put different types of fields into dictionary
            if field_suffix in COMMON_DOMAIN_FIELD_SUFFIXES:
                common_field_mappings[field_suffix] = field_name
            elif field_suffix in DATE_FIELD_SUFFIXES:
                date_field_mappings[field_suffix] = field_name
            elif field_name in COMMON_DOMAIN_FIELD_SUFFIXES:
                common_field_mappings[field_name] = field_name
            elif field_name != domain_id_field:
                specific_fields.append(field_name)

        _field_mappings[DOMAIN_COMMON_FIELDS] = common_field_mappings
        _field_mappings[DOMAIN_SPECIFIC_FIELDS] = specific_fields
        _field_mappings[DOMAIN_DATE_FIELDS] = date_field_mappings
        domain_fields[domain_table] = _field_mappings

    return domain_fields


def get_domain_fields(_domain_table):
    """
    This function retrieves all field names of a CDM table except for the id column such as condition_occurrence_id
    :param _domain_table:
    :return:
    """
    id_field = get_domain_id_field(_domain_table)
    fields = CDM_TABLE_SCHEMAS[_domain_table]
    return [field[NAME_FIELD] for field in fields if field[NAME_FIELD] != id_field]


if __name__ == '__main__':

    with open(resources.field_mappings_replaced_path, 'w') as fr:
        fr.write(FIELD_MAPPING_HEADER)
        field_dict = create_domain_field_dict()
        for src_table in DOMAIN_TABLE_NAMES:
            for dest_table in DOMAIN_TABLE_NAMES:
                if src_table == dest_table or exist_domain_mappings(src_table, dest_table):
                    field_mappings = generate_field_mappings(src_table, dest_table, field_dict[src_table],
                                                             field_dict[dest_table])
                    for dest_field, src_field in field_mappings.iteritems():
                        translation = 1 if TYPE_CONCEPT_SUFFIX in src_field \
                                           and TYPE_CONCEPT_SUFFIX in dest_field \
                                           and src_table != dest_table else 0

                        field_mapping = ATTRIBUTE_MAPPING_TEMPLATE.format(
                            src_table=src_table,
                            dest_table=dest_table,
                            src_field=src_field,
                            dest_field=dest_field,
                            translation=translation)
                        fr.write(field_mapping)
                        fr.write('\n')
        fr.close()

