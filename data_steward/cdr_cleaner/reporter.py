"""
A package to generate a csv file type report for cleaning rules.
"""
# Python imports
import csv
import inspect
import logging
import os
from copy import copy

# Project imports
import cdr_cleaner.args_parser as cleaning_parser
import cdr_cleaner.clean_cdr as control
import cdr_cleaner.clean_cdr_engine as engine
import constants.cdr_cleaner.clean_cdr as cdr_consts
import constants.cdr_cleaner.reporter as report_consts
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule

LOGGER = logging.getLogger(__name__)


def parse_args(raw_args=None):
    """
    Parse command line arguments for the cdr_cleaner package reporting utility.

    :param raw_args: The argument to parse, if passed as a list form another
        module.  If None, the command line is parsed.

    :returns: a namespace object for the given arguments.
    """
    parser = cleaning_parser.get_report_parser()
    return parser.parse_args(raw_args)


def get_function_info(func, fields_list):
    """
    For a function, provide the info that can be provided.

    Defaults all requested fields to 'unknown' values.  Adds a 'name' and
    'module' field even if not requested to give more information in the clean
    rules report.  Adds the documentation string of the function only if a
    description is requested.

    :param func:  The function that is part of the clean rules report.
    :param fields_list: The list of fields the user requested.

    :return: A dictionary of values representing the known and requested
        fields for the given function.
    """
    func_info = dict()

    for field in fields_list:
        func_info[field] = report_consts.UNKNOWN

    if report_consts.NAME not in fields_list:
        LOGGER.info(
            f"Adding '{report_consts.NAME}' field to notify report reader this "
            f"function ({func.__qualname__}), "
            "needs to be implemented as a class.")

    func_info[report_consts.NAME] = func.__name__

    if report_consts.MODULE not in fields_list:
        LOGGER.info(
            f"Adding '{report_consts.MODULE}' field to notify report reader this "
            f"function ({func.__qualname__}) "
            "needs to be implemented as a class.")

    func_info[report_consts.MODULE] = func.__module__

    if report_consts.DESCRIPTION in fields_list:
        func_info[report_consts.DESCRIPTION] = func.__doc__

    return func_info


def get_class_info(instance: object, fields_list: list) -> dict:
    """
    Returns the field info for a rule based on a class

    Defaults all requested fields to "unknown" values.  Adds a "name" and
    "module" field, even if not requseted, to give more information in the clean
    rules report.

    :param instance: Instance of class we be lookin' at.
    :param fields_list: The list of fields to locate.

    :return: A dictionary of values representing the known and requested
        fields for the given class
    """

    rule_info = {}

    # seed rule info with a buncha unkonwns
    for field in fields_list:
        rule_info[field] = report_consts.UNKNOWN

    # quickly determine if this class extends the base cleaning rule
    if not issubclass(type(instance), BaseCleaningRule):
        LOGGER.exception(
            f'Provided object {type(instance)} is not a subclass of {BaseCleaningRule}'
        )
        return rule_info

    LOGGER.info(f'{type(instance)} is a subclass of {BaseCleaningRule}')

    for field in fields_list:
        try:
            value = 'NO DATA'
            if field in report_consts.FIELDS_PROPERTIES_MAP:
                func = report_consts.FIELDS_PROPERTIES_MAP[field]
                value = getattr(instance, func, 'no data')
            elif field in report_consts.FIELDS_METHODS_MAP:
                func = report_consts.FIELDS_METHODS_MAP[field]
                value = getattr(instance, func, 'no data')()
            elif field in report_consts.CLASS_ATTRIBUTES_MAP:
                func = report_consts.CLASS_ATTRIBUTES_MAP[field]

                value = None
                for item in func.split('.'):
                    if not value:
                        value = getattr(instance, item)
                    else:
                        value = getattr(value, item)

            rule_info[field] = value

        except AttributeError:
            # an error occurred trying to access an expected attribute.
            # did the base class definition change recently?
            LOGGER.exception(
                f'An error occurred trying to get the value for {field}')
            rule_info[field] = report_consts.UNKNOWN
        except NotImplementedError:
            # some methods like get_sandbox_tablenames are not implemented for all rules
            LOGGER.exception(
                f'A NotImplementedError occurred trying to get the value for {field}')
            rule_info[field] = report_consts.UNKNOWN
    return rule_info


def get_stage_elements(data_stage, fields_list):
    """
    Return the field info for rules defined for this data_stage.

    For the given data_stage, this will determine the values of the
    requested fields.  This information will be returned as a list
    of dictionaries to preserve the information order.  Each dictionary
    contains values for the report for a single cleaning rule.

    :param data_stage: The data stage to report for.
    :param fields_list: The user defined fields to report back.

    :returns: a list of dictionaries representing the requested fields.
    """
    report_rows = []
    for rule_def in control.DATA_STAGE_RULES_MAPPING.get(data_stage, []):
        LOGGER.info(f'Testing rule definition {rule_def}...')

        # first element should be either instance of rule class or legacy rule function
        rule_type = rule_def[0]

        # determine which we're dealing with
        if inspect.isclass(rule_type):
            LOGGER.info(f'{rule_type} is a class')
            # this is a classed cleaning rule
            sig = inspect.signature(rule_type)
            params = ['foo'] * len(sig.parameters)
            LOGGER.info(
                f"Attempting to instantiate {rule_type}: {type(rule_type)}({', '.join(params)})"
            )
            instance = rule_type(*params)
            rule_info = get_class_info(instance, fields_list)
        else:
            # an error occurred indicating this is not a rule extending the
            # base cleaning rule.  provide the info we can and move on.
            LOGGER.exception(f'{rule_type} is not a class')
            # this is a function
            rule_info = get_function_info(rule_type, fields_list)

        report_rows.append(rule_info)

    return report_rows


def separate_sql_statements(unformatted_values):
    """
    Separate SQL statements into items with other identical fields.

    This must maintain the SQL statement order.  For example, if the user
    requests the fields 'name module sql', the input for this function will be
    a list of dictionaries of the where each dictionary will have the keys,
    '{name: <value>, module: <value>,  sql: "unknown" or [{dictionary attributes}]}'.
    The purpose of this function is to break into the 'sql' values (because a
    cleaning rule may contain more than one sql statement) and copy the other
    field values.

    For example, the following input:
    [{'name': 'foo', 'sql':[{'query': 'q1',...},{'query': 'q2'...}]}]

    Should be formatted as:
    [{'name': 'foo', 'sql': 'q1'}
     {'name': 'foo', 'sql': 'q2'}]
    """
    formatted_values = []
    for rule_values in unformatted_values:
        sql_list = []

        # gather the queries as a list
        sql_value = rule_values.get(report_consts.SQL, [])
        for query_dict in sql_value:
            try:
                sql_list.append(
                    query_dict.get(report_consts.QUERY, report_consts.UNKNOWN))
            except AttributeError:
                if sql_value == report_consts.UNKNOWN:
                    sql_list.append(report_consts.UNKNOWN)
                    break

                raise

        if sql_list:
            # generate a dictionary for each query
            for query in sql_list:
                # get a fresh copy for each rule.
                separated = copy(rule_values)
                separated[report_consts.SQL] = query.strip()
                formatted_values.append(separated)
        else:
            separated = copy(rule_values)
            separated[report_consts.SQL] = report_consts.UNKNOWN
            formatted_values.append(separated)

    return formatted_values


def format_values(rules_values, fields_list):
    """
    Format the fields' values for input to the DictWriter.

    This formats fields whose values are lists as joined strings.
    If the sql field is chosen, a line break is used to joing the sql strings.

    :param rules_values: The list of dictionaries containing field/value pairs for
        each field specified via arguments for each cleaning rule.
    :param fields_list: The list of fields to inspect
    """
    formatted_values = []

    if report_consts.SQL in fields_list:
        LOGGER.debug("SQL field exists")
        rules_values = separate_sql_statements(rules_values)

    for rule_values in rules_values:
        field_values = {}
        for field, value in rule_values.items():
            if isinstance(value, list):
                try:
                    value = ', '.join(value)
                except TypeError:
                    LOGGER.exception(f"erroneous field is {field}\n"
                                     f"erroneous value is {value}")
                    raise

            field_values[field] = value

        formatted_values.append(field_values)

    return formatted_values


def check_field_list_validity(fields_list, required_fields_dict):
    """
    Helper function to create a valid fields list for writing the CSV file.

    The CSV writer is a dictionary writer.

    :param fields_list: list of fields provided via the parse arguments
        command.  these are user requested feilds.
    :param required_fields_dict: The list of dictionaries that are actually
        generated by the get_stage_elements function.  It may have some
        additional fields that are not user specified.
    :returns: a list of fields that should be written to the csv file
    """
    known_fields = set()
    for value_dict in required_fields_dict:
        keys = value_dict.keys()
        known_fields.update(keys)

    final_fields = [field for field in fields_list]

    for field in known_fields:
        if field not in fields_list:
            final_fields.append(field)

    return final_fields


def write_csv_report(output_filepath, stages_list, fields_list):
    """
    Write a csv file for the indicated stages and fields.

    :param output_filepath: the filepath of a csv file.
    :param stages_list: a list of strings indicating the data stage to
        report for.  Should match to a stage value in
        curation/data_steward/constants/cdr_cleaner/clean_cdr.py DataStage.
    :param fields_list: a list of string fields that will be added to the
        csv file.
    """
    if not output_filepath.endswith('.csv'):
        raise RuntimeError(f"This file is not a csv file: {output_filepath}.")

    output_list = []
    for stage in stages_list:
        # get the fields and values
        required_fields_dict = get_stage_elements(stage, fields_list)
        # format dictionaries for writing
        required_fields_dict = format_values(required_fields_dict, fields_list)
        output_list.extend(required_fields_dict)

    fields_list = check_field_list_validity(fields_list, output_list)

    # write the contents to a csv file
    with open(output_filepath, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile,
                                fields_list,
                                delimiter=',',
                                lineterminator=os.linesep,
                                quoting=csv.QUOTE_ALL)
        writer.writeheader()
        for info in output_list:
            writer.writerow(info)


def main(raw_args=None):
    """
    Entry point for the clean rules reporter module.

    If you provide a list of arguments and settings, these will be parsed.
    If you leave this blank, the command line arguments are parsed.  This allows
    this module to be easily called from other python modules.

    :param raw_args: The list of arguments to parse.  Defaults to parsing the
        command line.
    """
    args = parse_args(raw_args)
    engine.add_console_logging(args.console_log)

    if cdr_consts.DataStage.UNSPECIFIED.value in args.data_stage:
        args.data_stage = [
            s.value
            for s in cdr_consts.DataStage
            if s is not cdr_consts.DataStage.UNSPECIFIED
        ]
        LOGGER.info(
            f"Data stage was {cdr_consts.DataStage.UNSPECIFIED.value}, so all stages "
            f"will be reported on:  {args.data_stage}")

    write_csv_report(args.output_filepath, args.data_stage, args.fields)

    LOGGER.info("Finished the reporting module")


if __name__ == '__main__':
    # run as main
    main()
