import argparse

import constants.cdr_cleaner.clean_cdr as consts
from constants.cdr_cleaner.reporter import (CLASS_ATTRIBUTES_MAP,
                                            FIELDS_METHODS_MAP,
                                            FIELDS_PROPERTIES_MAP)

SHORT_ARGUMENT = 'argument'
LONG_ARGUMENT = 'long_argument'
ACTION = 'action'
DEST = 'dest'
HELP = 'help'
TYPE = 'type'
DEFAULT = 'default'
REQUIRED = 'required'


def get_argument_parser():
    parser = argparse.ArgumentParser(
        description='Parse project_id and dataset_id',
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-p',
                        '--project_id',
                        action='store',
                        dest='project_id',
                        help=('Project associated with the '
                              'input and output datasets.'),
                        required=True)
    parser.add_argument('-d',
                        '--dataset_id',
                        action='store',
                        dest='dataset_id',
                        help='Dataset where cleaning rules are to be applied',
                        required=True)
    parser.add_argument('-b',
                        '--sandbox_dataset_id',
                        action='store',
                        dest='sandbox_dataset_id',
                        help=('Dataset to store intermediate results '
                              'or changes in.'),
                        required=True)
    parser.add_argument('-s',
                        '--console_log',
                        dest='console_log',
                        action='store_true',
                        help='Send logs to console')
    parser.add_argument('-l',
                        '--list_queries',
                        dest='list_queries',
                        action='store_true',
                        help='List the generated SQL without executing')
    return parser


def default_parse_args(additional_arguments=None):
    parser = get_argument_parser()

    if additional_arguments is not None:
        for argument in additional_arguments:
            short_arg = argument.pop(SHORT_ARGUMENT, None)
            long_arg = argument.pop(LONG_ARGUMENT, None)
            parser.add_argument(short_arg, long_arg, **argument)

    return parser.parse_args()


def parse_args():
    return default_parse_args()


def check_output_filepath(filepath):
    """
    Check and return an appropriate output_filepath parameter.

    Ensures the file is a csv file.  Ensures a value is set.  If
    a value is not set or is not a csv, it will return a
    default value.

    :param filepath:  string filepath name

    :returns: a string representing a filepath location.
    """
    if filepath.endswith('.csv'):
        return filepath

    return "clean_rules_report.csv"


def get_report_parser():
    parser = argparse.ArgumentParser(description=(
        'Reporting parameters from the clean rule package.  This utility '
        'will provide a csv style report for project management.'))

    methods = [k for k in FIELDS_METHODS_MAP.keys()]
    props = [k for k in FIELDS_PROPERTIES_MAP.keys()]
    class_attrs = [k for k in CLASS_ATTRIBUTES_MAP.keys()]
    fields = methods + props + class_attrs

    parser.add_argument('-d',
                        '--data-stage',
                        dest='data_stage',
                        action='store',
                        choices=[stage.value for stage in consts.DataStage],
                        nargs='+',
                        help='The data stage(s) you would like to report for.',
                        required=True)

    parser.add_argument('-f',
                        '--fields',
                        dest='fields',
                        action='store',
                        choices=fields,
                        nargs='+',
                        help='The field(s) to format in the report.',
                        required=True)

    parser.add_argument('-c',
                        '--console-log',
                        dest='console_log',
                        action='store_true',
                        help='Send logs to console')

    parser.add_argument('-o',
                        '--output-file',
                        dest='output_filepath',
                        action='store',
                        default='clean_rules_report',
                        help=('The filepath and name of the output file.  '
                              'Currently, only csv files are supported.  '
                              'Defaults to producing "clean_rules_report.csv" '
                              'in your current working directory.'),
                        type=check_output_filepath)

    return parser


def add_kwargs_to_args(args_list, kwargs):
    """
    adds kwargs to the list of default arguments

    :param args_list: list of required args for clean_cdr.main()
    :param kwargs: dictionary with key word arguments passed
    :return: list of input args for clean_cdr.main()
    """
    if kwargs:
        kwargs_list = []
        for kwarg, kwarg_value in kwargs.items():
            if len(kwarg) == 1:
                kwargs_list.append(f'-{kwarg}')
            else:
                kwargs_list.append(f'--{kwarg}')
            kwargs_list.append(kwarg_value)
        return args_list + kwargs_list

    return args_list
