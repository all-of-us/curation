import argparse

import constants.cdr_cleaner.clean_cdr as consts
from cdr_cleaner.reporter import FIELDS_METHODS_MAP, FIELDS_ATTRIBUTES_MAP

SHORT_ARGUMENT = 'argument'
LONG_ARGUMENT = 'long_argument'
ACTION = 'action'
DEST = 'dest'
HELP = 'help'
REQUIRED = 'required'


def parse_args():
    return default_parse_args()


def default_parse_args(additional_arguments=None):
    parser = get_argument_parser()

    if additional_arguments is not None:
        for argument in additional_arguments:
            parser.add_argument(argument[SHORT_ARGUMENT],
                                argument[LONG_ARGUMENT],
                                dest=argument[DEST],
                                action=argument[ACTION],
                                help=argument[HELP],
                                required=argument[REQUIRED])

    return parser.parse_args()


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

    methods = [k for k, _ in FIELDS_METHODS_MAP.items()]
    attrs = [k for k, _ in FIELDS_ATTRIBUTES_MAP.items()]
    fields = methods + attrs

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
