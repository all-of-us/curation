import argparse

SHORT_ARGUMENT = 'argument'
LONG_ARGUMENT = 'long_argument'
ACTION = 'action'
DEST = 'dest'
HELP = 'help'
REQUIRED = 'required'


def parse_args():
    return default_parse_args()


def default_parse_args(additional_arguments=None):
    parser = argparse.ArgumentParser(description='Parse project_id and dataset_id',
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-p', '--project_id',
                        action='store', dest='project_id',
                        help='Project associated with the input and output datasets',
                        required=True)
    parser.add_argument('-d', '--dataset_id',
                        action='store', dest='dataset_id',
                        help='Dataset where cleaning rules are to be applied',
                        required=True)
    parser.add_argument('-s', '--console_log', dest='console_log', action='store_true',
                        help='Send logs to console')

    if additional_arguments is not None:
        for argument in additional_arguments:
            parser.add_argument(argument[SHORT_ARGUMENT],
                                argument[LONG_ARGUMENT],
                                dest=argument[DEST],
                                action=argument[ACTION],
                                help=argument[HELP])

    return parser.parse_args()
