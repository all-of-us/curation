"""
A package to generate a csv file type report for cleaning rules.
"""
# Python imports
import logging

# Third party imports

# Project imports
import cdr_cleaner.args_parser as cleaning_parser
import cdr_cleaner.clean_cdr as control
import cdr_cleaner.clean_cdr_engine as engine

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


if __name__ == '__main__':
    # run as main
    args = parse_args()

    engine.add_console_logging(args.console_log)
    LOGGER.info("Set up arguments and logging")
    LOGGER.info(f"{args}")
