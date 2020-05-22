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
from constants.cdr_cleaner.clean_cdr import DataStage as stage

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


def get_stage_elements(stage, fields_list):

    pass


def write_csv_report(output_filepath, stages_list, fields_list):
    """
    Write a csv file for the indicated stages and fields.

    :param output_filepath: the filepath of a csv file.
    :param stages_list: a list of strings indicating the data stage to
        report for.  Should match to a stage value in
        curation/data_steward/constants/cdr_cleaner.clean_cdr.py DataStage.
    :param fields_list: a list of string fields that will be added to the
        csv file.
    """
    if output_filepath.endswith('.csv'):
        raise RuntimeError(f"This file is not a csv file: {output_filepath}.")

    for stage in stages_list:
        required_fields = get_stage_elements(stage, fields_list)


def main(raw_args=None):
    args = parse_args()
    engine.add_console_logging(args.console_log)
    LOGGER.info(f"{args}")

    if stage.UNSPECIFIED.value in args.data_stage:
        args.data_stage = [s.value for s in stage if s is not stage.UNSPECIFIED]
        LOGGER.info(f"Data stage was {stage.UNSPECIFIED.value}, so all stages "
                    f"will be reported on:  {args.data_stage}")

    write_csv_report(args.output_filepath, args.data_stage, args.fields)

    LOGGER.info("Finished the reporting module")


if __name__ == '__main__':
    # run as main
    main()
