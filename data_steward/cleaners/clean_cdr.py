"""
A module to serve as the entry point to the cleaners package.
"""
# python imports
import logging

# Project imports
import bq_utils
import clean_cdr_engine as clean_engine
import clean_rule_4 as rule_4

# import constants.bq_utils as bq_consts

LOGGER = logging.getLogger(__name__)


def clean_rdr_dataset(project=None, dataset=None):
    if dataset is None or dataset == '' or dataset.isspace():
        dataset = bq_utils.get_rdr_dataset_id()
        LOGGER.info('Dataset is unspecified.  Using default value of:\t%s', dataset)

    LOGGER.info("Cleaning rdr_dataset")
    rule_4.run_clean_rule_4(project, dataset)


def clean_ehr_dataset(project=None, dataset=None):
    if dataset is None or dataset == '' or dataset.isspace():
        dataset = bq_utils.get_dataset_id()
        LOGGER.info('Dataset is unspecified.  Using default value of:\t%s', dataset)

    LOGGER.info("Cleaning ehr_dataset")
    rule_4.run_clean_rule_4(project, dataset)


def clean_unioned_ehr_dataset(project=None, dataset=None):
    if dataset is None or dataset == '' or dataset.isspace():
        dataset = bq_utils.get_unioned_dataset_id()
        LOGGER.info('Dataset is unspecified.  Using default value of:\t%s', dataset)

    LOGGER.info("Cleaning unioned_dataset")
    rule_4.run_clean_rule_4(project, dataset)


def clean_ehr_rdr_dataset(project=None, dataset=None):
    if dataset is None or dataset == '' or dataset.isspace():
        dataset = bq_utils.get_ehr_rdr_dataset_id()
        clean_engine.LOGGER.info('Dataset is unspecified.  Using default value of:\t%s', dataset)

    LOGGER.info("Cleaning ehr_rdr_dataset")
    clean_engine.clean_dataset(project, dataset)


def clean_ehr_rdr_unidentified_dataset(project=None, dataset=None):
    if dataset is None or dataset == '' or dataset.isspace():
        dataset = bq_utils.get_combined_deid_dataset_id()
        clean_engine.LOGGER.info('Dataset is unspecified.  Using default value of:\t%s', dataset)

    LOGGER.info("Cleaning de-identified dataset")
    rule_4.run_clean_rule_4(project, dataset)


def clean_all_cdr():
    clean_rdr_dataset()
    clean_ehr_dataset()
    clean_unioned_ehr_dataset()
    clean_ehr_rdr_dataset()
    clean_ehr_rdr_unidentified_dataset()


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('-s', action='store_true', help=('Send logs to console'))
    args = parser.parse_args()
    clean_engine.add_console_logging(args.s)

    clean_all_cdr()
