"""
A module to serve as the entry point to the cleaners package.

It gathers the list of query strings to execute and sends them
to the query engine.
"""
# Python imports
import logging

# Third party imports
from google.appengine.api import app_identity

# Project imports
import bq_utils
import cleaners.clean_cdr_engine as clean_engine
import cleaners.query_generators.clean_years as clean_years
import cleaners.query_generators.id_deduplicate as id_dedup
import cleaners.query_generators.negative_ages as neg_ages
import cleaners.query_generators.null_invalid_foreign_keys as null_foreign_key
import cleaners.query_generators.person_id_validator as person_validator
import constants.cleaners.clean_cdr as clean_cdr_consts


LOGGER = logging.getLogger(__name__)


def _gather_ehr_queries(project, dataset):
    """
    gathers all the queries required to clean ehr dataset

    :param project: project name
    :param dataset: ehr dataset name
    :return: returns list of queries
    """
    query_list = []
    query_list.extend(id_dedup.get_id_deduplicate_queries(project, dataset))
    return query_list


def _gather_rdr_queries(project, dataset):
    """
    gathers all the queries required to clean rdr dataset

    :param project: project name
    :param dataset: rdr dataset name
    :return: returns list of queries
    """
    query_list = []
    query_list.extend(id_dedup.get_id_deduplicate_queries(project, dataset))
    query_list.extend(clean_years.get_year_of_birth_queries(project, dataset))
    query_list.extend(neg_ages.get_negative_ages_queries(project, dataset))
    return query_list


def _gather_ehr_rdr_queries(project, dataset):
    """
    gathers all the queries required to clean ehr_rdr dataset

    :param project: project name
    :param dataset: ehr_rdr dataset name
    :return: returns list of queries
    """
    query_list = []
    query_list.extend(id_dedup.get_id_deduplicate_queries(project, dataset))
    query_list.extend(null_foreign_key.null_invalid_foreign_keys(project, dataset))
    query_list.extend(clean_years.get_year_of_birth_queries(project, dataset))
    query_list.extend(neg_ages.get_negative_ages_queries(project, dataset))
    return query_list


def _gather_ehr_rdr_de_identified_queries(project, dataset):
    """
    gathers all the queries required to clean de_identified dataset

    :param project: project name
    :param dataset: de_identified dataset name
    :return: returns list of queries
    """
    query_list = []
    query_list.extend(id_dedup.get_id_deduplicate_queries(project, dataset))
    query_list.extend(clean_years.get_year_of_birth_queries(project, dataset))
    query_list.extend(neg_ages.get_negative_ages_queries(project, dataset))
    query_list.extend(person_id_validator.get_person_id_validation_queries(project, dataset))
    return query_list


def _gather_unioned_ehr_queries(project, dataset):
    """
    gathers all the queries required to clean unioned_ehr dataset

    :param project: project name
    :param dataset: unioned_ehr dataset name
    :return: returns list of queries
    """
    query_list = []
    query_list.extend(id_dedup.get_id_deduplicate_queries(project, dataset))
    query_list.extend(clean_years.get_year_of_birth_queries(project, dataset))
    query_list.extend(neg_ages.get_negative_ages_queries(project, dataset))
    return query_list


def clean_rdr_dataset(project=None, dataset=None):
    """
    Run all clean rules defined for the rdr dataset.

    :param project:  Name of the BigQuery project.
    :param dataset:  Name of the dataset to clean
    """
    if dataset is None or dataset == '' or dataset.isspace():
        dataset = bq_utils.get_rdr_dataset_id()
        LOGGER.info('Dataset is unspecified.  Using default value of:\t%s', dataset)

    query_list = _gather_rdr_queries(project, dataset)

    LOGGER.info("Cleaning rdr_dataset")
    clean_engine.clean_dataset(project, dataset, query_list)


def clean_ehr_dataset(project=None, dataset=None):
    """
    Run all clean rules defined for the ehr dataset.

    :param project:  Name of the BigQuery project.
    :param dataset:  Name of the dataset to clean
    """
    if dataset is None or dataset == '' or dataset.isspace():
        dataset = bq_utils.get_dataset_id()
        LOGGER.info('Dataset is unspecified.  Using default value of:\t%s', dataset)

    query_list = _gather_ehr_queries(project, dataset)

    LOGGER.info("Cleaning ehr_dataset")
    clean_engine.clean_dataset(project, dataset, query_list)


def clean_unioned_ehr_dataset(project=None, dataset=None):
    """
    Run all clean rules defined for the unioned ehr dataset.

    :param project:  Name of the BigQuery project.
    :param dataset:  Name of the dataset to clean
    """
    if dataset is None or dataset == '' or dataset.isspace():
        dataset = bq_utils.get_unioned_dataset_id()
        LOGGER.info('Dataset is unspecified.  Using default value of:\t%s', dataset)

    query_list = _gather_unioned_ehr_queries(project, dataset)

    LOGGER.info("Cleaning unioned_dataset")
    clean_engine.clean_dataset(project, dataset, query_list)


def clean_ehr_rdr_dataset(project=None, dataset=None):
    """
    Run all clean rules defined for the ehr and rdr dataset.

    :param project:  Name of the BigQuery project.
    :param dataset:  Name of the dataset to clean
    """
    if dataset is None or dataset == '' or dataset.isspace():
        dataset = bq_utils.get_ehr_rdr_dataset_id()
        LOGGER.info('Dataset is unspecified.  Using default value of:\t%s', dataset)

    query_list = _gather_ehr_rdr_queries(project, dataset)

    LOGGER.info("Cleaning ehr_rdr_dataset")
    clean_engine.clean_dataset(project, dataset, query_list)


def clean_ehr_rdr_de_identified_dataset(project=None, dataset=None):
    """
    Run all clean rules defined for the deidentified ehr and rdr dataset.

    :param project:  Name of the BigQuery project.
    :param dataset:  Name of the dataset to clean
    """
    if dataset is None or dataset == '' or dataset.isspace():
        dataset = bq_utils.get_combined_deid_dataset_id()
        LOGGER.info('Dataset is unspecified.  Using default value of:\t%s', dataset)

    query_list = _gather_ehr_rdr_de_identified_queries(project, dataset)

    LOGGER.info("Cleaning de-identified dataset")
    clean_engine.clean_dataset(project, dataset, query_list)


def get_dataset_and_project_names():
    """
    Get project and dataset names from environment variables.

    :return: A dictionary of dataset names and project name
    """
    project_and_dataset_names = dict()
    project_and_dataset_names[clean_cdr_consts.EHR_DATASET] = bq_utils.get_dataset_id()
    project_and_dataset_names[clean_cdr_consts.UNIONED_EHR_DATASET] = bq_utils.get_unioned_dataset_id()
    project_and_dataset_names[clean_cdr_consts.RDR_DATASET] = bq_utils.get_rdr_dataset_id()
    project_and_dataset_names[clean_cdr_consts.EHR_RDR_DATASET] = bq_utils.get_ehr_rdr_dataset_id()
    project_and_dataset_names[clean_cdr_consts.EHR_RDR_DE_IDENTIFIED] = bq_utils.get_combined_deid_dataset_id()
    project_and_dataset_names[clean_cdr_consts.PROJECT] = app_identity.get_application_id()

    return project_and_dataset_names


def clean_all_cdr():
    """
    Runs cleaning rules on all the datasets
    """
    id_dict = get_dataset_and_project_names()
    project = id_dict[clean_cdr_consts.PROJECT]

    clean_ehr_dataset(project, id_dict[clean_cdr_consts.EHR_DATASET])
    clean_unioned_ehr_dataset(project, id_dict[clean_cdr_consts.UNIONED_EHR_DATASET])
    clean_rdr_dataset(project, id_dict[clean_cdr_consts.RDR_DATASET])
    clean_ehr_rdr_dataset(project, id_dict[clean_cdr_consts.EHR_RDR_DATASET])
    clean_ehr_rdr_de_identified_dataset(
        project, id_dict[clean_cdr_consts.EHR_RDR_DE_IDENTIFIED]
    )


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('-s', action='store_true', help='Send logs to console')
    args = parser.parse_args()
    clean_engine.add_console_logging(args.s)

    clean_all_cdr()
