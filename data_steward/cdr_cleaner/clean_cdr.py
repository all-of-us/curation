"""
A module to serve as the entry point to the cdr_cleaner package.

It gathers the list of query strings to execute and sends them
to the query engine.
"""
# Python imports
import logging

# Third party imports
import app_identity

# Project imports
import bq_utils
from cdr_cleaner import clean_cdr_engine as clean_engine

# cleaning rule imports
from cdr_cleaner.cleaning_rules import clean_years as clean_years
from cdr_cleaner.cleaning_rules import domain_alignment as domain_alignment
from cdr_cleaner.cleaning_rules import drug_refills_days_supply as drug_refills_supply
from cdr_cleaner.cleaning_rules import replace_standard_id_in_domain_tables as replace_standard_concept_ids
from cdr_cleaner.cleaning_rules import id_deduplicate as id_dedup
from cdr_cleaner.cleaning_rules import negative_ages as neg_ages
from cdr_cleaner.cleaning_rules import no_data_30_days_after_death as no_data_30days_after_death
from cdr_cleaner.cleaning_rules import null_invalid_foreign_keys as null_foreign_key
from cdr_cleaner.cleaning_rules import person_id_validator as person_validator
from cdr_cleaner.cleaning_rules import temporal_consistency as bad_end_dates
from cdr_cleaner.cleaning_rules import valid_death_dates as valid_death_dates
from cdr_cleaner.cleaning_rules import drug_refills_days_supply as drug_refills_supply
from cdr_cleaner.cleaning_rules import domain_alignment as domain_mapping
from cdr_cleaner.cleaning_rules import fill_free_text_source_value as fill_source_value
from cdr_cleaner.cleaning_rules import populate_route_ids as populate_routes
from constants.cdr_cleaner import clean_cdr as clean_cdr_consts


LOGGER = logging.getLogger(__name__)


def _gather_ehr_queries(project_id, dataset_id):
    """
    gathers all the queries required to clean ehr dataset

    :param project_id: project name
    :param dataset_id: ehr dataset name
    :return: returns list of queries
    """
    query_list = []
    query_list.extend(id_dedup.get_id_deduplicate_queries(project_id, dataset_id))
    return query_list


def _gather_rdr_queries(project_id, dataset_id):
    """
    gathers all the queries required to clean rdr dataset

    :param project_id: project name
    :param dataset_id: rdr dataset name
    :return: returns list of queries
    """
    query_list = []
    query_list.extend(id_dedup.get_id_deduplicate_queries(project_id, dataset_id))
    query_list.extend(clean_years.get_year_of_birth_queries(project_id, dataset_id))
    query_list.extend(neg_ages.get_negative_ages_queries(project_id, dataset_id))
    query_list.extend(bad_end_dates.get_bad_end_date_queries(project_id, dataset_id))
    return query_list


def _gather_ehr_rdr_queries(project_id, dataset_id):
    """
    gathers all the queries required to clean ehr_rdr dataset

    :param project_id: project name
    :param dataset_id: ehr_rdr dataset name
    :return: returns list of queries
    """
    query_list = []
    query_list.extend(replace_standard_concept_ids.replace_standard_id_in_domain_tables(project_id, dataset_id))
    query_list.extend(domain_alignment.domain_alignment(project_id, dataset_id))
    query_list.extend(id_dedup.get_id_deduplicate_queries(project_id, dataset_id))
    query_list.extend(null_foreign_key.null_invalid_foreign_keys(project_id, dataset_id))
    query_list.extend(clean_years.get_year_of_birth_queries(project_id, dataset_id))
    query_list.extend(neg_ages.get_negative_ages_queries(project_id, dataset_id))
    query_list.extend(bad_end_dates.get_bad_end_date_queries(project_id, dataset_id))
    query_list.extend(no_data_30days_after_death.no_data_30_days_after_death(project_id, dataset_id))
    query_list.extend(valid_death_dates.get_valid_death_date_queries(project_id, dataset_id))
    query_list.extend(drug_refills_supply.get_days_supply_refills_queries(project_id, dataset_id))
    query_list.extend(populate_routes.get_route_mapping_queries(project_id, dataset_id))
    return query_list


def _gather_ehr_rdr_de_identified_queries(project_id, dataset_id):
    """
    gathers all the queries required to clean de_identified dataset

    :param project_id: project name
    :param dataset_id: de_identified dataset name
    :return: returns list of queries
    """
    query_list = []
    query_list.extend(id_dedup.get_id_deduplicate_queries(project_id, dataset_id))
    query_list.extend(clean_years.get_year_of_birth_queries(project_id, dataset_id))
    query_list.extend(neg_ages.get_negative_ages_queries(project_id, dataset_id))
    query_list.extend(bad_end_dates.get_bad_end_date_queries(project_id, dataset_id))
    query_list.extend(person_validator.get_person_id_validation_queries(project_id, dataset_id))
    query_list.extend(valid_death_dates.get_valid_death_date_queries(project_id, dataset_id))
    query_list.extend(drug_refills_supply.get_days_supply_refills_queries(project_id, dataset_id))
    query_list.extend(fill_source_value.get_fill_freetext_source_value_fields_queries(project_id, dataset_id))
    query_list.extend(populate_routes.get_route_mapping_queries(project_id, dataset_id))
    return query_list


def _gather_unioned_ehr_queries(project_id, dataset_id):
    """
    gathers all the queries required to clean unioned_ehr dataset

    :param project_id: project name
    :param dataset_id: unioned_ehr dataset name
    :return: returns list of queries
    """
    query_list = []
    query_list.extend(id_dedup.get_id_deduplicate_queries(project_id, dataset_id))
    query_list.extend(clean_years.get_year_of_birth_queries(project_id, dataset_id))
    query_list.extend(neg_ages.get_negative_ages_queries(project_id, dataset_id))
    query_list.extend(bad_end_dates.get_bad_end_date_queries(project_id, dataset_id))
    query_list.extend(valid_death_dates.get_valid_death_date_queries(project_id, dataset_id))
    query_list.extend(drug_refills_supply.get_days_supply_refills_queries(project_id, dataset_id))
    query_list.extend(populate_routes.get_route_mapping_queries(project_id, dataset_id))
    return query_list


def clean_rdr_dataset(project_id=None, dataset_id=None):
    """
    Run all clean rules defined for the rdr dataset.

    :param project_id:  Name of the BigQuery project.
    :param dataset_id:  Name of the dataset to clean
    """
    if dataset_id is None or dataset_id == '' or dataset_id.isspace():
        dataset_id = bq_utils.get_rdr_dataset_id()
        LOGGER.info('Dataset is unspecified.  Using default value of:\t%s', dataset_id)

    query_list = _gather_rdr_queries(project_id, dataset_id)

    LOGGER.info("Cleaning rdr_dataset")
    clean_engine.clean_dataset(project_id, dataset_id, query_list)


def clean_ehr_dataset(project_id=None, dataset_id=None):
    """
    Run all clean rules defined for the ehr dataset.

    :param project_id:  Name of the BigQuery project.
    :param dataset_id:  Name of the dataset to clean
    """
    if dataset_id is None or dataset_id == '' or dataset_id.isspace():
        dataset_id = bq_utils.get_dataset_id()
        LOGGER.info('Dataset is unspecified.  Using default value of:\t%s', dataset_id)

    query_list = _gather_ehr_queries(project_id, dataset_id)

    LOGGER.info("Cleaning ehr_dataset")
    clean_engine.clean_dataset(project_id, dataset_id, query_list)


def clean_unioned_ehr_dataset(project_id=None, dataset_id=None):
    """
    Run all clean rules defined for the unioned ehr dataset.

    :param project_id:  Name of the BigQuery project.
    :param dataset_id:  Name of the dataset to clean
    """
    if dataset_id is None or dataset_id == '' or dataset_id.isspace():
        dataset_id = bq_utils.get_unioned_dataset_id()
        LOGGER.info('Dataset is unspecified.  Using default value of:\t%s', dataset_id)

    query_list = _gather_unioned_ehr_queries(project_id, dataset_id)

    LOGGER.info("Cleaning unioned_dataset")
    clean_engine.clean_dataset(project_id, dataset_id, query_list)


def clean_ehr_rdr_dataset(project_id=None, dataset_id=None):
    """
    Run all clean rules defined for the ehr and rdr dataset.

    :param project_id:  Name of the BigQuery project.
    :param dataset_id:  Name of the dataset to clean
    """
    if dataset_id is None or dataset_id == '' or dataset_id.isspace():
        dataset_id = bq_utils.get_ehr_rdr_dataset_id()
        LOGGER.info('Dataset is unspecified.  Using default value of:\t%s', dataset_id)

    query_list = _gather_ehr_rdr_queries(project_id, dataset_id)

    LOGGER.info("Cleaning ehr_rdr_dataset")
    clean_engine.clean_dataset(project_id, dataset_id, query_list)


def clean_ehr_rdr_de_identified_dataset(project_id=None, dataset_id=None):
    """
    Run all clean rules defined for the deidentified ehr and rdr dataset.

    :param project_id:  Name of the BigQuery project.
    :param dataset_id:  Name of the dataset to clean
    """
    if dataset_id is None or dataset_id == '' or dataset_id.isspace():
        dataset_id = bq_utils.get_combined_deid_dataset_id()
        LOGGER.info('Dataset is unspecified.  Using default value of:\t%s', dataset_id)

    query_list = _gather_ehr_rdr_de_identified_queries(project_id, dataset_id)

    LOGGER.info("Cleaning de-identified dataset")
    clean_engine.clean_dataset(project_id, dataset_id, query_list)


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
