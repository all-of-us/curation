"""
A module to serve as the entry point to the cdr_cleaner package.

It gathers the list of query strings to execute and sends them
to the query engine.
"""
# Python imports
import logging

# Third party imports
from google.appengine.api import app_identity

# Project imports
import bq_utils
import cdr_cleaner.clean_cdr_engine as clean_engine

# cleaning rule imports
import cdr_cleaner.cleaning_rules.clean_years as clean_years
import cdr_cleaner.cleaning_rules.domain_alignment as domain_alignment
import cdr_cleaner.cleaning_rules.replace_standard_id_in_domain_tables as replace_standard_concept_ids
import cdr_cleaner.cleaning_rules.id_deduplicate as id_dedup
import cdr_cleaner.cleaning_rules.negative_ages as neg_ages
import cdr_cleaner.cleaning_rules.no_data_30_days_after_death as no_data_30days_after_death
import cdr_cleaner.cleaning_rules.null_invalid_foreign_keys as null_foreign_key
import cdr_cleaner.cleaning_rules.person_id_validator as person_validator
import cdr_cleaner.cleaning_rules.temporal_consistency as bad_end_dates
import cdr_cleaner.cleaning_rules.valid_death_dates as valid_death_dates
import cdr_cleaner.cleaning_rules.drug_refills_days_supply as drug_refills_supply
import cdr_cleaner.cleaning_rules.fill_free_text_source_value as fill_source_value
import cdr_cleaner.cleaning_rules.populate_route_ids as populate_routes
import cdr_cleaner.cleaning_rules.remove_records_with_wrong_date as remove_records_with_wrong_date
import cdr_cleaner.cleaning_rules.ensure_date_datetime_consistency as fix_datetimes
import constants.cdr_cleaner.clean_cdr as clean_cdr_consts


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
    query_list.extend(fix_datetimes.get_fix_incorrect_datetime_to_date_queries(project_id, dataset_id))
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
    query_list.extend(clean_years.get_year_of_birth_queries(project_id, dataset_id))
    query_list.extend(neg_ages.get_negative_ages_queries(project_id, dataset_id))
    query_list.extend(bad_end_dates.get_bad_end_date_queries(project_id, dataset_id))
    query_list.extend(no_data_30days_after_death.no_data_30_days_after_death(project_id, dataset_id))
    query_list.extend(valid_death_dates.get_valid_death_date_queries(project_id, dataset_id))
    query_list.extend(drug_refills_supply.get_days_supply_refills_queries(project_id, dataset_id))
    query_list.extend(populate_routes.get_route_mapping_queries(project_id, dataset_id))
    query_list.extend(fix_datetimes.get_fix_incorrect_datetime_to_date_queries(project_id, dataset_id))
    query_list.extend(remove_records_with_wrong_date.get_remove_records_with_wrong_date_queries(project_id, dataset_id))
    query_list.extend(null_foreign_key.null_invalid_foreign_keys(project_id, dataset_id))
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
    query_list.extend(fix_datetimes.get_fix_incorrect_datetime_to_date_queries(project_id, dataset_id))
    query_list.extend(remove_records_with_wrong_date.get_remove_records_with_wrong_date_queries(project_id, dataset_id))
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
    query_list.extend(fix_datetimes.get_fix_incorrect_datetime_to_date_queries(project_id, dataset_id))
    query_list.extend(remove_records_with_wrong_date.get_remove_records_with_wrong_date_queries(project_id, dataset_id))
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
