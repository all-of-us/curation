"""
A module to serve as the entry point to the cdr_cleaner package.

It gathers the list of query strings to execute and sends them
to the query engine.
"""
# Python imports
import logging

# Third party imports
from google.appengine.api import app_identity

import sandbox
from constants.cdr_cleaner.clean_cdr import DataStage as stage
# Project imports
import bq_utils
import cdr_cleaner.clean_cdr_engine as clean_engine
import cdr_cleaner.cleaning_rules.backfill_pmi_skip_codes as back_fill_pmi_skip
import cdr_cleaner.cleaning_rules.clean_ppi_numeric_fields_using_parameters as ppi_numeric_fields
import cdr_cleaner.cleaning_rules.clean_years as clean_years
import cdr_cleaner.cleaning_rules.domain_alignment as domain_alignment
import cdr_cleaner.cleaning_rules.drug_refills_days_supply as drug_refills_supply
import cdr_cleaner.cleaning_rules.ensure_date_datetime_consistency as fix_datetimes
import cdr_cleaner.cleaning_rules.fill_free_text_source_value as fill_source_value
import cdr_cleaner.cleaning_rules.id_deduplicate as id_dedup
import cdr_cleaner.cleaning_rules.maps_to_value_ppi_vocab_update as maps_to_value_vocab_update
import cdr_cleaner.cleaning_rules.negative_ages as neg_ages
import cdr_cleaner.cleaning_rules.no_data_30_days_after_death as no_data_30days_after_death
import cdr_cleaner.cleaning_rules.null_invalid_foreign_keys as null_foreign_key
import cdr_cleaner.cleaning_rules.populate_route_ids as populate_routes
import cdr_cleaner.cleaning_rules.remove_multiple_race_ethnicity_answers as remove_multiple_race_answers
import cdr_cleaner.cleaning_rules.remove_records_with_wrong_date as remove_records_with_wrong_date
import cdr_cleaner.cleaning_rules.replace_standard_id_in_domain_tables as replace_standard_concept_ids
import cdr_cleaner.cleaning_rules.temporal_consistency as bad_end_dates
import cdr_cleaner.cleaning_rules.valid_death_dates as valid_death_dates
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
    query_list.extend(maps_to_value_vocab_update.get_maps_to_value_ppi_vocab_update_queries(project_id, dataset_id))
    query_list.extend(back_fill_pmi_skip.get_run_pmi_fix_queries(project_id, dataset_id))
    query_list.extend(ppi_numeric_fields.get_clean_ppi_num_fields_using_parameters_queries(project_id, dataset_id))
    query_list.extend(
        remove_multiple_race_answers.get_remove_multiple_race_ethnicity_answers_queries(project_id, dataset_id))
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
    # TODO : Make null_invalid_foreign_keys able to run on de_identified dataset
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
    query_list.extend(neg_ages.get_negative_ages_queries(project_id, dataset_id))
    query_list.extend(bad_end_dates.get_bad_end_date_queries(project_id, dataset_id))
    query_list.extend(valid_death_dates.get_valid_death_date_queries(project_id, dataset_id))
    query_list.extend(fill_source_value.get_fill_freetext_source_value_fields_queries(project_id, dataset_id))
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
    if project_id is None:
        project_id = app_identity.get_application_id()
        LOGGER.info('Project is unspecified.  Using default value of:\t%s', project_id)

    if dataset_id is None:
        dataset_id = bq_utils.get_rdr_dataset_id()
        LOGGER.info('Dataset is unspecified.  Using default value of:\t%s', dataset_id)

    sandbox.create_sandbox_dataset(project_id=project_id, dataset_id=dataset_id)

    query_list = _gather_rdr_queries(project_id, dataset_id)

    LOGGER.info("Cleaning rdr_dataset")
    clean_engine.clean_dataset(project_id, query_list, stage.RDR)


def clean_ehr_dataset(project_id=None, dataset_id=None):
    """
    Run all clean rules defined for the ehr dataset.

    :param project_id:  Name of the BigQuery project.
    :param dataset_id:  Name of the dataset to clean
    """
    if project_id is None:
        project_id = app_identity.get_application_id()
        LOGGER.info('Project is unspecified.  Using default value of:\t%s', project_id)

    if dataset_id is None:
        dataset_id = bq_utils.get_dataset_id()
        LOGGER.info('Dataset is unspecified.  Using default value of:\t%s', dataset_id)

    sandbox.create_sandbox_dataset(project_id=project_id, dataset_id=dataset_id)

    query_list = _gather_ehr_queries(project_id, dataset_id)

    LOGGER.info("Cleaning ehr_dataset")
    clean_engine.clean_dataset(project_id, query_list, stage.EHR)


def clean_unioned_ehr_dataset(project_id=None, dataset_id=None):
    """
    Run all clean rules defined for the unioned ehr dataset.

    :param project_id:  Name of the BigQuery project.
    :param dataset_id:  Name of the dataset to clean
    """
    if project_id is None:
        project_id = app_identity.get_application_id()
        LOGGER.info('Project is unspecified.  Using default value of:\t%s', project_id)

    if dataset_id is None:
        dataset_id = bq_utils.get_unioned_dataset_id()
        LOGGER.info('Dataset is unspecified.  Using default value of:\t%s', dataset_id)

    sandbox.create_sandbox_dataset(project_id=project_id, dataset_id=dataset_id)

    query_list = _gather_unioned_ehr_queries(project_id, dataset_id, stage.UNIONED)

    LOGGER.info("Cleaning unioned_dataset")
    clean_engine.clean_dataset(project_id, query_list)


def clean_ehr_rdr_dataset(project_id=None, dataset_id=None):
    """
    Run all clean rules defined for the ehr and rdr dataset.

    :param project_id:  Name of the BigQuery project.
    :param dataset_id:  Name of the dataset to clean
    """
    if project_id is None:
        project_id = app_identity.get_application_id()
        LOGGER.info('Project is unspecified.  Using default value of:\t%s', project_id)

    if dataset_id is None:
        dataset_id = bq_utils.get_ehr_rdr_dataset_id()
        LOGGER.info('Dataset is unspecified.  Using default value of:\t%s', dataset_id)

    sandbox.create_sandbox_dataset(project_id=project_id, dataset_id=dataset_id)

    query_list = _gather_ehr_rdr_queries(project_id, dataset_id)

    LOGGER.info("Cleaning ehr_rdr_dataset")
    clean_engine.clean_dataset(project_id, query_list, stage.COMBINED)


def clean_ehr_rdr_de_identified_dataset(project_id=None, dataset_id=None):
    """
    Run all clean rules defined for the deidentified ehr and rdr dataset.

    :param project_id:  Name of the BigQuery project.
    :param dataset_id:  Name of the dataset to clean
    """
    if project_id is None:
        project_id = app_identity.get_application_id()
        LOGGER.info('Project is unspecified.  Using default value of:\t%s', project_id)

    if dataset_id is None:
        dataset_id = bq_utils.get_combined_deid_dataset_id()
        LOGGER.info('Dataset is unspecified.  Using default value of:\t%s', dataset_id)

    sandbox.create_sandbox_dataset(project_id=project_id, dataset_id=dataset_id)

    query_list = _gather_ehr_rdr_de_identified_queries(project_id, dataset_id)

    LOGGER.info("Cleaning de-identified dataset")
    clean_engine.clean_dataset(project_id, query_list, stage.DEID)


def clean_all_cdr():
    """
    Runs cleaning rules on all the datasets
    """
    clean_ehr_dataset()
    clean_unioned_ehr_dataset()
    clean_rdr_dataset()
    clean_ehr_rdr_dataset()
    clean_ehr_rdr_de_identified_dataset()


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-d', '--data_stage',
                        required=True, dest='data_stage',
                        action='store',
                        type=stage,
                        choices=list([s for s in stage if s is not stage.UNSPECIFIED]),
                        help='Specify the dataset')
    parser.add_argument('-s',
                        action='store_true',
                        help='Send logs to console')
    args = parser.parse_args()
    clean_engine.add_console_logging(args.s)
    if args.data_stage == stage.EHR:
        clean_ehr_dataset()
    elif args.data_stage == stage.UNIONED:
        clean_unioned_ehr_dataset()
    elif args.data_stage == stage.RDR:
        clean_rdr_dataset()
    elif args.data_stage == stage.COMBINED:
        clean_ehr_rdr_dataset()
    elif args.data_stage == stage.DEID:
        clean_ehr_rdr_de_identified_dataset()
    else:
        raise EnvironmentError('Dataset selection should be from [ehr, unioned, rdr, combined, deid]')
