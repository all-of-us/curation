"""
A module to serve as the entry point to the cdr_cleaner package.

It gathers the list of query strings to execute and sends them
to the query engine.
"""
# Python imports
import logging
import inspect

# Third party imports
import app_identity

# Project imports
import bq_utils
import sandbox
import cdr_cleaner.clean_cdr_engine as clean_engine
import cdr_cleaner.cleaning_rules.backfill_pmi_skip_codes as back_fill_pmi_skip
import cdr_cleaner.cleaning_rules.clean_ppi_numeric_fields_using_parameters as ppi_numeric_fields
import cdr_cleaner.cleaning_rules.clean_years as clean_years
import cdr_cleaner.cleaning_rules.domain_alignment as domain_alignment
import cdr_cleaner.cleaning_rules.drop_duplicate_states as drop_duplicate_states
import cdr_cleaner.cleaning_rules.drop_participants_without_ppi_or_ehr as drop_participants_without_ppi_or_ehr
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
import cdr_cleaner.cleaning_rules.repopulate_person_post_deid as repopulate_person
import cdr_cleaner.cleaning_rules.round_ppi_values_to_nearest_integer as round_ppi_values
import cdr_cleaner.cleaning_rules.temporal_consistency as bad_end_dates
import cdr_cleaner.cleaning_rules.valid_death_dates as valid_death_dates
import cdr_cleaner.cleaning_rules.update_family_history_qa_codes as update_family_history
import cdr_cleaner.cleaning_rules.remove_invalid_procedure_source_records as invalid_procedure_source
import cdr_cleaner.manual_cleaning_rules.clean_smoking_ppi as smoking
import cdr_cleaner.cleaning_rules.drop_multiple_measurements as drop_mult_meas
import cdr_cleaner.cleaning_rules.drop_extreme_measurements as extreme_measurements
import cdr_cleaner.manual_cleaning_rules.negative_ppi as negative_ppi
import cdr_cleaner.manual_cleaning_rules.ppi_drop_duplicate_responses as ppi_drop_duplicates
import cdr_cleaner.manual_cleaning_rules.remove_operational_pii_fields as operational_pii_fields
import cdr_cleaner.manual_cleaning_rules.update_questiona_answers_not_mapped_to_omop as map_questions_answers_to_omop
import cdr_cleaner.cleaning_rules.remove_aian_participants as remove_aian_participants
import cdr_cleaner.cleaning_rules.remove_non_matching_participant as validate_missing_participants
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from constants.cdr_cleaner.clean_cdr import DataStage as stage
from constants.cdr_cleaner import clean_cdr as cdr_consts

LOGGER = logging.getLogger(__name__)

EHR_CLEANING_CLASSES = [
    (id_dedup.get_id_deduplicate_queries,),
]

UNIONED_EHR_CLEANING_CLASSES = [
    (id_dedup.get_id_deduplicate_queries,),
    (clean_years.get_year_of_birth_queries,),
    (neg_ages.get_negative_ages_queries,),
    (bad_end_dates.get_bad_end_date_queries,),
    (drug_refills_supply.get_days_supply_refills_queries,),
    # trying to load a table while creating query strings,
    # won't work with mocked strings.  should use base class
    # setup_query_execution function to load dependencies before query execution
    # (populate_routes.get_route_mapping_queries,),
    (
        fix_datetimes.get_fix_incorrect_datetime_to_date_queries,),
    (remove_records_with_wrong_date.get_remove_records_with_wrong_date_queries,
    ),
    # DC-698 opened to fix it.
    #    (invalid_procedure_source.get_remove_invalid_procedure_source_queries,),
]

RDR_CLEANING_CLASSES = [
    (maps_to_value_vocab_update.get_maps_to_value_ppi_vocab_update_queries,),
    (back_fill_pmi_skip.get_run_pmi_fix_queries,),
    (ppi_numeric_fields.get_clean_ppi_num_fields_using_parameters_queries,),
    (remove_multiple_race_answers.
     get_remove_multiple_race_ethnicity_answers_queries,),
    (negative_ppi.get_update_ppi_queries,),
    # trying to load a table while creating query strings,
    # won't work with mocked strings.  should use base class
    # setup_query_execution function to load dependencies before query execution
    #(smoking.get_queries_clean_smoking,),
    (
        ppi_drop_duplicates.
        get_remove_duplicate_set_of_responses_to_same_questions_queries,),
    # trying to load a table while creating query strings,
    # won't work with mocked strings.  should use base class
    # setup_query_execution function to load dependencies before query execution
    #(operational_pii_fields.get_remove_operational_pii_fields_query,),
    # trying to load a table while creating query strings,
    # won't work with mocked strings.  should use base class
    # setup_query_execution function to load dependencies before query execution
    #map_questions_answers_to_omop.
    #(get_update_questions_answers_not_mapped_to_omop,),
    (
        round_ppi_values.get_round_ppi_values_queries,),
    (update_family_history.get_update_family_history_qa_queries,),
    (extreme_measurements.get_drop_extreme_measurement_queries,),
    (drop_mult_meas.get_drop_multiple_measurement_queries,),
]

COMBINED_CLEANING_CLASSES = [
    # trying to load a table while creating query strings,
    # won't work with mocked strings.  should use base class
    # setup_query_execution function to load dependencies before query execution
    #    (replace_standard_concept_ids.replace_standard_id_in_domain_tables,),
    # trying to load a table while creating query strings,
    # won't work with mocked strings.  should use base class
    # setup_query_execution function to load dependencies before query execution
    #    (domain_alignment.domain_alignment,),
    (
        drop_participants_without_ppi_or_ehr.get_queries,),
    (id_dedup.get_id_deduplicate_queries,),
    (clean_years.get_year_of_birth_queries,),
    (neg_ages.get_negative_ages_queries,),
    (bad_end_dates.get_bad_end_date_queries,),
    (no_data_30days_after_death.no_data_30_days_after_death,),
    (valid_death_dates.get_valid_death_date_queries,),
    (drug_refills_supply.get_days_supply_refills_queries,),
    # trying to load a table while creating query strings,
    # won't work with mocked strings.  should use base class
    # setup_query_execution function to load dependencies before query execution
    #    (populate_routes.get_route_mapping_queries,),
    (
        fix_datetimes.get_fix_incorrect_datetime_to_date_queries,),
    (remove_records_with_wrong_date.get_remove_records_with_wrong_date_queries,
    ),
    (drop_duplicate_states.get_drop_duplicate_states_queries,),
    # TODO : Make null_invalid_foreign_keys able to run on de_identified dataset
    (
        null_foreign_key.null_invalid_foreign_keys,),
    (remove_aian_participants.get_queries,),
    (validate_missing_participants.delete_records_for_non_matching_participants,
    )
]

DEID_BASE_CLEANING_CLASSES = [
    (id_dedup.get_id_deduplicate_queries,),
    (neg_ages.get_negative_ages_queries,),
    (bad_end_dates.get_bad_end_date_queries,),
    (valid_death_dates.get_valid_death_date_queries,),
    (fill_source_value.get_fill_freetext_source_value_fields_queries,),
    (repopulate_person.get_repopulate_person_post_deid_queries,),
]

DEID_CLEAN_CLEANING_CLASSES = []


def add_module_info_decorator(query_function, *positional_args, **keyword_args):
    """
    A decorator for adding the module information to the list of query dictionaries generated by the cleaning rules
    
    :param query_function: a function that generates a list of query dictionaries
    :return: a list of query dictionaries containing the module information
    """

    function_name = query_function.__name__
    module_name = inspect.getmodule(query_function).__name__
    _, line_no = inspect.getsourcelines(query_function)

    module_info_dict = {
        cdr_consts.MODULE_NAME: module_name,
        cdr_consts.FUNCTION_NAME: function_name,
        cdr_consts.LINE_NO: line_no
    }

    # Expand the query dictionary with the module_info_dict
    return [
        dict(**query, **module_info_dict)
        for query in query_function(*positional_args, **keyword_args)
    ]


def _gather_ehr_queries(project_id, dataset_id, sandbox_dataset_id):
    """
    gathers all the queries required to clean ehr dataset

    :param project_id: project name
    :param dataset_id: ehr dataset name
    :return: returns list of queries
    """
    return _get_query_list(EHR_CLEANING_CLASSES, project_id, dataset_id,
                           sandbox_dataset_id)


def _gather_rdr_queries(project_id, dataset_id, sandbox_dataset_id):
    """
    gathers all the queries required to clean rdr dataset

    :param project_id: project name
    :param dataset_id: rdr dataset name
    :param sandbox_dataset_id: sandbox_dataset_id
    :return: returns list of queries
    """
    return _get_query_list(RDR_CLEANING_CLASSES, project_id, dataset_id,
                           sandbox_dataset_id)


def _gather_combined_queries(project_id, dataset_id, sandbox_dataset_id):
    """
    gathers all the queries required to clean combined dataset

    :param project_id: project name
    :param dataset_id: combined dataset name
    :return: returns list of queries
    """
    return _get_query_list(COMBINED_CLEANING_CLASSES, project_id, dataset_id,
                           sandbox_dataset_id)


def _gather_unioned_ehr_queries(project_id, dataset_id, sandbox_dataset_id):
    """
    gathers all the queries required to clean unioned_ehr dataset

    :param project_id: project name
    :param dataset_id: unioned_ehr dataset name
    :return: returns list of queries
    """
    return _get_query_list(UNIONED_EHR_CLEANING_CLASSES, project_id, dataset_id,
                           sandbox_dataset_id)


def _gather_deid_base_cleaning_queries(project_id, dataset_id,
                                       sandbox_dataset_id):
    """
    gathers all the queries required to clean de_identified dataset

    These queries are applied to a copy of `<dataset_release_tag>_combined_deid`
    to create `<dataset_release_tag>_combined_deid_base`.  This dataset is
    copied for use by the Workbench team.

    :param project_id: project name
    :param dataset_id: de_identified dataset name
    :return: returns list of queries
    """
    return _get_query_list(DEID_BASE_CLEANING_CLASSES, project_id, dataset_id,
                           sandbox_dataset_id)


def _gather_deid_clean_cleaning_queries(project_id, dataset_id,
                                        sandbox_dataset_id):
    """
    gathers all the queries required to clean base version of de_identified dataset

    These queries are applied to a copy of `<dataset_release_tag>_combined_deid_base`
    to create `<dataset_release_tag>_combined_deid_clean`.  This dataset is
    copied for use by the Cohort Builder team.

    :param project_id: project name
    :param dataset_id: de_identified dataset name
    :return: returns list of queries
    """
    return _get_query_list(DEID_CLEAN_CLEANING_CLASSES, project_id, dataset_id,
                           sandbox_dataset_id)


def _get_query_list(cleaning_classes, project_id, dataset_id,
                    sandbox_dataset_id):
    """
    gathers all the queries required to clean a dataset

    :param cleaning_classes:  the list of classes generating SQL cleaning statements
    :param project_id: project name
    :param dataset_id: de_identified dataset name
    :return: returns list of queries
    """
    query_list = []

    for class_info in cleaning_classes:
        clazz = class_info[0]
        try:
            instance = clazz(project_id, dataset_id, sandbox_dataset_id)
        except TypeError:
            # raised when called with the 3 parameters and only 2 are needed
            query_list.extend(
                add_module_info_decorator(clazz, project_id, dataset_id))
        else:
            # should eventually be the main component of this function.
            # Everything should transition to using a common base class.
            if isinstance(instance, BaseCleaningRule):

                keywords = class_info[-1]
                if isinstance(keywords, dict):
                    positionals = class_info[1:-1]
                else:
                    keywords = {}
                    positionals = class_info[1:]

                query_list.extend(
                    add_module_info_decorator(
                        instance.get_query_dictionary_list, *positionals,
                        **keywords))
            else:
                # if the class is not of the common base class, raise an error
                # will prevent running manual cleaning rules that have not been
                # transitioned into proper cleaning rules
                #                raise TypeError(
                #                    '{} is not an instance of BaseCleaningRule'.format(
                #                        instance.__class__.__name__))
                # is raised when a function is called without all the required variables
                query_list.extend(
                    add_module_info_decorator(clazz, project_id, dataset_id,
                                              sandbox_dataset_id))

    return query_list


def clean_rdr_dataset(project_id=None, dataset_id=None):
    """
    Run all clean rules defined for the rdr dataset.

    :param project_id:  Name of the BigQuery project.
    :param dataset_id:  Name of the dataset to clean
    """
    if project_id is None:
        project_id = app_identity.get_application_id()
        LOGGER.info('Project is unspecified.  Using default value of:\t%s',
                    project_id)

    if dataset_id is None:
        dataset_id = bq_utils.get_rdr_dataset_id()
        LOGGER.info('Dataset is unspecified.  Using default value of:\t%s',
                    dataset_id)

    sandbox_dataset_id = sandbox.create_sandbox_dataset(project_id=project_id,
                                                        dataset_id=dataset_id)

    query_list = _gather_rdr_queries(project_id, dataset_id, sandbox_dataset_id)

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
        LOGGER.info('Project is unspecified.  Using default value of:\t%s',
                    project_id)

    if dataset_id is None:
        dataset_id = bq_utils.get_dataset_id()
        LOGGER.info('Dataset is unspecified.  Using default value of:\t%s',
                    dataset_id)

    sandbox_dataset_id = sandbox.create_sandbox_dataset(project_id=project_id,
                                                        dataset_id=dataset_id)

    query_list = _gather_ehr_queries(project_id, dataset_id, sandbox_dataset_id)

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
        LOGGER.info('Project is unspecified.  Using default value of:\t%s',
                    project_id)

    if dataset_id is None:
        dataset_id = bq_utils.get_unioned_dataset_id()
        LOGGER.info('Dataset is unspecified.  Using default value of:\t%s',
                    dataset_id)

    sandbox_dataset_id = sandbox.create_sandbox_dataset(project_id=project_id,
                                                        dataset_id=dataset_id)

    query_list = _gather_unioned_ehr_queries(project_id, dataset_id,
                                             sandbox_dataset_id)

    LOGGER.info("Cleaning unioned_dataset")
    clean_engine.clean_dataset(project_id, query_list, stage.UNIONED)


def clean_combined_dataset(project_id=None, dataset_id=None):
    """
    Run all clean rules defined for the ehr and rdr dataset.

    :param project_id:  Name of the BigQuery project.
    :param dataset_id:  Name of the dataset to clean
    """
    if project_id is None:
        project_id = app_identity.get_application_id()
        LOGGER.info('Project is unspecified.  Using default value of:\t%s',
                    project_id)

    if dataset_id is None:
        dataset_id = bq_utils.get_combined_dataset_id()
        LOGGER.info('Dataset is unspecified.  Using default value of:\t%s',
                    dataset_id)

    sandbox_dataset_id = sandbox.create_sandbox_dataset(project_id=project_id,
                                                        dataset_id=dataset_id)

    query_list = _gather_combined_queries(project_id, dataset_id,
                                          sandbox_dataset_id)

    LOGGER.info("Cleaning combined_dataset")
    clean_engine.clean_dataset(project_id, query_list, stage.COMBINED)


def clean_combined_de_identified_dataset(project_id=None, dataset_id=None):
    """
    Run all clean rules defined for the deidentified ehr and rdr dataset.

    :param project_id:  Name of the BigQuery project.
    :param dataset_id:  Name of the dataset to clean
    """
    if project_id is None:
        project_id = app_identity.get_application_id()
        LOGGER.info('Project is unspecified.  Using default value of:\t%s',
                    project_id)

    if dataset_id is None:
        dataset_id = bq_utils.get_combined_deid_dataset_id()
        LOGGER.info('Dataset is unspecified.  Using default value of:\t%s',
                    dataset_id)

    sandbox_dataset_id = sandbox.create_sandbox_dataset(project_id=project_id,
                                                        dataset_id=dataset_id)

    query_list = _gather_deid_base_cleaning_queries(project_id, dataset_id,
                                                    sandbox_dataset_id)

    LOGGER.info("Cleaning de-identified dataset")
    clean_engine.clean_dataset(project_id, query_list, stage.DEID_BASE)


def clean_combined_de_identified_clean_dataset(project_id=None,
                                               dataset_id=None):
    """
    Run all clean rules defined for the deidentified ehr and rdr clean dataset.
    :param project_id:  Name of the BigQuery project.
    :param dataset_id:  Name of the dataset to clean
    """
    if project_id is None:
        project_id = app_identity.get_application_id()
        LOGGER.info('Project is unspecified.  Using default value of:\t%s',
                    project_id)

    if dataset_id is None:
        dataset_id = bq_utils.get_combined_deid_clean_dataset_id()
        LOGGER.info('Dataset is unspecified.  Using default value of:\t%s',
                    dataset_id)

    sandbox_dataset_id = sandbox.create_sandbox_dataset(project_id=project_id,
                                                        dataset_id=dataset_id)

    query_list = _gather_deid_clean_cleaning_queries(project_id, dataset_id,
                                                     sandbox_dataset_id)

    LOGGER.info("Cleaning de-identified dataset")
    clean_engine.clean_dataset(project_id, query_list, stage.DEID_CLEAN)


def clean_all_cdr():
    """
    Runs cleaning rules on all the datasets
    """
    clean_ehr_dataset()
    clean_unioned_ehr_dataset()
    clean_rdr_dataset()
    clean_combined_dataset()
    clean_combined_de_identified_dataset()
    clean_combined_de_identified_clean_dataset()


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-d',
                        '--data_stage',
                        required=True,
                        dest='data_stage',
                        action='store',
                        type=stage,
                        choices=list(
                            [s for s in stage if s is not stage.UNSPECIFIED]),
                        help='Specify the dataset')
    parser.add_argument('-s', action='store_true', help='Send logs to console')
    args = parser.parse_args()
    clean_engine.add_console_logging(args.s)
    if args.data_stage == stage.EHR:
        clean_ehr_dataset()
    elif args.data_stage == stage.UNIONED:
        clean_unioned_ehr_dataset()
    elif args.data_stage == stage.RDR:
        clean_rdr_dataset()
    elif args.data_stage == stage.COMBINED:
        clean_combined_dataset()
    elif args.data_stage == stage.DEID_BASE:
        clean_combined_de_identified_dataset()
    elif args.data_stage == stage.DEID_CLEAN:
        clean_combined_de_identified_clean_dataset()
    else:
        raise OSError(
            f'Dataset selection should be from [{stage.EHR}, {stage.UNIONED}, {stage.RDR}, {stage.COMBINED},'
            f' {stage.DEID_BASE}, {stage.DEID_CLEAN}]')
