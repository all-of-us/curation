"""
A module to serve as the entry point to the cdr_cleaner package.

It gathers the list of query strings to execute and sends them
to the query engine.
"""
# Python imports
import logging

# Third party imports

# Project imports
import cdr_cleaner.clean_cdr_engine as clean_engine
import cdr_cleaner.cleaning_rules.backfill_pmi_skip_codes as back_fill_pmi_skip
import cdr_cleaner.cleaning_rules.clean_years as clean_years
import cdr_cleaner.cleaning_rules.domain_alignment as domain_alignment
import cdr_cleaner.cleaning_rules.drop_duplicate_states as drop_duplicate_states
import cdr_cleaner.cleaning_rules.drop_extreme_measurements as extreme_measurements
import cdr_cleaner.cleaning_rules.drop_multiple_measurements as drop_mult_meas
import \
    cdr_cleaner.cleaning_rules.drop_participants_without_ppi_or_ehr as drop_participants_without_ppi_or_ehr
import cdr_cleaner.cleaning_rules.drug_refills_days_supply as drug_refills_supply
import cdr_cleaner.cleaning_rules.fill_free_text_source_value as fill_source_value
import cdr_cleaner.cleaning_rules.id_deduplicate as id_dedup
import cdr_cleaner.cleaning_rules.maps_to_value_ppi_vocab_update as maps_to_value_vocab_update
import cdr_cleaner.cleaning_rules.negative_ages as neg_ages
import cdr_cleaner.cleaning_rules.no_data_30_days_after_death as no_data_30days_after_death
import cdr_cleaner.cleaning_rules.null_invalid_foreign_keys as null_foreign_key
import cdr_cleaner.cleaning_rules.populate_route_ids as populate_routes
import cdr_cleaner.cleaning_rules.remove_aian_participants as remove_aian_participants
import cdr_cleaner.cleaning_rules.remove_ehr_data_past_deactivation_date as remove_ehr_data
import \
    cdr_cleaner.cleaning_rules.remove_invalid_procedure_source_records as invalid_procedure_source
import \
    cdr_cleaner.cleaning_rules.remove_multiple_race_ethnicity_answers as remove_multiple_race_answers
import cdr_cleaner.cleaning_rules.remove_non_matching_participant as validate_missing_participants
import cdr_cleaner.cleaning_rules.remove_records_with_wrong_date as remove_records_with_wrong_date
import \
    cdr_cleaner.cleaning_rules.replace_standard_id_in_domain_tables as replace_standard_concept_ids
import cdr_cleaner.cleaning_rules.repopulate_person_post_deid as repopulate_person
import cdr_cleaner.cleaning_rules.round_ppi_values_to_nearest_integer as round_ppi_values
import cdr_cleaner.cleaning_rules.temporal_consistency as bad_end_dates
import cdr_cleaner.cleaning_rules.update_family_history_qa_codes as update_family_history
import cdr_cleaner.cleaning_rules.valid_death_dates as valid_death_dates
import cdr_cleaner.manual_cleaning_rules.clean_smoking_ppi as smoking
import cdr_cleaner.manual_cleaning_rules.negative_ppi as negative_ppi
import cdr_cleaner.manual_cleaning_rules.remove_operational_pii_fields as operational_pii_fields
import \
    cdr_cleaner.manual_cleaning_rules.update_questiona_answers_not_mapped_to_omop as map_questions_answers_to_omop
import sandbox
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from cdr_cleaner.cleaning_rules.clean_height_weight import CleanHeightAndWeight
from cdr_cleaner.cleaning_rules.clean_mapping import CleanMappingExtTables
from cdr_cleaner.cleaning_rules.clean_ppi_numeric_fields_using_parameters import \
    CleanPPINumericFieldsUsingParameters
from cdr_cleaner.cleaning_rules.create_person_ext_table import CreatePersonExtTable
from cdr_cleaner.cleaning_rules.date_shift_cope_responses import DateShiftCopeResponses
from cdr_cleaner.cleaning_rules.deid.fitbit_dateshift import FitbitDateShiftRule
from cdr_cleaner.cleaning_rules.deid.pid_rid_map import PIDtoRID
from cdr_cleaner.cleaning_rules.deid.remove_fitbit_data_if_max_age_exceeded import \
    RemoveFitbitDataIfMaxAgeExceeded
from cdr_cleaner.cleaning_rules.drop_duplicate_ppi_questions_and_answers import \
    DropDuplicatePpiQuestionsAndAnswers
from cdr_cleaner.cleaning_rules.drop_ppi_duplicate_responses import DropPpiDuplicateResponses
from cdr_cleaner.cleaning_rules.drop_zero_concept_ids import DropZeroConceptIDs
from cdr_cleaner.cleaning_rules.ensure_date_datetime_consistency import \
    EnsureDateDatetimeConsistency
from cdr_cleaner.cleaning_rules.measurement_table_suppression import MeasurementRecordsSuppression
from cdr_cleaner.cleaning_rules.null_concept_ids_for_numeric_ppi import NullConceptIDForNumericPPI
from cdr_cleaner.cleaning_rules.ppi_branching import PpiBranching
from cdr_cleaner.cleaning_rules.rdr_observation_source_concept_id_suppression import (
    ObservationSourceConceptIDRowSuppression)
from cdr_cleaner.cleaning_rules.truncate_rdr_using_date import TruncateRdrData
from cdr_cleaner.cleaning_rules.unit_normalization import UnitNormalization
from cdr_cleaner.cleaning_rules.update_fields_numbers_as_strings import UpdateFieldsNumbersAsStrings
from cdr_cleaner.cleaning_rules.fix_unmapped_survey_answers import FixUnmappedSurveyAnswers
from constants.cdr_cleaner import clean_cdr as cdr_consts
from constants.cdr_cleaner.clean_cdr import DataStage
from constants.cdr_cleaner import clean_cdr_engine as ce_consts

LOGGER = logging.getLogger(__name__)

EHR_CLEANING_CLASSES = [(id_dedup.get_id_deduplicate_queries,),
                        (CleanMappingExtTables,)]

UNIONED_EHR_CLEANING_CLASSES = [
    (id_dedup.get_id_deduplicate_queries,),
    (clean_years.get_year_of_birth_queries,),
    (neg_ages.get_negative_ages_queries,),
    (bad_end_dates.get_bad_end_date_queries,),
    (drug_refills_supply.get_days_supply_refills_queries,),
    # trying to load a table while creating query strings,
    # won't work with mocked strings.  should use base class
    # setup_query_execution function to load dependencies before query execution
    (
        populate_routes.get_route_mapping_queries,),
    (EnsureDateDatetimeConsistency,),
    (remove_records_with_wrong_date.get_remove_records_with_wrong_date_queries,
    ),
    (invalid_procedure_source.get_remove_invalid_procedure_source_queries,),
    (remove_ehr_data.remove_ehr_data_queries,),
    (CleanMappingExtTables,),
]

RDR_CLEANING_CLASSES = [
    (TruncateRdrData,),
    (PpiBranching,),
    # execute FixUnmappedSurveyAnswers before the dropping responses rules get executed
    # (e.g. DropPpiDuplicateResponses and DropDuplicatePpiQuestionsAndAnswers)
    (
        FixUnmappedSurveyAnswers,),
    (ObservationSourceConceptIDRowSuppression,),
    (maps_to_value_vocab_update.get_maps_to_value_ppi_vocab_update_queries,),
    (back_fill_pmi_skip.get_run_pmi_fix_queries,),
    (CleanPPINumericFieldsUsingParameters,),
    (NullConceptIDForNumericPPI,),
    (remove_multiple_race_answers.
     get_remove_multiple_race_ethnicity_answers_queries,),
    (negative_ppi.get_update_ppi_queries,),
    # trying to load a table while creating query strings,
    # won't work with mocked strings.  should use base class
    # setup_query_execution function to load dependencies before query execution
    (
        smoking.get_queries_clean_smoking,),
    (DropPpiDuplicateResponses,),
    # trying to load a table while creating query strings,
    # won't work with mocked strings.  should use base class
    # setup_query_execution function to load dependencies before query execution
    (
        operational_pii_fields.get_remove_operational_pii_fields_query,),
    # trying to load a table while creating query strings,
    # won't work with mocked strings.  should use base class
    # setup_query_execution function to load dependencies before query execution
    (
        map_questions_answers_to_omop.
        get_update_questions_answers_not_mapped_to_omop,),
    (round_ppi_values.get_round_ppi_values_queries,),
    (update_family_history.get_update_family_history_qa_queries,),
    (DropDuplicatePpiQuestionsAndAnswers,),
    (extreme_measurements.get_drop_extreme_measurement_queries,),
    (drop_mult_meas.get_drop_multiple_measurement_queries,),
    (UpdateFieldsNumbersAsStrings,),
]

COMBINED_CLEANING_CLASSES = [
    # trying to load a table while creating query strings,
    # won't work with mocked strings.  should use base class
    # setup_query_execution function to load dependencies before query execution
    (
        replace_standard_concept_ids.replace_standard_id_in_domain_tables,),
    # trying to load a table while creating query strings,
    # won't work with mocked strings.  should use base class
    # setup_query_execution function to load dependencies before query execution
    (
        domain_alignment.domain_alignment,),
    (drop_participants_without_ppi_or_ehr.get_queries,),
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
    (
        populate_routes.get_route_mapping_queries,),
    (EnsureDateDatetimeConsistency,),
    (remove_records_with_wrong_date.get_remove_records_with_wrong_date_queries,
    ),
    (drop_duplicate_states.get_drop_duplicate_states_queries,),
    # TODO : Make null_invalid_foreign_keys able to run on de_identified dataset
    (
        null_foreign_key.null_invalid_foreign_keys,),
    (remove_aian_participants.get_queries,),
    (validate_missing_participants.delete_records_for_non_matching_participants,
    ),
    (CleanMappingExtTables,)
]

FITBIT_CLEANING_CLASSES = [
    (RemoveFitbitDataIfMaxAgeExceeded,),
    (PIDtoRID,),
    (FitbitDateShiftRule,),
]

DEID_BASE_CLEANING_CLASSES = [
    (id_dedup.get_id_deduplicate_queries,),
    (neg_ages.get_negative_ages_queries,),
    (bad_end_dates.get_bad_end_date_queries,),
    (valid_death_dates.get_valid_death_date_queries,),
    (fill_source_value.get_fill_freetext_source_value_fields_queries,),
    (repopulate_person.get_repopulate_person_post_deid_queries,),
    (DateShiftCopeResponses,),
    (CreatePersonExtTable,),
    (CleanMappingExtTables,),
]

DEID_CLEAN_CLEANING_CLASSES = [
    (MeasurementRecordsSuppression,),
    (CleanHeightAndWeight,),  # dependent on MeasurementRecordsSuppression
    (UnitNormalization,),  # dependent on CleanHeightAndWeight
    (DropZeroConceptIDs,),
    (CleanMappingExtTables,)
]

DATA_STAGE_RULES_MAPPING = {
    DataStage.EHR.value: EHR_CLEANING_CLASSES,
    DataStage.UNIONED.value: UNIONED_EHR_CLEANING_CLASSES,
    DataStage.RDR.value: RDR_CLEANING_CLASSES,
    DataStage.COMBINED.value: COMBINED_CLEANING_CLASSES,
    DataStage.DEID_BASE.value: DEID_BASE_CLEANING_CLASSES,
    DataStage.DEID_CLEAN.value: DEID_CLEAN_CLEANING_CLASSES,
    DataStage.FITBIT.value: FITBIT_CLEANING_CLASSES,
}


def get_parser():
    """
    Create a parser which raises invalid enum errors

    :return: parser
    """
    from cdr_cleaner import args_parser

    engine_parser = args_parser.get_argument_parser()
    engine_parser.add_argument(
        '-a',
        '--data_stage',
        required=True,
        dest='data_stage',
        action='store',
        type=DataStage,
        choices=list([s for s in DataStage if s is not DataStage.UNSPECIFIED]),
        help='Specify the dataset')
    return engine_parser


PARSING_ERROR_MESSAGE_FORMAT = (
    'Error parsing %(arg)s. Please use "--key value" to specify custom arguments. '
    'Custom arguments need an associated keyword to store their value.')


def _to_kwarg_key(arg):
    if not arg.startswith('--'):
        raise RuntimeError(PARSING_ERROR_MESSAGE_FORMAT.format(arg=arg))
    key = arg[2:]
    if not key:
        raise RuntimeError(PARSING_ERROR_MESSAGE_FORMAT.format(arg=arg))
    return key


def _to_kwarg_val(val):
    # likely invalid use of args- allowing single dash e.g. negative values
    if val.startswith('--'):
        raise RuntimeError(PARSING_ERROR_MESSAGE_FORMAT.format(arg=val))
    return val


def _get_kwargs(optional_args):
    if len(optional_args) % 2:
        raise RuntimeError(
            f'All provided arguments need key-value pairs in {optional_args}')
    return {
        _to_kwarg_key(arg): _to_kwarg_val(value)
        for arg, value in zip(optional_args[0::2], optional_args[1::2])
    }


def fetch_args_kwargs(args=None):
    """
    Fetch parsers and parse input to generate full list of args and keyword args

    :return: args: All the provided arguments
            kwargs: Optional keyword arguments excluding '-p', '-d', '-s', '-l'
                as specified in args_parser.get_base_arg_parser which a cleaning
                rule might require
    """
    basic_parser = get_parser()
    common_args, unknown_args = basic_parser.parse_known_args(args)
    custom_args = _get_kwargs(unknown_args)
    return common_args, custom_args


def get_required_params(rules):
    """
    Get the full set of parameters required to run specified rules
    :param rules: list of cleaning rules
    :return: set of parameter names
    """
    result = set()
    for rule in rules:
        clazz = rule[0]
        rule_args = clean_engine.get_rule_args(clazz)
        result.update(rule['name'] for rule in rule_args if rule['required'])
    return result


def validate_custom_params(rules, **kwargs):
    """
    Raises error if required custom parameters are missing for any CR in list of CRs
    
    :param rules: list of cleaning rule classes/functions
    :param kwargs: dictionary of provided arguments
    :return: None
    :raises: RuntimeError if missing parameters required by any CR
    """
    required_params = get_required_params(rules)
    missing = required_params - set(kwargs.keys()) - set(
        ce_consts.CLEAN_ENGINE_REQUIRED_PARAMS)
    # TODO warn if extra args supplied than rules require
    if missing:
        raise RuntimeError(f'Missing required custom parameter(s): {missing}')


if __name__ == '__main__':
    args, kwargs = fetch_args_kwargs()

    rules = DATA_STAGE_RULES_MAPPING[args.data_stage]
    validate_custom_params(rules, **kwargs)

    if args.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            project_id=args.project_id,
            dataset_id=args.dataset_id,
            sandbox_dataset_id=args.sandbox_dataset_id,
            rules=rules,
            **kwargs)
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(args.console_log)
        clean_engine.clean_dataset(project_id=args.project_id,
                                   dataset_id=args.dataset_id,
                                   sandbox_dataset_id=args.sandbox_dataset_id,
                                   rules=rules,
                                   **kwargs)
