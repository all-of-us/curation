"""
A module to serve as the entry point to the cdr_cleaner package.

It gathers the list of query strings to execute and sends them
to the query engine.
"""
# Python imports
import logging

# Project imports
import cdr_cleaner.clean_cdr_engine as clean_engine
import cdr_cleaner.cleaning_rules.backfill_pmi_skip_codes as back_fill_pmi_skip
import cdr_cleaner.cleaning_rules.clean_years as clean_years
import cdr_cleaner.cleaning_rules.domain_alignment as domain_alignment
import cdr_cleaner.cleaning_rules.drop_duplicate_states as drop_duplicate_states
import cdr_cleaner.cleaning_rules.drop_extreme_measurements as extreme_measurements
import cdr_cleaner.cleaning_rules.drop_multiple_measurements as drop_mult_meas
from cdr_cleaner.cleaning_rules.drop_participants_without_ppi_or_ehr import DropParticipantsWithoutPPI
import cdr_cleaner.cleaning_rules.drug_refills_days_supply as drug_refills_supply
import cdr_cleaner.cleaning_rules.maps_to_value_ppi_vocab_update as maps_to_value_vocab_update
import cdr_cleaner.cleaning_rules.populate_route_ids as populate_routes
import cdr_cleaner.cleaning_rules.remove_aian_participants as remove_aian_participants
import \
    cdr_cleaner.cleaning_rules.remove_invalid_procedure_source_records as invalid_procedure_source
import cdr_cleaner.cleaning_rules.remove_non_matching_participant as validate_missing_participants
import cdr_cleaner.cleaning_rules.remove_records_with_wrong_date as remove_records_with_wrong_date
import cdr_cleaner.cleaning_rules.round_ppi_values_to_nearest_integer as round_ppi_values
import cdr_cleaner.cleaning_rules.update_family_history_qa_codes as update_family_history
import cdr_cleaner.manual_cleaning_rules.clean_smoking_ppi as smoking
import cdr_cleaner.manual_cleaning_rules.negative_ppi as negative_ppi
import cdr_cleaner.manual_cleaning_rules.remove_operational_pii_fields as operational_pii_fields
import \
    cdr_cleaner.manual_cleaning_rules.update_questiona_answers_not_mapped_to_omop as map_questions_answers_to_omop
from cdr_cleaner.cleaning_rules.clean_height_weight import CleanHeightAndWeight
from cdr_cleaner.cleaning_rules.clean_mapping import CleanMappingExtTables
from cdr_cleaner.cleaning_rules.clean_ppi_numeric_fields_using_parameters import \
    CleanPPINumericFieldsUsingParameters
from cdr_cleaner.cleaning_rules.create_person_ext_table import CreatePersonExtTable
from cdr_cleaner.cleaning_rules.date_shift_cope_responses import DateShiftCopeResponses
from cdr_cleaner.cleaning_rules.remove_ehr_data_without_consent import RemoveEhrDataWithoutConsent
from cdr_cleaner.cleaning_rules.generate_ext_tables import GenerateExtTables
from cdr_cleaner.cleaning_rules.deid.ct_pid_rid_map import CtPIDtoRID
from cdr_cleaner.cleaning_rules.deid.fitbit_dateshift import FitbitDateShiftRule
from cdr_cleaner.cleaning_rules.deid.fitbit_pid_rid_map import FitbitPIDtoRID
from cdr_cleaner.cleaning_rules.deid.remove_fitbit_data_if_max_age_exceeded import \
    RemoveFitbitDataIfMaxAgeExceeded
from cdr_cleaner.cleaning_rules.deid.repopulate_person_controlled_tier import \
    RepopulatePersonControlledTier
from cdr_cleaner.cleaning_rules.deid.genaralize_cope_insurance_answers import GeneralizeCopeInsuranceAnswers
from cdr_cleaner.cleaning_rules.drop_cope_duplicate_responses import DropCopeDuplicateResponses
from cdr_cleaner.cleaning_rules.drop_duplicate_ppi_questions_and_answers import \
    DropDuplicatePpiQuestionsAndAnswers
from cdr_cleaner.cleaning_rules.drop_ppi_duplicate_responses import DropPpiDuplicateResponses
from cdr_cleaner.cleaning_rules.drop_zero_concept_ids import DropZeroConceptIDs
from cdr_cleaner.cleaning_rules.ensure_date_datetime_consistency import \
    EnsureDateDatetimeConsistency
from cdr_cleaner.cleaning_rules.fill_source_value_text_fields import FillSourceValueTextFields
from cdr_cleaner.cleaning_rules.fix_unmapped_survey_answers import FixUnmappedSurveyAnswers
from cdr_cleaner.cleaning_rules.id_deduplicate import DeduplicateIdColumn
from cdr_cleaner.cleaning_rules.measurement_table_suppression import MeasurementRecordsSuppression
from cdr_cleaner.cleaning_rules.no_data_30_days_after_death import NoDataAfterDeath
from cdr_cleaner.cleaning_rules.null_concept_ids_for_numeric_ppi import NullConceptIDForNumericPPI
from cdr_cleaner.cleaning_rules.null_invalid_foreign_keys import NullInvalidForeignKeys
from cdr_cleaner.cleaning_rules.ppi_branching import PpiBranching
from cdr_cleaner.cleaning_rules.rdr_observation_source_concept_id_suppression import (
    ObservationSourceConceptIDRowSuppression)
from cdr_cleaner.cleaning_rules.remove_multiple_race_ethnicity_answers import RemoveMultipleRaceEthnicityAnswersQueries
from cdr_cleaner.cleaning_rules.deid.motor_vehicle_accident_suppression import \
    MotorVehicleAccidentSuppression
from cdr_cleaner.cleaning_rules.deid.birth_information_suppression import \
    BirthInformationSuppression
from cdr_cleaner.cleaning_rules.replace_standard_id_in_domain_tables import \
    ReplaceWithStandardConceptId
from cdr_cleaner.cleaning_rules.ehr_submission_data_cutoff import EhrSubmissionDataCutoff
from cdr_cleaner.cleaning_rules.repopulate_person_post_deid import RepopulatePersonPostDeid
from cdr_cleaner.cleaning_rules.truncate_rdr_using_date import TruncateRdrData
from cdr_cleaner.cleaning_rules.unit_normalization import UnitNormalization
from cdr_cleaner.cleaning_rules.update_fields_numbers_as_strings import UpdateFieldsNumbersAsStrings
from cdr_cleaner.cleaning_rules.temporal_consistency import TemporalConsistency
from cdr_cleaner.cleaning_rules.valid_death_dates import ValidDeathDates
from cdr_cleaner.cleaning_rules.negative_ages import NegativeAges
from cdr_cleaner.cleaning_rules.deid.explicit_identifier_suppression import ExplicitIdentifierSuppression
from cdr_cleaner.cleaning_rules.deid.geolocation_concept_suppression import GeoLocationConceptSuppression
from cdr_cleaner.cleaning_rules.null_person_birthdate import NullPersonBirthdate
from cdr_cleaner.cleaning_rules.race_ethnicity_record_suppression import RaceEthnicityRecordSuppression
from cdr_cleaner.cleaning_rules.table_suppression import TableSuppression
from cdr_cleaner.cleaning_rules.deid.cope_survey_response_suppression import CopeSurveyResponseSuppression
from cdr_cleaner.cleaning_rules.deid.registered_cope_survey_suppression import RegisteredCopeSurveyQuestionsSuppression
from cdr_cleaner.cleaning_rules.deid.questionnaire_response_id_map import QRIDtoRID
from cdr_cleaner.cleaning_rules.generalize_zip_codes import GeneralizeZipCodes
from cdr_cleaner.cleaning_rules.free_text_survey_response_suppression import FreeTextSurveyResponseSuppression
from cdr_cleaner.cleaning_rules.cancer_concept_suppression import CancerConceptSuppression
from cdr_cleaner.cleaning_rules.deid.organ_transplant_concept_suppression import OrganTransplantConceptSuppression
from cdr_cleaner.cleaning_rules.identifying_field_suppression import IDFieldSuppression
from cdr_cleaner.cleaning_rules.aggregate_zip_codes import AggregateZipCodes
from cdr_cleaner.cleaning_rules.remove_extra_tables import RemoveExtraTables
from cdr_cleaner.cleaning_rules.store_pid_rid_mappings import StoreNewPidRidMappings
from cdr_cleaner.cleaning_rules.update_invalid_zip_codes import UpdateInvalidZipCodes
from cdr_cleaner.manual_cleaning_rules.survey_version_info import COPESurveyVersionTask
from cdr_cleaner.cleaning_rules.deid.string_fields_suppression import StringFieldsSuppression
from cdr_cleaner.cleaning_rules.generalize_state_by_population import GeneralizeStateByPopulation
from cdr_cleaner.cleaning_rules.section_participation_concept_suppression import SectionParticipationConceptSuppression
from cdr_cleaner.cleaning_rules.covid_ehr_vaccine_concept_suppression import CovidEHRVaccineConceptSuppression
from cdr_cleaner.cleaning_rules.truncate_fitbit_data import TruncateFitbitData
from constants.cdr_cleaner import clean_cdr_engine as ce_consts
from constants.cdr_cleaner.clean_cdr import DataStage

# Third party imports

LOGGER = logging.getLogger(__name__)

EHR_CLEANING_CLASSES = [
    (CleanMappingExtTables,),  # should be one of the last cleaning rules run
]

UNIONED_EHR_CLEANING_CLASSES = [
    (EhrSubmissionDataCutoff,
    ),  # should run before EnsureDateDatetimeConsistency
    (DeduplicateIdColumn,),
    (clean_years.get_year_of_birth_queries,),
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
    (CleanMappingExtTables,),  # should be one of the last cleaning rules run
]

RDR_CLEANING_CLASSES = [
    (StoreNewPidRidMappings,),
    (TruncateRdrData,),
    (PpiBranching,),
    # execute FixUnmappedSurveyAnswers before the dropping responses rules get executed
    # (e.g. DropPpiDuplicateResponses and DropDuplicatePpiQuestionsAndAnswers)
    (
        FixUnmappedSurveyAnswers,),
    (ObservationSourceConceptIDRowSuppression,),
    (UpdateFieldsNumbersAsStrings,),
    (maps_to_value_vocab_update.get_maps_to_value_ppi_vocab_update_queries,),
    (back_fill_pmi_skip.get_run_pmi_fix_queries,),
    (CleanPPINumericFieldsUsingParameters,),
    (RemoveMultipleRaceEthnicityAnswersQueries,),
    (negative_ppi.get_update_ppi_queries,),
    # trying to load a table while creating query strings,
    # won't work with mocked strings.  should use base class
    # setup_query_execution function to load dependencies before query execution
    (
        smoking.get_queries_clean_smoking,),
    (DropPpiDuplicateResponses,),
    (DropCopeDuplicateResponses,),
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
    (NullConceptIDForNumericPPI,),
    (DropDuplicatePpiQuestionsAndAnswers,),
    (extreme_measurements.get_drop_extreme_measurement_queries,),
    (drop_mult_meas.get_drop_multiple_measurement_queries,),
    (UpdateInvalidZipCodes,),
]

COMBINED_CLEANING_CLASSES = [
    # trying to load a table while creating query strings,
    # won't work with mocked strings.  should use base class
    # setup_query_execution function to load dependencies before query execution
    (
        ReplaceWithStandardConceptId,),
    # trying to load a table while creating query strings,
    # won't work with mocked strings.  should use base class
    # setup_query_execution function to load dependencies before query execution
    (
        domain_alignment.domain_alignment,),
    (DropParticipantsWithoutPPI,),
    (clean_years.get_year_of_birth_queries,),
    (NegativeAges,),
    # Valid Death dates needs to be applied before no data after death as running no data after death is
    # wiping out the needed consent related data for cleaning.
    (
        ValidDeathDates,),
    (NoDataAfterDeath,),
    (RemoveEhrDataWithoutConsent,),
    (drug_refills_supply.get_days_supply_refills_queries,),
    # trying to load a table while creating query strings,
    # won't work with mocked strings.  should use base class
    # setup_query_execution function to load dependencies before query execution
    (
        populate_routes.get_route_mapping_queries,),
    (TemporalConsistency,),
    (EnsureDateDatetimeConsistency,),  # dependent on TemporalConsistency
    (remove_records_with_wrong_date.get_remove_records_with_wrong_date_queries,
    ),
    (drop_duplicate_states.get_drop_duplicate_states_queries,),
    # TODO : Make null_invalid_foreign_keys able to run on de_identified dataset
    (
        NullInvalidForeignKeys,),
    (remove_aian_participants.get_queries,),
    (validate_missing_participants.delete_records_for_non_matching_participants,
    ),
    (CleanMappingExtTables,),  # should be one of the last cleaning rules run
]

FITBIT_CLEANING_CLASSES = [
    (TruncateFitbitData,),
]

FITBIT_DEID_CLEANING_CLASSES = [
    (RemoveFitbitDataIfMaxAgeExceeded,),
    (FitbitPIDtoRID,),
    (FitbitDateShiftRule,),
]

CONTROLLED_TIER_FITBIT_CLEANING_CLASSES = [
    (FitbitPIDtoRID,),
]

DEID_BASE_CLEANING_CLASSES = [
    (FillSourceValueTextFields,),
    (RepopulatePersonPostDeid,),
    (DateShiftCopeResponses,),
    (CreatePersonExtTable,),
    (CleanMappingExtTables,),  # should be one of the last cleaning rules run
]

DEID_CLEAN_CLEANING_CLASSES = [
    (MeasurementRecordsSuppression,),
    (CleanHeightAndWeight,),  # dependent on MeasurementRecordsSuppression
    (UnitNormalization,),  # dependent on CleanHeightAndWeight
    (DropZeroConceptIDs,),
    (CleanMappingExtTables,),  # should be one of the last cleaning rules run
]

CONTROLLED_TIER_DEID_CLEANING_CLASSES = [
    (CtPIDtoRID,),
    (QRIDtoRID,),  # Should run before any row suppression rules
    (NullPersonBirthdate,),
    (TableSuppression,),
    (GeneralizeZipCodes,),  # Should run after any data remapping rules
    (RaceEthnicityRecordSuppression,
    ),  # Should run after any data remapping rules
    (FreeTextSurveyResponseSuppression,
    ),  # Should run after any data remapping rules
    (MotorVehicleAccidentSuppression,),
    (ExplicitIdentifierSuppression,),
    (GeoLocationConceptSuppression,),
    (OrganTransplantConceptSuppression,),
    (BirthInformationSuppression,),
    (StringFieldsSuppression,),
    (CopeSurveyResponseSuppression,),
    (IDFieldSuppression,),  # Should run after any data remapping
    (GenerateExtTables,),
    (COPESurveyVersionTask,
    ),  # Should run after GenerateExtTables and before CleanMappingExtTables
    (CancerConceptSuppression,),  # Should run after any data remapping rules
    (AggregateZipCodes,),
    (SectionParticipationConceptSuppression,),
    (RemoveExtraTables,),  # Should be last cleaning rule to be run
    (CleanMappingExtTables,),  # should be one of the last cleaning rules run
]

CONTROLLED_TIER_DEID_BASE_CLEANING_CLASSES = [
    (FillSourceValueTextFields,),
    (RepopulatePersonControlledTier,),
    (CreatePersonExtTable,),
    (CleanMappingExtTables,),  # should be one of the last cleaning rules run
]

CONTROLLED_TIER_DEID_CLEAN_CLEANING_CLASSES = [
    (MeasurementRecordsSuppression,),
    (CleanHeightAndWeight,),  # dependent on MeasurementRecordsSuppression
    (UnitNormalization,),  # dependent on CleanHeightAndWeight
    (DropZeroConceptIDs,),
    (CleanMappingExtTables,),  # should be one of the last cleaning rules run
]

REGISTERED_TIER_DEID_CLEANING_CLASSES = [
    # Data mappings/re-mappings
    ####################################
    (
        QRIDtoRID,),  # Should run before any row suppression rules
    (GenerateExtTables,),
    (COPESurveyVersionTask,
    ),  # Should run after GenerateExtTables and before CleanMappingExtTables

    # Data generalizations
    ####################################
    (
        GeneralizeStateByPopulation,),
    (GeneralizeCopeInsuranceAnswers,),

    # Data suppressions
    ####################################
    (
        CovidEHRVaccineConceptSuppression,),  # should run after QRIDtoRID
    (StringFieldsSuppression,),
    (SectionParticipationConceptSuppression,),
    (RegisteredCopeSurveyQuestionsSuppression,),
]

DATA_STAGE_RULES_MAPPING = {
    DataStage.EHR.value:
        EHR_CLEANING_CLASSES,
    DataStage.UNIONED.value:
        UNIONED_EHR_CLEANING_CLASSES,
    DataStage.RDR.value:
        RDR_CLEANING_CLASSES,
    DataStage.COMBINED.value:
        COMBINED_CLEANING_CLASSES,
    DataStage.DEID_BASE.value:
        DEID_BASE_CLEANING_CLASSES,
    DataStage.DEID_CLEAN.value:
        DEID_CLEAN_CLEANING_CLASSES,
    DataStage.FITBIT.value:
        FITBIT_CLEANING_CLASSES,
    DataStage.CONTROLLED_TIER_FITBIT.value:
        CONTROLLED_TIER_FITBIT_CLEANING_CLASSES,
    DataStage.FITBIT_DEID.value:
        FITBIT_DEID_CLEANING_CLASSES,
    DataStage.CONTROLLED_TIER_DEID.value:
        CONTROLLED_TIER_DEID_CLEANING_CLASSES,
    DataStage.CONTROLLED_TIER_DEID_BASE.value:
        CONTROLLED_TIER_DEID_BASE_CLEANING_CLASSES,
    DataStage.CONTROLLED_TIER_DEID_CLEAN.value:
        CONTROLLED_TIER_DEID_CLEAN_CLEANING_CLASSES,
    DataStage.REGISTERED_TIER_DEID.value:
        REGISTERED_TIER_DEID_CLEANING_CLASSES
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
    # TODO: Move this function to as project level arg_parser so it can be reused.
    if not arg.startswith('--'):
        raise RuntimeError(PARSING_ERROR_MESSAGE_FORMAT.format(arg=arg))
    key = arg[2:]
    if not key:
        raise RuntimeError(PARSING_ERROR_MESSAGE_FORMAT.format(arg=arg))
    return key


def _to_kwarg_val(val):
    # TODO: Move this function to as project level arg_parser so it can be reused.
    # likely invalid use of args- allowing single dash e.g. negative values
    if val.startswith('--'):
        raise RuntimeError(PARSING_ERROR_MESSAGE_FORMAT.format(arg=val))
    return val


def _get_kwargs(optional_args):
    # TODO: Move this function to as project level arg_parser so it can be reused.
    # TODO: Move this function to as project level arg_parser so it can be reused.
    if len(optional_args) % 2:
        raise RuntimeError(
            f'All provided arguments need key-value pairs in {optional_args}')
    return {
        _to_kwarg_key(arg): _to_kwarg_val(value)
        for arg, value in zip(optional_args[0::2], optional_args[1::2])
    }


def fetch_args_kwargs(parser, args=None):
    """
    Fetch parsers and parse input to generate full list of args and keyword args

    :return: args: All the provided arguments
            kwargs: Optional keyword arguments excluding '-p', '-d', '-s', '-l'
                as specified in args_parser.get_base_arg_parser which a cleaning
                rule might require
    """
    # TODO: Move this function to as project level arg_parser so it can be reused.
    common_args, unknown_args = parser.parse_known_args(args)
    custom_args = _get_kwargs(unknown_args)
    return common_args, custom_args


def get_required_params(rules):
    """
    Get the full set of parameters required to run specified rules
    :param rules: list of cleaning rules
    :return: set of parameter names
    """
    result = dict()
    for rule in rules:
        clazz = rule[0]
        rule_args = clean_engine.get_rule_args(clazz)
        for rule_arg in rule_args:
            if rule_arg['required']:
                param_name = rule_arg['name']
                if param_name not in result.keys():
                    result[param_name] = list()
                result[param_name].append(clazz.__name__)
    return result


def get_missing_custom_params(rules, **kwargs):
    required_params = get_required_params(rules)
    missing = set(required_params.keys()) - set(kwargs.keys()) - set(
        ce_consts.CLEAN_ENGINE_REQUIRED_PARAMS)
    missing_param_rules = {
        k: v for k, v in required_params.items() if k in missing
    }
    return missing_param_rules


def validate_custom_params(rules, **kwargs):
    """
    Raises error if required custom parameters are missing for any CR in list of CRs

    :param rules: list of cleaning rule classes/functions
    :param kwargs: dictionary of provided arguments
    :return: None
    :raises: RuntimeError if missing parameters required by any CR
    """
    missing_param_rules = get_missing_custom_params(rules, **kwargs)
    # TODO warn if extra args supplied than rules require
    if missing_param_rules:
        raise RuntimeError(
            f'Missing required custom parameter(s): {missing_param_rules}')


def main(args=None):
    """
    :param args: list of all the arguments to apply the cleaning rules
    :return:
    """
    parser = get_parser()
    args, kwargs = fetch_args_kwargs(parser, args)

    rules = DATA_STAGE_RULES_MAPPING[args.data_stage.value]
    validate_custom_params(rules, **kwargs)

    if args.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            project_id=args.project_id,
            dataset_id=args.dataset_id,
            sandbox_dataset_id=args.sandbox_dataset_id,
            rules=rules,
            table_namer=args.data_stage.value,
            **kwargs)
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(args.console_log)
        clean_engine.clean_dataset(project_id=args.project_id,
                                   dataset_id=args.dataset_id,
                                   sandbox_dataset_id=args.sandbox_dataset_id,
                                   rules=rules,
                                   table_namer=args.data_stage.value,
                                   **kwargs)


if __name__ == '__main__':
    main()
