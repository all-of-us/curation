"""
A module to serve as the entry point to the cdr_cleaner package.

It gathers the list of query strings to execute and sends them
to the query engine.
"""
# Python imports
import logging
import typing

# Project imports
import cdr_cleaner.clean_cdr_engine as clean_engine
from cdr_cleaner.cleaning_rules.backfill_lifestyle import BackfillLifestyle
from cdr_cleaner.cleaning_rules.backfill_overall_health import BackfillOverallHealth
from cdr_cleaner.cleaning_rules.backfill_the_basics import BackfillTheBasics
from cdr_cleaner.cleaning_rules.calculate_bmi import CalculateBmi
from cdr_cleaner.cleaning_rules.calculate_primary_death_record import CalculatePrimaryDeathRecord
from cdr_cleaner.cleaning_rules.clean_by_birth_year import CleanByBirthYear
from cdr_cleaner.cleaning_rules.convert_pre_post_coordinated_concepts import ConvertPrePostCoordinatedConcepts
from cdr_cleaner.cleaning_rules.create_aian_lookup import CreateAIANLookup
from cdr_cleaner.cleaning_rules.create_expected_ct_list import StoreExpectedCTList
from cdr_cleaner.cleaning_rules.deid.ct_additional_privacy_suppression import CTAdditionalPrivacyConceptSuppression
from cdr_cleaner.cleaning_rules.deid.rt_additional_privacy_suppression import RTAdditionalPrivacyConceptSuppression
from cdr_cleaner.cleaning_rules.domain_alignment import DomainAlignment
import cdr_cleaner.cleaning_rules.drop_duplicate_states as drop_duplicate_states
from cdr_cleaner.cleaning_rules.drop_extreme_measurements import DropExtremeMeasurements
from cdr_cleaner.cleaning_rules.drop_multiple_measurements import DropMultipleMeasurements
from cdr_cleaner.cleaning_rules.drop_participants_without_any_basics import DropParticipantsWithoutAnyBasics
from cdr_cleaner.cleaning_rules.clean_survey_conduct_recurring_surveys import CleanSurveyConductRecurringSurveys
from cdr_cleaner.cleaning_rules.update_survey_source_concept_id import UpdateSurveySourceConceptId
from cdr_cleaner.cleaning_rules.drop_unverified_survey_data import DropUnverifiedSurveyData
from cdr_cleaner.cleaning_rules.drug_refills_days_supply import DrugRefillsDaysSupply
from cdr_cleaner.cleaning_rules.maps_to_value_ppi_vocab_update import MapsToValuePpiVocabUpdate
from cdr_cleaner.cleaning_rules.populate_route_ids import PopulateRouteIds
from cdr_cleaner.cleaning_rules.populate_survey_conduct_ext import PopulateSurveyConductExt
from cdr_cleaner.cleaning_rules.remove_invalid_procedure_source_records import RemoveInvalidProcedureSourceRecords
from cdr_cleaner.cleaning_rules.remove_non_matching_participant import RemoveNonMatchingParticipant
from cdr_cleaner.cleaning_rules.sandbox_and_remove_withdrawn_pids import SandboxAndRemoveWithdrawnPids
from cdr_cleaner.cleaning_rules.remove_records_with_wrong_date import RemoveRecordsWithWrongDate
from cdr_cleaner.cleaning_rules.remove_participants_under_18years import RemoveParticipantsUnder18Years
from cdr_cleaner.cleaning_rules.round_ppi_values_to_nearest_integer import RoundPpiValuesToNearestInteger
from cdr_cleaner.cleaning_rules.replace_freetext_notes import ReplaceFreeTextNotes
from cdr_cleaner.cleaning_rules.update_family_history_qa_codes import UpdateFamilyHistoryCodes
from cdr_cleaner.cleaning_rules.remove_operational_pii_fields import RemoveOperationalPiiFields
from cdr_cleaner.cleaning_rules.update_ppi_negative_pain_level import UpdatePpiNegativePainLevel
from cdr_cleaner.cleaning_rules.clean_height_weight import CleanHeightAndWeight
from cdr_cleaner.cleaning_rules.clean_mapping import CleanMappingExtTables
from cdr_cleaner.cleaning_rules.clean_ppi_numeric_fields_using_parameters import \
    CleanPPINumericFieldsUsingParameters
from cdr_cleaner.cleaning_rules.create_person_ext_table import CreatePersonExtTable
from cdr_cleaner.cleaning_rules.date_unshift_cope_responses import DateUnShiftCopeResponses
from cdr_cleaner.cleaning_rules.deid.survey_conduct_dateshift import SurveyConductDateShiftRule
from cdr_cleaner.cleaning_rules.remove_ehr_data_without_consent import RemoveEhrDataWithoutConsent
from cdr_cleaner.cleaning_rules.generate_ext_tables import GenerateExtTables
from cdr_cleaner.cleaning_rules.truncate_fitbit_data import TruncateFitbitData
from cdr_cleaner.cleaning_rules.truncate_era_tables import TruncateEraTables
# from cdr_cleaner.cleaning_rules.clean_digital_health_data import CleanDigitalHealthStatus
from cdr_cleaner.cleaning_rules.remove_non_existing_pids import RemoveNonExistingPids
from cdr_cleaner.cleaning_rules.drop_invalid_sleep_level_records import DropInvalidSleepLevelRecords
from cdr_cleaner.cleaning_rules.deid.fitbit_dateshift import FitbitDateShiftRule
from cdr_cleaner.cleaning_rules.deid.fitbit_deid_src_id import FitbitDeidSrcID
from cdr_cleaner.cleaning_rules.deid.fitbit_pid_rid_map import FitbitPIDtoRID
from cdr_cleaner.cleaning_rules.deid.remove_fitbit_data_if_max_age_exceeded import \
    RemoveFitbitDataIfMaxAgeExceeded
from cdr_cleaner.cleaning_rules.deid.rt_ct_pid_rid_map import RtCtPIDtoRID
from cdr_cleaner.cleaning_rules.deid.repopulate_person_controlled_tier import \
    RepopulatePersonControlledTier
from cdr_cleaner.cleaning_rules.deid.conflicting_hpo_state_generalization import \
    ConflictingHpoStateGeneralize
from cdr_cleaner.cleaning_rules.deid.generalize_cope_insurance_answers import GeneralizeCopeInsuranceAnswers
from cdr_cleaner.cleaning_rules.deid.generalize_indian_health_services import GeneralizeIndianHealthServices
from cdr_cleaner.cleaning_rules.drop_cope_duplicate_responses import DropCopeDuplicateResponses
from cdr_cleaner.cleaning_rules.drop_duplicate_ppi_questions_and_answers import \
    DropDuplicatePpiQuestionsAndAnswers
from cdr_cleaner.cleaning_rules.drop_row_duplicates import DropRowDuplicates
from cdr_cleaner.cleaning_rules.clean_smoking_ppi import CleanSmokingPpi
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
from cdr_cleaner.cleaning_rules.deid.year_of_birth_records_suppression import \
    YearOfBirthRecordsSuppression
from cdr_cleaner.cleaning_rules.replace_standard_id_in_domain_tables import \
    ReplaceWithStandardConceptId
from cdr_cleaner.cleaning_rules.remove_participant_data_past_deactivation_date import \
    RemoveParticipantDataPastDeactivationDate
from cdr_cleaner.cleaning_rules.ehr_submission_data_cutoff import EhrSubmissionDataCutoff
from cdr_cleaner.cleaning_rules.repopulate_person_post_deid import RepopulatePersonPostDeid
from cdr_cleaner.cleaning_rules.truncate_rdr_using_date import TruncateRdrData
from cdr_cleaner.cleaning_rules.unit_normalization import UnitNormalization
from cdr_cleaner.cleaning_rules.update_cope_flu_concepts import UpdateCopeFluQuestionConcept
from cdr_cleaner.cleaning_rules.update_fields_numbers_as_strings import UpdateFieldsNumbersAsStrings
from cdr_cleaner.cleaning_rules.temporal_consistency import TemporalConsistency
from cdr_cleaner.cleaning_rules.valid_death_dates import ValidDeathDates
from cdr_cleaner.cleaning_rules.negative_ages import NegativeAges
from cdr_cleaner.cleaning_rules.deid.explicit_identifier_suppression import ExplicitIdentifierSuppression
from cdr_cleaner.cleaning_rules.deid.geolocation_concept_suppression import GeoLocationConceptSuppression
from cdr_cleaner.cleaning_rules.null_person_birthdate import NullPersonBirthdate
from cdr_cleaner.cleaning_rules.race_ethnicity_record_suppression import RaceEthnicityRecordSuppression
from cdr_cleaner.cleaning_rules.table_suppression import TableSuppression
from cdr_cleaner.cleaning_rules.deid.controlled_cope_survey_suppression import ControlledCopeSurveySuppression
from cdr_cleaner.cleaning_rules.deid.registered_cope_survey_suppression import RegisteredCopeSurveyQuestionsSuppression
from cdr_cleaner.cleaning_rules.deid.questionnaire_response_id_map import QRIDtoRID
from cdr_cleaner.cleaning_rules.generalize_zip_codes import GeneralizeZipCodes
from cdr_cleaner.cleaning_rules.free_text_survey_response_suppression import FreeTextSurveyResponseSuppression
from cdr_cleaner.cleaning_rules.cancer_concept_suppression import CancerConceptSuppression
from cdr_cleaner.cleaning_rules.identifying_field_suppression import IDFieldSuppression
from cdr_cleaner.cleaning_rules.aggregate_zip_codes import AggregateZipCodes
from cdr_cleaner.cleaning_rules.remove_extra_tables import RemoveExtraTables
from cdr_cleaner.cleaning_rules.store_pid_rid_mappings import StoreNewPidRidMappings
from cdr_cleaner.cleaning_rules.store_new_duplicate_measurement_concept_ids import \
    StoreNewDuplicateMeasurementConceptIds
from cdr_cleaner.cleaning_rules.update_invalid_zip_codes import UpdateInvalidZipCodes
from cdr_cleaner.cleaning_rules.deid.survey_version_info import COPESurveyVersionTask
from cdr_cleaner.cleaning_rules.deid.string_fields_suppression import StringFieldsSuppression
from cdr_cleaner.cleaning_rules.generalize_sex_gender_concepts import GeneralizeSexGenderConcepts
from cdr_cleaner.cleaning_rules.generalize_state_by_population import GeneralizeStateByPopulation
from cdr_cleaner.cleaning_rules.section_participation_concept_suppression import SectionParticipationConceptSuppression
from cdr_cleaner.cleaning_rules.deid.recent_concept_suppression import RecentConceptSuppression
from cdr_cleaner.cleaning_rules.missing_concept_record_suppression import MissingConceptRecordSuppression
from cdr_cleaner.cleaning_rules.create_deid_questionnaire_response_map import CreateDeidQuestionnaireResponseMap
from cdr_cleaner.cleaning_rules.set_unmapped_question_answer_survey_concepts import (
    SetConceptIdsForSurveyQuestionsAnswers)
from cdr_cleaner.cleaning_rules.map_health_insurance_responses import MapHealthInsuranceResponses
from cdr_cleaner.cleaning_rules.vehicular_accident_concept_suppression import VehicularAccidentConceptSuppression
from cdr_cleaner.cleaning_rules.deid.ct_replaced_concept_suppression import \
    ControlledTierReplacedConceptSuppression
from cdr_cleaner.cleaning_rules.dedup_measurement_value_as_concept_id import DedupMeasurementValueAsConceptId
from cdr_cleaner.cleaning_rules.drop_orphaned_pids import DropOrphanedPIDS
from cdr_cleaner.cleaning_rules.drop_orphaned_survey_conduct_ids import DropOrphanedSurveyConductIds
from cdr_cleaner.cleaning_rules.deid.deidentify_aian_zip3_values import DeidentifyAIANZip3Values
import constants.global_variables
from constants.cdr_cleaner import clean_cdr_engine as ce_consts
from constants.cdr_cleaner.clean_cdr import DataStage, DATA_CONSISTENCY, CRON_RETRACTION
from cdr_cleaner.cleaning_rules.generate_research_device_ids import GenerateResearchDeviceIds
from cdr_cleaner.cleaning_rules.deid.fitbit_device_id import DeidFitbitDeviceId
from cdr_cleaner.cleaning_rules.drop_survey_data_via_survey_conduct import DropViaSurveyConduct
from cdr_cleaner.cleaning_rules.generate_wear_study_table import GenerateWearStudyTable

# Third party imports

LOGGER = logging.getLogger(__name__)

EHR_CLEANING_CLASSES = [
    (CleanMappingExtTables,),  # should be one of the last cleaning rules run
]

UNIONED_EHR_CLEANING_CLASSES = [
    (EhrSubmissionDataCutoff,
    ),  # should run before EnsureDateDatetimeConsistency
    (DeduplicateIdColumn,),
    (CleanByBirthYear,),
    (EnsureDateDatetimeConsistency,),
    (RemoveRecordsWithWrongDate,),
    (RemoveInvalidProcedureSourceRecords,),
    (CalculatePrimaryDeathRecord,),
    (CleanMappingExtTables,),  # should be one of the last cleaning rules run
]

RDR_CLEANING_CLASSES = [
    (StoreNewPidRidMappings,),
    (CreateDeidQuestionnaireResponseMap,),
    (CreateAIANLookup,),
    (TruncateRdrData,),
    (RemoveParticipantsUnder18Years,),
    (SandboxAndRemoveWithdrawnPids,),
    # execute SetConceptIdsForSurveyQuestionAnswers before PpiBranching gets executed
    # since PpiBranching relies on fully mapped concepts
    (
        SetConceptIdsForSurveyQuestionsAnswers,),
    (MapHealthInsuranceResponses,),
    (PpiBranching,),
    # execute FixUnmappedSurveyAnswers before the dropping responses rules get executed
    # (e.g. DropPpiDuplicateResponses and DropDuplicatePpiQuestionsAndAnswers)
    (
        FixUnmappedSurveyAnswers,),
    (ObservationSourceConceptIDRowSuppression,),
    (UpdateFieldsNumbersAsStrings,),
    (UpdateCopeFluQuestionConcept,),
    (MapsToValuePpiVocabUpdate,),
    (BackfillTheBasics,),
    (BackfillLifestyle,),
    (BackfillOverallHealth,),
    (CleanPPINumericFieldsUsingParameters,),
    (RemoveMultipleRaceEthnicityAnswersQueries,),
    (UpdatePpiNegativePainLevel,),
    # trying to load a table while creating query strings,
    # won't work with mocked strings.  should use base class
    # setup_query_execution function to load dependencies before query execution
    (
        DropPpiDuplicateResponses,),
    (DropCopeDuplicateResponses,),
    (RemoveOperationalPiiFields,),
    (RoundPpiValuesToNearestInteger,),
    (UpdateFamilyHistoryCodes,),
    (ConvertPrePostCoordinatedConcepts,),
    (CleanSmokingPpi,),
    (NullConceptIDForNumericPPI,),
    (DropDuplicatePpiQuestionsAndAnswers,),
    (CalculateBmi,),
    (DropExtremeMeasurements,),
    (DropMultipleMeasurements,),
    (CleanByBirthYear,),
    (UpdateInvalidZipCodes,),
    (CleanSurveyConductRecurringSurveys,),
    (UpdateSurveySourceConceptId,),
    (DropUnverifiedSurveyData,),
    (DropParticipantsWithoutAnyBasics,),
    (StoreExpectedCTList,),
    (DropOrphanedSurveyConductIds,),
    (CalculatePrimaryDeathRecord,),
    (DropRowDuplicates,),
    (CleanMappingExtTables,),
]

COMBINED_CLEANING_CLASSES = [
    # trying to load a table while creating query strings,
    # won't work with mocked strings.  should use base class
    # setup_query_execution function to load dependencies before query execution
    (
        ReplaceWithStandardConceptId,),
    (MissingConceptRecordSuppression,),
    (DomainAlignment,),
    (NegativeAges,),
    # Valid Death dates needs to be applied before no data after death as running no data after death is
    # wiping out the needed consent related data for cleaning.
    (
        ValidDeathDates,),
    (RemoveEhrDataWithoutConsent,),
    (StoreNewDuplicateMeasurementConceptIds,),
    (DedupMeasurementValueAsConceptId,),
    (DrugRefillsDaysSupply,),
    (PopulateRouteIds,),
    (TemporalConsistency,),
    (EnsureDateDatetimeConsistency,),
    (drop_duplicate_states.get_drop_duplicate_states_queries,),
    # TODO : Make null_invalid_foreign_keys able to run on de_identified dataset
    (
        NullInvalidForeignKeys,),
    (RemoveParticipantDataPastDeactivationDate,),
    (RemoveNonMatchingParticipant,),
    (ReplaceFreeTextNotes,),
    (DropOrphanedSurveyConductIds,),
    (DropOrphanedPIDS,),
    (GenerateExtTables,),
    (COPESurveyVersionTask,
    ),  # Should run after GenerateExtTables and before CleanMappingExtTables
    (PopulateSurveyConductExt,),
    (CalculatePrimaryDeathRecord,),
    (NoDataAfterDeath,),  # should run after CalculatePrimaryDeathRecord
    (CleanMappingExtTables,),  # should be one of the last cleaning rules run
]

FITBIT_CLEANING_CLASSES = [
    (TruncateFitbitData,),
    (RemoveParticipantDataPastDeactivationDate,),
    # (CleanDigitalHealthStatus,),
    (
        DropInvalidSleepLevelRecords,),
    (RemoveNonExistingPids,),  # assumes combined dataset is ready for reference
    (GenerateResearchDeviceIds,),
]

REGISTERED_TIER_DEID_CLEANING_CLASSES = [
    # Data mappings/re-mappings
    ####################################
    # TODO: Uncomment rule after date-shift removed from deid module
    # (SurveyConductDateShiftRule,),
    (
        QRIDtoRID,),  # Should run before any row suppression rules

    # Data generalizations
    ####################################
    (
        ConflictingHpoStateGeneralize,),
    (GeneralizeStateByPopulation,),
    (GeneralizeCopeInsuranceAnswers,),
    (GeneralizeIndianHealthServices,),
    # (GeneralizeSexGenderConcepts,),

    # Data suppressions
    ####################################
    (
        RecentConceptSuppression,),  # should run after QRIDtoRID
    (VehicularAccidentConceptSuppression,),
    (BirthInformationSuppression,),  # run after VehicularAccidentConcept
    (SectionParticipationConceptSuppression,),
    (RegisteredCopeSurveyQuestionsSuppression,),
    (ExplicitIdentifierSuppression,),
    (CancerConceptSuppression,),
    (RTAdditionalPrivacyConceptSuppression,),
    (StringFieldsSuppression,),
    (FreeTextSurveyResponseSuppression,),
    (DropOrphanedSurveyConductIds,),
    (DropOrphanedPIDS,),
    (CalculatePrimaryDeathRecord,),
    (GenerateWearStudyTable,),
    (DropViaSurveyConduct,),  # should run after wear study table creation
    (CleanMappingExtTables,),  # should be one of the last cleaning rules run
]

REGISTERED_TIER_DEID_BASE_CLEANING_CLASSES = [
    (FillSourceValueTextFields,),
    (RepopulatePersonPostDeid,),
    (DateUnShiftCopeResponses,),
    (CreatePersonExtTable,),
    (CalculatePrimaryDeathRecord,),
    (CleanMappingExtTables,),  # should be one of the last cleaning rules run
]

REGISTERED_TIER_DEID_CLEAN_CLEANING_CLASSES = [
    # TODO: uncomment when pid-rid logic is removed from legacy deid
    # (RtCtPIDtoRID,),
    (
        MeasurementRecordsSuppression,),
    (CleanHeightAndWeight,),  # dependent on MeasurementRecordsSuppression
    (UnitNormalization,),  # dependent on CleanHeightAndWeight
    (DropZeroConceptIDs,),
    (DropOrphanedSurveyConductIds,),
    (CalculatePrimaryDeathRecord,),
    (NoDataAfterDeath,),  # should run after CalculatePrimaryDeathRecord
    (CleanMappingExtTables,),  # should be one of the last cleaning rules run
]

REGISTERED_TIER_FITBIT_CLEANING_CLASSES = [
    (RemoveFitbitDataIfMaxAgeExceeded,),
    (DeidFitbitDeviceId,
    ),  # This rule must occur so that PID can map to device_id
    (FitbitPIDtoRID,),
    (FitbitDeidSrcID,),
    (RemoveNonExistingPids,),  # assumes RT dataset is ready for reference
    (FitbitDateShiftRule,),
]

CONTROLLED_TIER_DEID_CLEANING_CLASSES = [
    (RtCtPIDtoRID,),
    (QRIDtoRID,),  # Should run before any row suppression rules
    (TruncateEraTables,),
    (NullPersonBirthdate,),
    (TableSuppression,),
    (ControlledTierReplacedConceptSuppression,),
    (GeneralizeZipCodes,),  # Should run after any data remapping rules
    # (RaceEthnicityRecordSuppression,),  # Should run after any data remapping rules
    (
        MotorVehicleAccidentSuppression,),
    (VehicularAccidentConceptSuppression,),
    (ExplicitIdentifierSuppression,),
    (GeoLocationConceptSuppression,),
    (BirthInformationSuppression,),
    (YearOfBirthRecordsSuppression,),
    (ControlledCopeSurveySuppression,),
    (IDFieldSuppression,),  # Should run after any data remapping
    (CancerConceptSuppression,),  # Should run after any data remapping rules
    (SectionParticipationConceptSuppression,),
    (CTAdditionalPrivacyConceptSuppression,),
    (StringFieldsSuppression,),
    (AggregateZipCodes,),
    (DeidentifyAIANZip3Values,),
    (FreeTextSurveyResponseSuppression,),
    (DropOrphanedSurveyConductIds,),
    (DropOrphanedPIDS,),
    (GenerateWearStudyTable,),
    (DropViaSurveyConduct,),  # should run after wear study table creation
    (RemoveExtraTables,),  # Should be last cleaning rule to be run
    (CalculatePrimaryDeathRecord,),
    (CleanMappingExtTables,),  # should be one of the last cleaning rules run
]

CONTROLLED_TIER_DEID_BASE_CLEANING_CLASSES = [
    (FillSourceValueTextFields,),
    (RepopulatePersonControlledTier,),
    (CreatePersonExtTable,),
    (CalculatePrimaryDeathRecord,),
    (CleanMappingExtTables,),  # should be one of the last cleaning rules run
]

CONTROLLED_TIER_DEID_CLEAN_CLEANING_CLASSES = [
    (MeasurementRecordsSuppression,),
    (CleanHeightAndWeight,),  # dependent on MeasurementRecordsSuppression
    (UnitNormalization,),  # dependent on CleanHeightAndWeight
    (DropZeroConceptIDs,),
    (DropOrphanedSurveyConductIds,),
    (CalculatePrimaryDeathRecord,),
    (NoDataAfterDeath,),  # should run after CalculatePrimaryDeathRecord
    (CleanMappingExtTables,),  # should be one of the last cleaning rules run
]

CONTROLLED_TIER_FITBIT_CLEANING_CLASSES = [
    (DeidFitbitDeviceId,
    ),  # This rule must occur so that PID can map to device_id
    (FitbitPIDtoRID,),
    (FitbitDeidSrcID,),
    (RemoveNonExistingPids,),  # assumes CT dataset is ready for reference
]

DATA_CONSISTENCY_CLEANING_CLASSES = [
    (DropOrphanedSurveyConductIds,),
    (DropOrphanedPIDS,),
    (CleanMappingExtTables,),  # should be one of the last cleaning rules run
]

CRON_RETRACTION_CLEANING_CLASSES = [
    (CleanMappingExtTables,),  # should be one of the last cleaning rules run
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
    DataStage.FITBIT.value:
        FITBIT_CLEANING_CLASSES,
    DataStage.REGISTERED_TIER_DEID.value:
        REGISTERED_TIER_DEID_CLEANING_CLASSES,
    DataStage.REGISTERED_TIER_DEID_BASE.value:
        REGISTERED_TIER_DEID_BASE_CLEANING_CLASSES,
    DataStage.REGISTERED_TIER_DEID_CLEAN.value:
        REGISTERED_TIER_DEID_CLEAN_CLEANING_CLASSES,
    DataStage.REGISTERED_TIER_FITBIT.value:
        REGISTERED_TIER_FITBIT_CLEANING_CLASSES,
    DataStage.CONTROLLED_TIER_DEID.value:
        CONTROLLED_TIER_DEID_CLEANING_CLASSES,
    DataStage.CONTROLLED_TIER_DEID_BASE.value:
        CONTROLLED_TIER_DEID_BASE_CLEANING_CLASSES,
    DataStage.CONTROLLED_TIER_DEID_CLEAN.value:
        CONTROLLED_TIER_DEID_CLEAN_CLEANING_CLASSES,
    DataStage.CONTROLLED_TIER_FITBIT.value:
        CONTROLLED_TIER_FITBIT_CLEANING_CLASSES,
    DataStage.DATA_CONSISTENCY.value:
        DATA_CONSISTENCY_CLEANING_CLASSES,
    DataStage.CRON_RETRACTION.value:
        CRON_RETRACTION_CLEANING_CLASSES
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
    engine_parser.add_argument(
        '--run_as',
        required=True,
        dest='run_as',
        action='store',
        help='Service account email address to impersonate')
    return engine_parser


PARSING_ERROR_MESSAGE_FORMAT = (
    'Error parsing %(arg)s. Please use "--key value" or "--key=value" to specify custom arguments. '
    'Custom arguments need an associated keyword to store their value.')


def _to_kwarg_key(arg) -> str:
    # TODO: Move this function to as project level arg_parser so it can be reused.
    if not arg.startswith('--'):
        raise RuntimeError(PARSING_ERROR_MESSAGE_FORMAT.format(arg=arg))
    key = arg[2:]
    if not key:
        raise RuntimeError(PARSING_ERROR_MESSAGE_FORMAT.format(arg=arg))
    return key


def _to_kwarg_val(val: str) -> str:
    # TODO: Move this function to as project level arg_parser so it can be reused.
    # likely invalid use of args- allowing single dash e.g. negative values
    if val.startswith('--'):
        raise RuntimeError(PARSING_ERROR_MESSAGE_FORMAT.format(arg=val))
    return val


def _get_kwargs(optional_args: typing.List[str]) -> typing.Dict:
    """
    This creates and ensures a {key: value} pair from _get_kwargs(...).
    """
    arg_list = []
    for arg in optional_args:
        arg_list.extend(arg.split("="))

    if len(arg_list) % 2:
        raise RuntimeError(PARSING_ERROR_MESSAGE_FORMAT.format(arg=arg_list))

    # _to_kwarg_key(...) and _to_kwargs_val(...) are validators
    return {
        _to_kwarg_key(arg): _to_kwarg_val(value)
        for arg, value in zip(arg_list[0::2], arg_list[1::2])
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

    # NOTE Retraction uses DATA_CONSISTENCY or CRON_RETRACTION data stage. For retraction,
    # all datasets share one sandbox dataset. Table_namer needs dataset_id so
    # the sandbox tables will not overwrite each other.
    if args.data_stage.value in [DATA_CONSISTENCY, CRON_RETRACTION]:
        table_namer = f"{args.data_stage.value}_{args.dataset_id}"
    else:
        table_namer = args.data_stage.value

    if args.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            project_id=args.project_id,
            dataset_id=args.dataset_id,
            sandbox_dataset_id=args.sandbox_dataset_id,
            rules=rules,
            table_namer=table_namer,
            **kwargs)
        for query in query_list:
            LOGGER.info(query)
    else:
        # Disable logging if running retraction cron
        if not constants.global_variables.DISABLE_SANDBOX:
            clean_engine.add_console_logging(args.console_log)
        clean_engine.clean_dataset(project_id=args.project_id,
                                   dataset_id=args.dataset_id,
                                   sandbox_dataset_id=args.sandbox_dataset_id,
                                   rules=rules,
                                   table_namer=table_namer,
                                   run_as=args.run_as,
                                   **kwargs)


if __name__ == '__main__':
    main()
