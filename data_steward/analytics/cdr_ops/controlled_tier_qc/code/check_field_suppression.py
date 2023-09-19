import pandas as pd

from analytics.cdr_ops.controlled_tier_qc.utils.helpers import run_check_by_row
from analytics.cdr_ops.controlled_tier_qc.sql.query_templates import (
    QUERY_SUPPRESSED_NULLABLE_FIELD_NOT_NULL,
    QUERY_SUPPRESSED_REQUIRED_FIELD_NOT_EMPTY,
    QUERY_SUPPRESSED_NUMERIC_NOT_ZERO, QUERY_VEHICLE_ACCIDENT_SUPPRESSION_ICD9,
    QUERY_VEHICLE_ACCIDENT_SUPPRESSION_ICD10, QUERY_CANCER_CONCEPT_SUPPRESSION,
    QUERY_SUPPRESSED_FREE_TEXT_RESPONSE, QUERY_GEOLOCATION_SUPPRESSION)


def check_field_suppression(check_df,
                            project_id,
                            post_dataset_id,
                            pre_deid_dataset=None,
                            mapping_dataset=None,
                            questionnaire_response_dataset=None):
    """Run field suppression check

    Parameters
    ----------
    check_df: pd.DataFrame
        Dataframe containing the checks that need to be done
    project_id: str
        Google Bigquery project_id
    post_dataset_id: str
        Bigquery dataset after de-id rules were run
    pre_deid_dataset: str
        Bigquery dataset before de-id rules were run
    mapping_dataset: str
        *_deid sandbox dataset
    questionnaire_response_dataset: str
        ID of questionnaire response dataset

    Returns
    -------
    pd.DataFrame
    """
    nullable_field = check_df[check_df['is_nullable'] == 'YES']
    required_numeric_field = check_df[(check_df['is_nullable'] == 'NO') &
                                      (check_df['data_type'] == 'INT64')]
    required_other_field = check_df[(check_df['is_nullable'] == 'NO') &
                                    (check_df['data_type'] != 'INT64')]

    nullable_field_check = run_check_by_row(
        nullable_field, QUERY_SUPPRESSED_NULLABLE_FIELD_NOT_NULL, project_id,
        post_dataset_id)

    required_numeric_field_check = run_check_by_row(
        required_numeric_field, QUERY_SUPPRESSED_NUMERIC_NOT_ZERO, project_id,
        post_dataset_id)

    required_other_field_check = run_check_by_row(
        required_other_field, QUERY_SUPPRESSED_REQUIRED_FIELD_NOT_EMPTY,
        project_id, post_dataset_id)

    return pd.concat([
        nullable_field_check, required_numeric_field_check,
        required_other_field_check
    ],
                     sort=True)


def check_vehicle_accident_suppression(check_df,
                                       project_id,
                                       post_deid_dataset,
                                       pre_deid_dataset=None,
                                       mapping_dataset=None,
                                       questionnaire_response_dataset=None):
    """Run motor vehicle accident suppression check

    Parameters
    ----------
    check_df: pd.DataFrame
        Dataframe containing the checks that need to be done
    project_id: str
        Google Bigquery project_id
    post_deid_dataset: str
        Bigquery dataset after de-id rules were run
    pre_deid_dataset: str
        Bigquery dataset before de-id rules were run
    mapping_dataset: str
        *_deid sandbox dataset
    questionnaire_response_dataset: str
        ID of questionnaire response dataset

    Returns
    -------
    pd.DataFrame
    """
    icd9_vehicle_accident = run_check_by_row(
        check_df, QUERY_VEHICLE_ACCIDENT_SUPPRESSION_ICD9, project_id,
        post_deid_dataset)
    icd10_vehicle_accident = run_check_by_row(
        check_df, QUERY_VEHICLE_ACCIDENT_SUPPRESSION_ICD10, project_id,
        post_deid_dataset)
    return pd.concat([icd9_vehicle_accident, icd10_vehicle_accident], sort=True)


def check_field_cancer_concept_suppression(check_df,
                                           project_id,
                                           post_deid_dataset,
                                           pre_deid_dataset=None,
                                           mapping_dataset=None,
                                           questionnaire_response_dataset=None):
    """Run suppression check for some cancer concepts

    Parameters
    ----------
    check_df: pd.DataFrame
        Dataframe containing the checks that need to be done
    project_id: str
        Google Bigquery project_id
    post_deid_dataset: str
        Bigquery dataset after de-id rules were run
    pre_deid_dataset: str
        Bigquery dataset before de-id rules were run
    mapping_dataset: str
        *_deid sandbox dataset
    questionnaire_response_dataset: str
        ID of questionnaire response dataset

    Returns
    -------
    pd.DataFrame
    """
    cancer_concept = run_check_by_row(check_df,
                                      QUERY_CANCER_CONCEPT_SUPPRESSION,
                                      project_id, post_deid_dataset)
    return cancer_concept


def check_field_freetext_response_suppression(
        check_df,
        project_id,
        post_deid_dataset,
        pre_deid_dataset=None,
        mapping_dataset=None,
        questionnaire_response_dataset=None):
    free_text_concept = run_check_by_row(check_df,
                                         QUERY_SUPPRESSED_FREE_TEXT_RESPONSE,
                                         project_id, post_deid_dataset)
    return free_text_concept


def check_field_geolocation_records_suppression(
        check_df,
        project_id,
        post_deid_dataset,
        pre_deid_dataset=None,
        mapping_dataset=None,
        questionnaire_response_dataset=None):
    return run_check_by_row(check_df, QUERY_GEOLOCATION_SUPPRESSION, project_id,
                            post_deid_dataset)