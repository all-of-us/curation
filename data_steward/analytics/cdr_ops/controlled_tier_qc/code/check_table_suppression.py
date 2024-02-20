import pandas as pd

from analytics.cdr_ops.controlled_tier_qc.ct_utils.helpers import run_check_by_row
from analytics.cdr_ops.controlled_tier_qc.sql.query_templates import QUERY_SUPPRESSED_TABLE


def check_table_suppression(check_df,
                            project_id,
                            post_dataset_id,
                            pre_deid_dataset=None,
                            mapping_dataset=None,
                            questionnaire_response_dataset=None):
    """Run table suppression check
    
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
    questionnaire_response_dataset
        ID of the dataset containing questionnaire_response deid mapping table

    Returns
    -------
    pd.DataFrame
    """
    table_check = run_check_by_row(check_df, QUERY_SUPPRESSED_TABLE, project_id,
                                   post_dataset_id)

    return table_check.reset_index(drop=True)
