import pandas as pd

from analytics.cdr_ops.controlled_tier_qc.utils.helpers import run_check_by_row
from analytics.cdr_ops.controlled_tier_qc.sql.query_templates import (
    QUERY_SUPPRESSED_CONCEPT)


def check_concept_suppression(check_df,
                              project_id,
                              post_dataset_id,
                              pre_deid_dataset=None,
                              mapping_dataset=None):
    """Run concept suppression check
    
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

    Returns
    -------
    pd.DataFrame
    """
    concept_check = run_check_by_row(check_df, QUERY_SUPPRESSED_CONCEPT,
                                     project_id, post_dataset_id)

    return concept_check.reset_index(drop=True)
