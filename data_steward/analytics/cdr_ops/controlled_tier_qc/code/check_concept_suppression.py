import pandas as pd

from analytics.cdr_ops.controlled_tier_qc.utils.helpers import run_check_by_row
from analytics.cdr_ops.controlled_tier_qc.sql.query_templates import (
    QUERY_SUPPRESSED_CONCEPT)


def check_concept_suppression(check_df,
                              project_id,
                              post_dataset_id,
                              pre_deid_dataset=None,
                              mapping_dataset=None) -> pd.DataFrame:
    """Run quality check to see if the specified concept is suppressed as expected.
    :param check_df: dataframe that has the data from either 
                     CONCEPT_CSV_FILE, FIELD_CSV_FILE, TABLE_CSV_FILE, or MAPPING_CSV_FILE
    :param project_id: Project ID of the dataset.
    :param post_deid_dataset: ID of the dataset after DEID.
    :param pre_deid_dataset: ID of the dataset before DEID.
    :param mapping_dataset: ID of the dataset for mapping.
    :returns: dataframe that has the results of this check.
    """
    concept_check = run_check_by_row(check_df, QUERY_SUPPRESSED_CONCEPT,
                                     project_id, post_dataset_id)

    return concept_check.reset_index(drop=True)
