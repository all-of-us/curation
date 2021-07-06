import pandas as pd

from utils.helpers import run_check_by_row
from sql.query_templates import (QUERY_ID_NOT_OF_CORRECT_TYPE, QUERY_ID_NOT_CHANGED_BY_DEID, 
                                QUERY_ID_NOT_IN_MAPPING, QUERY_ID_NOT_MAPPED_PROPERLY, QUERY_SITE_ID_NOT_MAPPED_PROPERLY,
                                QUERY_ZIP_CODE_GENERALIZATION, QUERY_ZIP_CODE_TRANSFORMATION)


def check_mapping(check_df, project_id, post_dataset_id, pre_deid_dataset, mapping_dataset):
    """Run mapping verification rules
    
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
    # Correct type
    type_check = run_check_by_row(check_df, QUERY_ID_NOT_OF_CORRECT_TYPE,
        project_id, post_dataset_id, pre_deid_dataset, "ID datatype has been changed", mapping_dataset)
    
    # id changed by de-id
    id_map_check = run_check_by_row(check_df, QUERY_ID_NOT_CHANGED_BY_DEID,
        project_id, post_dataset_id, pre_deid_dataset, "Old ID still in the dataset", mapping_dataset)

    # new id not in mapping
    id_not_in_mapping_check = run_check_by_row(check_df, QUERY_ID_NOT_IN_MAPPING,
        project_id, post_dataset_id, pre_deid_dataset, "New ID not found in mapping table", mapping_dataset)

    # # new id properly mapped to old id
    id_properly_mapped_check = run_check_by_row(check_df, QUERY_ID_NOT_MAPPED_PROPERLY,
        project_id, post_dataset_id, pre_deid_dataset, "ID not properly mapped", mapping_dataset)
    
    return pd.concat([type_check, id_map_check, id_not_in_mapping_check, id_properly_mapped_check], sort=True)


def check_site_mapping(check_df, project_id, post_dataset_id, pre_deid_dataset, mapping_dataset):
    """Run mapping verification rules
    
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
    # Correct type
    type_check = run_check_by_row(check_df, QUERY_ID_NOT_OF_CORRECT_TYPE,
        project_id, post_dataset_id, pre_deid_dataset, "Site ID datatype has been changed", mapping_dataset)
    
    # new id not in mapping
    id_not_in_mapping_check = run_check_by_row(check_df, QUERY_ID_NOT_IN_MAPPING,
        project_id, post_dataset_id, pre_deid_dataset, "New Site ID not found in mapping table", mapping_dataset)

    # # new id properly mapped to old id
    id_properly_mapped_check = run_check_by_row(check_df, QUERY_SITE_ID_NOT_MAPPED_PROPERLY,
        project_id, post_dataset_id, pre_deid_dataset, "Site ID not properly mapped", mapping_dataset)
    
    return pd.concat([type_check, id_not_in_mapping_check, id_properly_mapped_check], sort=True)



def check_mapping_zipcode_generalization(check_df, project_id, post_dataset_id, pre_deid_dataset, mapping_dataset):
    """Run zipcode generalization check
    
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
    zip_check = run_check_by_row(check_df, QUERY_ZIP_CODE_GENERALIZATION,
        project_id, post_dataset_id, pre_deid_dataset, "Zip code value generalized")
    
    return zip_check

def check_mapping_zipcode_transformation(check_df, project_id, post_dataset_id, pre_deid_dataset, mapping_dataset):
    zip_transformation = run_check_by_row(check_df, QUERY_ZIP_CODE_TRANSFORMATION,
        project_id, post_dataset_id, pre_deid_dataset, "Zip code value transformed")

    return zip_transformation