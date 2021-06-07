"""
A utility to add concept_ids that need to be suppressed in DEID, to the _concept_ids_suppression lookup table

If collecting concept_ids by query, add query as a global variable below and add the variable to the queries list in
get_concepts_query()

If collecting concept_ids by csv file, upload file to
`data_steward/deid/config/internal_tables/concept_ids_suppression_files`

If additional columns need to be added to the lookup table, append to the columns list in the function
create_concept_id_lookup_table() in aou.py

Ensure the columns in the query/csv file match the current table schema, there is cleanup as a double precaution in
get_concepts_via_csv()
"""
# Python Imports
import logging
import os

# Third Party Imports
import pandas as pd

# Project Imports
from resources import DEID_PATH
from common import JINJA_ENV

LOGGER = logging.getLogger(__name__)
LOGS_PATH = '../logs'


def get_all_concept_ids(columns, input_dataset, client):
    """
    function to collect concept_ids from both csv files and queries. combines the data and filters out to match
    columns passed and defined in create_concept_id_lookup_table() from aou.py to create the _concept_ids_suppression
    lookup table

    :param columns: columns passed from create_concept_id_lookup_table() from aou.py
    :param input_dataset: input dataset queries will reference and  the lookup table will be saved to
    :param client: bq client
    :return: combined, clean dataframe with concept_ids from csv files and queries
    """
    # get concept_ids from csv files
    csv_data = get_concepts_via_csv()
    # get and append concept_ids from queries
    query_data = get_concepts_via_query(input_dataset, client)

    all_concept_ids = pd.concat([csv_data, query_data])

    # only keep columns that match columns given as argument
    cols = [col for col in all_concept_ids.columns if col in columns]
    all_concept_ids = all_concept_ids[cols]

    return all_concept_ids


def check_concept_id_field(df):
    """
    helper function to check that 'concept_id' is included in the dataframe schema

    :param df: dataframe to check
    :return: true or false
    """

    df_columns = df.columns
    if 'concept_id' in df_columns:
        return True
    else:
        return False


def get_concepts_via_csv():
    """
    function to collect and process files in `data_steward/deid/config/internal_tables/concept_ids_suppression_files`
    to populate the _concept_ids_suppression lookup table

    :return: dataframe of concept_ids and information from all csv files located in specified folder
    """

    folder_path = os.path.join(DEID_PATH, 'config', 'internal_tables',
                               'concept_ids_suppression_files')
    final_csv_data_df = pd.DataFrame()

    LOGGER.info(
        "Processing files in folder to append data to _concept_ids_suppression table"
    )
    # Loop through all file in directory
    for file in os.listdir(folder_path):
        file_data_df = pd.read_csv(os.path.join(folder_path, file))
        # Clean up column headers
        file_data_df.columns = file_data_df.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(', '')\
            .str.replace(')', '')

        if check_concept_id_field(file_data_df):
            final_csv_data_df = final_csv_data_df.append(file_data_df)
        else:
            LOGGER.info(
                f"ERROR: file: {file} does not contain concept_id in schema, will not process."
            )
            continue

    LOGGER.info(
        f"Adding {len(final_csv_data_df.index)} rows from csv files, to dataframe to create _concept_ids_suppression lookup table"
    )
    return final_csv_data_df


def get_concepts_via_query(input_dataset, client):
    """
    function to collect and run queries to populate the _concept_ids_suppression lookup table

    :param input_dataset: input dataset where lookup table will live
    :param client: BQ client
    :return: dataframe of results from query
    """

    # Add queries from above to queries list, to append return values to _concept_ids_suppression lookup table
    queries = []

    final_query_data_df = pd.DataFrame()

    LOGGER.info(
        "Running queries to append data to _concept_ids_suppression table")
    for q in queries:
        query_data_df = client.query(
            JINJA_ENV.from_string(q).render(
                input_dataset=input_dataset)).to_dataframe()
        # verify csv file contains 'concept_id' column
        if check_concept_id_field(query_data_df):
            final_query_data_df = final_query_data_df.append(query_data_df)
        else:
            LOGGER.info(
                f"Query: {q} does not contain concept_id in schema, will not process."
            )
            continue

    LOGGER.info(
        f"Adding {len(final_query_data_df.index)} rows from queries, to dataframe to create _concept_ids_suppression "
        f"lookup table")
    return final_query_data_df
