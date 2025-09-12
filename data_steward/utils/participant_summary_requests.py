"""
Utility to fetch and store deactivated participants information.

Original Issues: DC-1213, DC-1042, DC-971, DC-972, DC-1795

The intent of the get_deactivated_participants function is to call and store deactivated participants information by
    leveraging the RDR Participant Summary API. Deactivated participant information stored is `participantId`,
    `suspensionStatus`, and `suspensionTime`
The intent of the get_participant_information function is to retrieve the information needed for participant
    validation for a single site based on the site name (hpo_id). The information necessary for this request as defined
    in the ticket (DC-1213) as well as the Participant Summary Field List
    (https://all-of-us-raw-data-repository.readthedocs.io/en/latest/api_workflows/field_reference/participant_summary_field_list.html)
    are `participantId`, `firstName`, `middleName`, `lastName`, `streetAddress`, `streetAddress2`, `city`, `state`,
    `zipCode`, `phoneNumber`, `email`, `dateOfBirth`, `sex`
"""
# Python imports
import re
import logging
from typing import List, Dict

# Third party imports
import pandas
from google.cloud.bigquery.schema import SchemaField
from google.cloud.bigquery import (LoadJobConfig, TimePartitioning,
                                   TimePartitioningType)

# Project imports
from common import DIGITAL_HEALTH_SHARING_STATUS
from gcloud.bq import BigQueryClient

LOGGER = logging.getLogger(__name__)


def camel_to_snake_case(in_str: str):
    """
    Convert camel case to snake case

    :param in_str: camel case formatted string
    :return snake case formatted string
    """
    return '_'.join(re.split('(?=[A-Z])', in_str)).lower()


def process_digital_health_data_to_json(api_data: List[Dict],
                                        columns: List[str],
                                        column_map: Dict) -> List[Dict]:
    """
    Converts digital health data retrieved from PS API to curation convention formatted json objects

    :param api_data: digital_health_sharing_status data retrieved from PS API
    :param columns: columns of interest
    :param column_map: columns to be renamed as {old_name: new_name, ..}
    :return: list of json objects with supplied columns and renamed as per column_map
    """
    participant_records = []

    for full_participant_record in api_data:
        resource = full_participant_record.get('resource', {})
        # loop over fields that exist in both resource_dict and columns and save reformatted key-value pairs
        limited_participant_record = {
            camel_to_snake_case(col): resource[col]
            for col in resource.keys() & columns
        }

        # re-map for columns that are to be mapped
        for col in column_map & limited_participant_record.keys():
            limited_participant_record[
                column_map[col]] = limited_participant_record.pop(col)

        # convert participant id to integer
        limited_participant_record['person_id'] = participant_id_to_int(
            limited_participant_record.get('person_id'))

        # Extract all wearables data into a list of dicts
        wearables_data = limited_participant_record.pop(
            DIGITAL_HEALTH_SHARING_STATUS)

        # loop over all wearables data
        for wearable, latest_record in wearables_data.items():
            # Set type of wearable
            limited_participant_record['wearable'] = wearable

            # Update participant record with re-formatted keys and latest_record
            # This will also update the participant record to the next wearable if it exists
            limited_participant_record.update({
                camel_to_snake_case(col): val
                for col, val in latest_record.items()
            })

            # Extract all historical records for the current wearable
            historical_records = []
            for record in limited_participant_record.get('history', []):
                historical_records.append({
                    camel_to_snake_case(col): val
                    for col, val in record.items()
                })
            limited_participant_record['history'] = historical_records

            # Store new participant record for each wearable
            participant_records.append(limited_participant_record.copy())

    return participant_records


def participant_id_to_int(participant_id: str):
    """
    Transforms the participantId received from RDR ParticipantSummary API from an
    alphanumeric string to an integer string.

    :param participant_id: the RDR internal unique ID of a participant
    :return: returns the participantId as integer data type
    """

    return int(participant_id[1:])


def set_dataframe_date_fields(df: pandas.DataFrame,
                              schema: List[SchemaField]) -> pandas.DataFrame:
    """Convert dataframe fields from string to datetime for BQ schema fields
        with type in ['DATE', 'DATETIME', 'TIMESTAMP'].

    :param df: A dataframe
    :param schema: A list of schema fields
    :return: A modified dataframe with date fields converted to type datetime
    """
    df = df.copy()
    for schema_field in schema:
        field_name = schema_field.name
        if schema_field.field_type.upper() in (
                'DATE', 'DATETIME', 'TIMESTAMP') and field_name in df.columns:
            df[field_name] = pandas.to_datetime(df[field_name], errors='coerce')
            if schema_field.field_type.upper() == 'DATE':
                df[field_name] = df[field_name].dt.date

    return df


def store_participant_data(df: pandas.DataFrame,
                           client: BigQueryClient,
                           destination_table: str,
                           schema=None,
                           to_hour_partition=None,
                           append=False):
    """
    Stores the fetched participant data in a BigQuery dataset. If the
    table doesn't exist, it will create that table. If the table does
    exist, it will append the data onto that designated table.

    :param df: pandas dataframe created to hold participant data fetched from ParticipantSummary API
    :param client: a BigQueryClient
    :param destination_table: name of the table to be written in the form of dataset.tablename
    :param schema: a list of SchemaField objects corresponding to the destination table
    :param to_hour_partition: Boolean to indicate store to current hour partition or no partition
    :param append: append data to table

    :return: returns the bq job_id for the loading of participant data
    """
    # Parameter check
    if not client:
        raise RuntimeError(f'A bigquery client is needed to create the tables')

    if not schema:
        schema = client.get_table_schema(destination_table.split('.')[-1])

    # Dataframe data fields must be of type datetime
    df = set_dataframe_date_fields(df, schema)

    if to_hour_partition:
        # Clear partition for current hour to prevent duplication (overwrite existing data for the hour)
        LOGGER.info(f"Clearing current hour partition for {destination_table}")
        clear_partition_query = f"DELETE FROM {destination_table} WHERE _PARTITIONTIME = CURRENT_TIMESTAMP"
        clear_job = client.query(clear_partition_query)
        clear_job.result()
        load_job_config = LoadJobConfig(
            schema=schema,
            time_partitioning=TimePartitioning(type_=TimePartitioningType.HOUR))
    else:
        if append:
            write_disposition = 'WRITE_APPEND'
        else:
            write_disposition = 'WRITE_EMPTY'
        load_job_config = LoadJobConfig(schema=schema,
                                        write_disposition=write_disposition)

    # Run load job with specified config
    job = client.load_table_from_dataframe(df,
                                           destination_table,
                                           job_config=load_job_config)
    job.result()

    return job.job_id
