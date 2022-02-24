"""
Interact with Google Cloud BigQuery
"""
# Python stl imports
import os

# Third-party imports
from google.cloud import bigquery
from google.cloud.bigquery import Client
from google.auth import default
from opentelemetry import trace
from opentelemetry.exporter.cloud_trace import CloudTraceSpanExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

# Project imports
from utils import auth
from resources import fields_for


class BigQueryClient(Client):
    """
    A client that extends GCBQ functionality
    See https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.client.Client.html#google.cloud.bigquery.client.Client
    """

    def __init__(self, project_id: str, scopes=None, credentials=None):
        """
        :param project_id: Identifies the project to create a cloud BigQuery client for
        :param scopes: List of Google scopes as strings
        :param credentials: Google credentials object (ignored if scopes is defined,
            uses delegated credentials instead)

        :return:  A BigQueryClient instance
        """
        tracer_provider = TracerProvider()
        cloud_trace_exporter = CloudTraceSpanExporter(project_id=project_id)
        tracer_provider.add_span_processor(
            BatchSpanProcessor(cloud_trace_exporter))
        trace.set_tracer_provider(tracer_provider)
        tracer = trace.get_tracer(__name__)
        with tracer.start_as_current_span(project_id):
            if scopes:
                credentials, project_id = default()
                credentials = auth.delegated_credentials(credentials,
                                                         scopes=scopes)
            super().__init__(project=project_id, credentials=credentials)

    def get_table_schema(self, table_name: str, fields=None) -> list:
        """
        A helper method to create big query SchemaFields for dictionary definitions.

        Given the table name, reads the schema from the schema definition file
        and returns a list of SchemaField objects that can be used for table
        creation.

        :param table_name:  the table name to get BigQuery SchemaField information
            for.
        :param fields: An optional argument to provide fields/schema as a list of JSON objects
        :returns:  a list of SchemaField objects representing the table's schema.
        """
        if fields:
            fields = fields
        else:
            fields = fields_for(table_name)

        schema = []
        for column in fields:
            name = column.get('name')
            field_type = column.get('type')
            column_def = bigquery.SchemaField(name,
                                              field_type).from_api_repr(column)

            schema.append(column_def)

        return schema

    def _to_standard_sql_type(self, field_type: str) -> str:
        """
        Get standard SQL type corresponding to a SchemaField type

        :param field_type: type in SchemaField object (can be legacy or standard SQL type)
        :return: standard SQL type name
        """
        upper_field_type = field_type.upper()
        standard_sql_type_code = bigquery.schema.LEGACY_TO_STANDARD_TYPES.get(
            upper_field_type)
        if not standard_sql_type_code:
            raise ValueError(f'{field_type} is not a valid field type')
        standard_sql_type = bigquery.StandardSqlDataTypes(
            standard_sql_type_code)
        return standard_sql_type.name

    def _to_sql_field(self,
                      field: bigquery.SchemaField) -> bigquery.SchemaField:
        """
        Convert all types in a schema field object to standard SQL types (not legacy)

        :param field: the schema field object
        :return: a converted schema field object
        """
        return bigquery.SchemaField(
            field.name, self._to_standard_sql_type(field.field_type),
            field.mode, field.description, field.fields)
