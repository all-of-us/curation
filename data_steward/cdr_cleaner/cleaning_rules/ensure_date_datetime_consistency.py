"""
Ensuring there are no null datetimes.

Original Issues: DC-614, DC-509, and DC-432

The intent is to copy the date over to the datetime field if the datetime
field is null.
"""

# Python imports
import logging

# Project imports
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from cdr_cleaner.cleaning_rules import field_mapping
import constants.bq_utils as bq_consts
import constants.cdr_cleaner.clean_cdr as cdr_consts
import common

# Third party imports
from googleapiclient.errors import HttpError
from oauth2client.client import HttpAccessTokenRefreshError

LOGGER = logging.getLogger(__name__)

TABLE_DATES = {
    common.CONDITION_OCCURRENCE: {
        'condition_start_datetime': 'condition_start_date',
        'condition_end_datetime': 'condition_end_date'
    },
    common.DRUG_EXPOSURE: {
        'drug_exposure_start_datetime': 'drug_exposure_start_date',
        'drug_exposure_end_datetime': 'drug_exposure_end_date'
    },
    common.DEVICE_EXPOSURE: {
        'device_exposure_start_datetime': 'device_exposure_start_date',
        'device_exposure_end_datetime': 'device_exposure_end_date'
    },
    common.MEASUREMENT: {
        'measurement_datetime': 'measurement_date'
    },
    common.OBSERVATION: {
        'observation_datetime': 'observation_date'
    },
    common.PROCEDURE_OCCURRENCE: {
        'procedure_datetime': 'procedure_date'
    },
    common.DEATH: {
        'death_datetime': 'death_date'
    },
    common.SPECIMEN: {
        'specimen_datetime': 'specimen_date'
    },
    common.OBSERVATION_PERIOD: {
        'observation_period_start_datetime': 'observation_period_start_date',
        'observation_period_end_datetime': 'observation_period_end_date'
    },
    common.VISIT_OCCURRENCE: {
        'visit_start_datetime': 'visit_start_date',
        'visit_end_datetime': 'visit_end_date'
    }
}

FIX_DATETIME_QUERY = """
SELECT {cols}
FROM `{project_id}.{dataset_id}.{table_id}`
"""

FIX_NULL_DATETIME_IN_GET_COLS_QUERY = """
CASE
WHEN {field} IS NULL
THEN CAST(DATETIME({date_field}, MAKETIME(00,00,00)) AS TIMESTAMP
WHEN EXTRACT(DATE FROM {field}) = {date_field}
THEN {field}
ELSE CAST(DATETIME({date_field}, EXTRACT(TIME FROM {field})) AS TIMESTAMP
END AS {field}
"""

class EnsureDateDatetimeConsistency(BaseCleaningRule):
    """
    Ensure no nulls and consistency in the datetime and date fields
    """

    def __init__(self, project_id, dataset_id, sandbox_dataset_id):
        """
        Initialize the class with proper information.

        Set the issue numbers, description and affected datasets. As other tickets may affect this SQL,
        append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = (
            'Ensure date and datetime consistency where nulls exist in nullable fields'
            'by setting date in datetime to the date in date field and time as midnight (00,00,00)'
        )
        super().__init__(issue_numbers=['DC-614', 'DC-509', 'DC-432'],
                         description=desc,
                         affected_datasets=[cdr_consts.RDR, cdr_consts.UNIONED, cdr_consts.COMBINED],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id)

    def get_cols(self):
        """
        Generates the fields to choose along with case statements to generate datetime
        And ensure no null datetime values

        :param self: table for which the fields are pulled
        :return: cols
        """
        table_fields = field_mapping.get_domain_fields(self)
        col_exprs = []
        for field in table_fields:
            if field in TABLE_DATES[self]:
                col_expr = FIX_NULL_DATETIME_IN_GET_COLS_QUERY.format(
                            field=field, date_field=TABLE_DATES[self][field])
            else:
                col_expr = field
            col_exprs.append(col_expr)
        cols = ', '.join(col_exprs)
        return cols

    def get_query_specs(self):
        """
        This function generates a list of query dicts for ensuring the dates and datetimes are consistent

        :param project_id: the project_id in which the query is run
        :param dataset_id: the dataset_id in which the query is run
        :return: a list of query dicts for ensuring the dates and datetimes are consistent
        """
        queries = []
        for table in TABLE_DATES:
            query = dict()
            query[cdr_consts.QUERY] = FIX_DATETIME_QUERY.format(
                project=self.get_project_id(),
                dataset=self.get_dataset_id(),
                table_id=table,
                cols=self.get_cols())
            query[cdr_consts.DESTINATION_TABLE] = table
            query[cdr_consts.DISPOSITION] = bq_consts.WRITE_TRUNCATE
            query[cdr_consts.DESTINATION_DATASET] = self.get_dataset_id()
            queries.append(query)
        return queries

    def setup_rule(self):
        """
        Function to run any data upload options before executing a query.
        """
        pass

    def log_queries(self):
        """
        Helper function to print the SQL a class generates.

        If the inheriting class builds table inside the get_query_specs function,
        the inheriting class will need to override this function.
        """
        try:
            query_list = self.get_query_specs()
        except (KeyError, HttpAccessTokenRefreshError, HttpError) as err:
            LOGGER.exception("cannot list queries for %s",
                             self.__class__.__name__)
            raise

        for query in query_list:
            LOGGER.info('Generated SQL Query:\n%s',
                        query.get(cdr_consts.QUERY, 'NO QUERY FOUND'))


# def get_cols(table_id):
#     """
#     Generates the fields to choose along with case statements to generate datetime
#
#     :param table_id: table for which the fields
#     :return:
#     """
#     table_fields = field_mapping.get_domain_fields(table_id)
#     col_exprs = []
#     for field in table_fields:
#         if field in TABLE_DATES[table_id]:
#             if field_mapping.is_field_required(table_id, field):
#                 col_expr = (
#                     ' CASE'
#                     ' WHEN EXTRACT(DATE FROM {field}) = {date_field}'
#                     ' THEN {field}'
#                     ' ELSE CAST(DATETIME({date_field}, EXTRACT(TIME FROM {field})) AS TIMESTAMP)'
#                     ' END AS {field}').format(
#                         field=field, date_field=TABLE_DATES[table_id][field])
#             else:
#                 col_expr = (' CASE'
#                             ' WHEN EXTRACT(DATE FROM {field}) = {date_field}'
#                             ' THEN {field}'
#                             ' ELSE NULL'
#                             ' END AS {field}').format(
#                                 field=field,
#                                 date_field=TABLE_DATES[table_id][field])
#         else:
#             col_expr = field
#         col_exprs.append(col_expr)
#     cols = ', '.join(col_exprs)
#     return cols

# def get_fix_incorrect_datetime_to_date_queries(project_id, dataset_id):
#     """
#     This function generates a list of query dicts for ensuring the dates and datetimes are consistent
#
#     :param project_id: the project_id in which the query is run
#     :param dataset_id: the dataset_id in which the query is run
#     :return: a list of query dicts for ensuring the dates and datetimes are consistent
#     """
#     queries = []
#     for table in TABLE_DATES:
#         query = dict()
#         query[cdr_consts.QUERY] = FIX_DATETIME_QUERY.format(
#             project_id=project_id,
#             dataset_id=dataset_id,
#             table_id=table,
#             cols=get_cols(table))
#         query[cdr_consts.DESTINATION_TABLE] = table
#         query[cdr_consts.DISPOSITION] = bq_consts.WRITE_TRUNCATE
#         query[cdr_consts.DESTINATION_DATASET] = dataset_id
#         queries.append(query)
#     return queries




if __name__ == '__main__':
    import cdr_cleaner.clean_cdr_engine as clean_engine
    import cdr_cleaner.args_parser as parser

    ARGS = parser.parse_args()

    clean_engine.add_console_logging(ARGS.console_log)
    query_list = EnsureDateDatetimeConsistency(ARGS.project_id, ARGS.dataset_id, ARGS.sandbox_dataset_id)
    # query_list = get_fix_incorrect_datetime_to_date_queries(
    #     ARGS.project_id, ARGS.dataset_id)
    clean_engine.clean_dataset(ARGS.project_id, query_list)
