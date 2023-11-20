"""
Ensuring there are no null datetimes.

Original Issues: DC-614, DC-509, DC-432 and DC-2635

The intent is to copy the date over to the datetime field if the datetime
field is null or incorrect.
"""

# Python imports
import logging

# Project imports
import common
import constants.bq_utils as bq_consts
import constants.cdr_cleaner.clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules import field_mapping
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from resources import fields_for

LOGGER = logging.getLogger(__name__)

TABLE_DATES = {
    common.AOU_DEATH: {
        'death_datetime': 'death_date'
    },
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
    common.NOTE: {
        'note_datetime': 'note_date'
    },
    common.SPECIMEN: {
        'specimen_datetime': 'specimen_date'
    },
    common.SURVEY_CONDUCT: {
        'survey_start_datetime': 'survey_start_date',
        'survey_end_datetime': 'survey_end_date'
    },
    common.OBSERVATION_PERIOD: {
        'observation_period_start_datetime': 'observation_period_start_date',
        'observation_period_end_datetime': 'observation_period_end_date'
    },
    common.VISIT_OCCURRENCE: {
        'visit_start_datetime': 'visit_start_date',
        'visit_end_datetime': 'visit_end_date'
    },
    common.VISIT_DETAIL: {
        'visit_detail_start_datetime': 'visit_detail_start_date',
        'visit_detail_end_datetime': 'visit_detail_end_date'
    }
}

FIX_DATETIME_QUERY = """
SELECT {cols}
FROM `{project_id}.{dataset_id}.{table_id}`
"""

FIX_NULL_OR_INCORRECT_DATETIME_QUERY = common.JINJA_ENV.from_string("""
CASE
WHEN {{field}} IS NULL
{% if field.endswith('end_datetime') %}
THEN CAST(DATETIME({{date_field}}, TIME(11,59,59)) AS TIMESTAMP)
{% else %}
THEN CAST(DATETIME({{date_field}}, TIME(00,00,00)) AS TIMESTAMP)
{% endif %}
WHEN EXTRACT(DATE FROM {{field}}) = {{date_field}}
THEN {{field}}
ELSE CAST(DATETIME({{date_field}}, EXTRACT(TIME FROM {{field}})) AS TIMESTAMP)
END AS {{field}}
""")


class EnsureDateDatetimeConsistency(BaseCleaningRule):
    """
    Ensure no nulls and consistency in the datetime and date fields
    """

    # TODO Add depends_on TemporalConsistency

    def __init__(self, project_id, dataset_id, sandbox_dataset_id):
        """
        Initialize the class with proper information.

        Set the issue numbers, description and affected datasets. As other tickets may affect this SQL,
        append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = (
            'Ensures consistency when the datetime field is null or when the date and datetime fields are equal by '
            '(1) If the datetime field is null: Setting the datetime field\'s values to the date field\'s date and '
            'midnight (00:00:00); or '
            '(2) If the date and datetime fields are equal: Setting the datetime field\'s values to the date from the '
            'date field and the time from the datetime field.')
        super().__init__(
            issue_numbers=['DC-614', 'DC-509', 'DC-432', 'DC-2635'],
            description=desc,
            affected_datasets=[cdr_consts.UNIONED],
            project_id=project_id,
            dataset_id=dataset_id,
            sandbox_dataset_id=sandbox_dataset_id,
            affected_tables=[
                common.AOU_DEATH, common.CONDITION_OCCURRENCE,
                common.DRUG_EXPOSURE, common.DEVICE_EXPOSURE,
                common.MEASUREMENT, common.OBSERVATION,
                common.PROCEDURE_OCCURRENCE, common.SPECIMEN,
                common.SURVEY_CONDUCT, common.DEATH, common.NOTE,
                common.OBSERVATION_PERIOD, common.VISIT_OCCURRENCE,
                common.VISIT_DETAIL
            ])

    def get_cols(self, table):
        """
        Generates the fields to choose along with case statements to generate datetime
        And ensures no null datetime values

        :param self: instance of EnsureDateDatetimeConsistency class
        :param table: table for which the fields are pulled
        :return: cols
        """
        if table == common.AOU_DEATH:
            table_fields = [field['name'] for field in fields_for(table)]
        else:
            table_fields = field_mapping.get_domain_fields(table)

        col_exprs = []
        for field in table_fields:
            if field in TABLE_DATES[table]:
                col_expr = FIX_NULL_OR_INCORRECT_DATETIME_QUERY.render(
                    field=field, date_field=TABLE_DATES[table][field])
            else:
                col_expr = field
            col_exprs.append(col_expr)
        cols = ', '.join(col_exprs)
        return cols

    def get_query_specs(self):
        """
        This function generates a list of query dicts for ensuring the dates and datetimes are consistent

        :return: a list of query dicts for ensuring the dates and datetimes are consistent
        """
        queries = []
        for table in TABLE_DATES:
            query = dict()
            query[cdr_consts.QUERY] = FIX_DATETIME_QUERY.format(
                project_id=self.project_id,
                dataset_id=self.dataset_id,
                table_id=table,
                cols=self.get_cols(table))
            query[cdr_consts.DESTINATION_TABLE] = table
            query[cdr_consts.DISPOSITION] = bq_consts.WRITE_TRUNCATE
            query[cdr_consts.DESTINATION_DATASET] = self.dataset_id
            queries.append(query)
        return queries

    def setup_rule(self, client):
        """
        Function to run any data upload options before executing a query.
        """
        pass

    def get_sandbox_tablenames(self):
        """
        Returns an empty list because this rule does not use sandbox tables.
        """
        return []

    def setup_validation(self, client):
        """
        Run required steps for validation setup

        This abstract method was added to the base class after this rule was authored.
        This rule needs to implement logic to setup validation on cleaning rules that
        will be updating or deleting the values.
        Until done no issue exists for this yet.
        """
        raise NotImplementedError("Please fix me.")

    def validate_rule(self, client):
        """
        Validates the cleaning rule which deletes or updates the data from the tables

        This abstract method was added to the base class after this rule was authored.
        This rule needs to implement logic to run validation on cleaning rules that will
        be updating or deleting the values.
        Until done no issue exists for this yet.
        """
        raise NotImplementedError("Please fix me.")


if __name__ == '__main__':
    import cdr_cleaner.clean_cdr_engine as clean_engine
    import cdr_cleaner.args_parser as parser

    ARGS = parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id, ARGS.dataset_id, ARGS.sandbox_dataset_id,
            [(EnsureDateDatetimeConsistency,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(EnsureDateDatetimeConsistency,)])
