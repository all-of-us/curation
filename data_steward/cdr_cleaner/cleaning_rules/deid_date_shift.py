"""
The basic date shifting rule..

Original Issue:  DC-1005
"""
# Python Imports
import logging

# Third party imports
from jinja2 import Environment

# Project imports
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from constants.cdr_cleaner import clean_cdr as cdr_consts

LOGGER = logging.getLogger(__name__)

ISSUE_NUMBERS = ['DC-1005']

TABLES_AND_SCHEMAS = {
    'activity_summary': [{
        "mode": "NULLABLE",
        "name": "date",
        "type": "DATE"
    }, {
        "mode": "NULLABLE",
        "name": "activity_calories",
        "type": "FLOAT"
    }, {
        "mode": "NULLABLE",
        "name": "calories_bmr",
        "type": "FLOAT"
    }, {
        "mode": "NULLABLE",
        "name": "calories_out",
        "type": "FLOAT"
    }, {
        "mode": "NULLABLE",
        "name": "elevation",
        "type": "FLOAT"
    }, {
        "mode": "NULLABLE",
        "name": "fairly_active_minutes",
        "type": "FLOAT"
    }, {
        "mode": "NULLABLE",
        "name": "floors",
        "type": "INTEGER"
    }, {
        "mode": "NULLABLE",
        "name": "lightly_active_minutes",
        "type": "FLOAT"
    }, {
        "mode": "NULLABLE",
        "name": "marginal_calories",
        "type": "FLOAT"
    }, {
        "mode": "NULLABLE",
        "name": "sedentary_minutes",
        "type": "FLOAT"
    }, {
        "mode": "NULLABLE",
        "name": "steps",
        "type": "INTEGER"
    }, {
        "mode": "NULLABLE",
        "name": "very_active_minutes",
        "type": "FLOAT"
    }, {
        "mode": "NULLABLE",
        "name": "person_id",
        "type": "INTEGER"
    }],
    'heart_rate_minute_level': [{
        "mode": "NULLABLE",
        "name": "datetime",
        "type": "DATETIME"
    }, {
        "mode": "NULLABLE",
        "name": "heart_rate_value",
        "type": "INTEGER"
    }, {
        "mode": "NULLABLE",
        "name": "person_id",
        "type": "INTEGER"
    }],
    'heart_rate_summary': [{
        "mode": "NULLABLE",
        "name": "person_id",
        "type": "INTEGER"
    }, {
        "mode": "NULLABLE",
        "name": "date",
        "type": "DATE"
    }, {
        "mode": "NULLABLE",
        "name": "zone_name",
        "type": "STRING"
    }, {
        "mode": "NULLABLE",
        "name": "min_heart_rate",
        "type": "INTEGER"
    }, {
        "mode": "NULLABLE",
        "name": "max_heart_rate",
        "type": "INTEGER"
    }, {
        "mode": "NULLABLE",
        "name": "minute_in_zone",
        "type": "INTEGER"
    }, {
        "mode": "NULLABLE",
        "name": "calorie_count",
        "type": "FLOAT"
    }],
    'steps_intraday': [{
        "mode": "NULLABLE",
        "name": "datetime",
        "type": "DATETIME"
    }, {
        "mode": "NULLABLE",
        "name": "steps",
        "type": "NUMERIC"
    }, {
        "mode": "NULLABLE",
        "name": "person_id",
        "type": "INTEGER"
    }]
}

jinja_env = Environment(
    # help protect against cross-site scripting vulnerabilities
    autoescape=True,
    # block tags on their own lines
    # will not cause extra white space
    trim_blocks=True,
    lstrip_blocks=True,
    # syntax highlighting should be better
    # with these comment delimiters
    comment_start_string='--',
    comment_end_string=' --')

DATE_FIELD_SHIFT = jinja_env.from_string("""
DATE_SUB( CAST({{field}} AS DATE), INTERVAL (
    SELECT
      shift
    FROM
    -- could be _deid_map or pid_rid_mapping --
      `{{project}}.{{map_dataset}}.{{map_table}}` AS map
    WHERE
      map.research_id = {{table}}.person_id) DAY) AS {{field}}
""")

DATETIME_FIELD_SHIFT = jinja_env.from_string("""
  TIMESTAMP_SUB( CAST({{field}} AS TIMESTAMP), INTERVAL (
    SELECT
      shift
    FROM
    -- could be _deid_map or pid_rid_mapping --
      `{{project}}.{{map_dataset}}.{{map_table}}` AS map
    WHERE
      map.research_id = {table}.person_id) DAY) AS {{field}}
""")

SELECT_STATEMENT = jinja_env.from_string("""
CREATE OR REPLACE `{{project}}.{{dataset}}.{{table}}` AS (
SELECT 
{{fields}}
FROM `{{project}}.{{dataset}}.{{table}}`) 
""")


class DateShiftFitbitFieldsRule(BaseCleaningRule):
    """
    Suppress rows by values in the observation_source_concept_id field.
    """

    def __init__(self, project_id, dataset_id, sandbox_dataset_id):
        """
        Initialize the class.

        Set the issue numbers, description and affected datasets.  As other
        tickets may affect this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = (f'Date shift date and timestamp fields by the date shift '
                f'calculated in the static mapping table.')
        super().__init__(issue_numbers=ISSUE_NUMBERS,
                         description=desc,
                         affected_datasets=[cdr_consts.RDR],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         affected_tables=["all"])

    def get_query_specs(self):
        """
        Return a list of dictionary query specifications.

        :return:  A list of dictionaries.  Each dictionary contains a
            single query and a specification for how to execute that query.
            The specifications are optional but the query is required.
        """
        date_shift_queries = []
        for table, schema in TABLES_AND_SCHEMAS.items():
            fields = []
            for field in schema:
                field_type = field.get('type').lower()
                field_name = field.get('name')
                if field_type == 'date':
                    shift_string = DATE_FIELD_SHIFT.render(
                        project=self.project_id,
                        map_dataset='pipeline_logging',
                        map_table='pid_rid_mapping',
                        field=field_name,
                        table=table)
                    fields.append(shift_string)
                elif field_type == 'timestamp':
                    shift_string = DATETIME_FIELD_SHIFT.render(
                        project=self.project_id,
                        map_dataset='pipeline_logging',
                        map_table='pid_rid_mapping',
                        field=field_name,
                        table=table)
                    fields.append(shift_string)
                else:
                    fields.append(field_name)

            fields_string = ', '.join(fields)

            query = SELECT_STATEMENT.render(project=self.project_id,
                                            dataset=self.dataset_id,
                                            table=table,
                                            fields=fields_string)

            date_shift_queries.append(query)

        return date_shift_queries

    def setup_rule(self, client):
        """
        Function to run any data upload options before executing a query.
        """
        pass

    def setup_validation(self, client):
        """
        Run required steps for validation setup

        This abstract method was added to the base class after this rule was authored.
        This rule needs to implement logic to setup validation on cleaning rules that
        will be updating or deleting the values.
        Until done no issue exists for this yet.
        """
        raise NotImplementedError("Please fix me.")

    def validate_rule(self):
        """
        Validates the cleaning rule which deletes or updates the data from the tables

        This abstract method was added to the base class after this rule was authored.
        This rule needs to implement logic to run validation on cleaning rules that will
        be updating or deleting the values.
        Until done no issue exists for this yet.
        """
        raise NotImplementedError("Please fix me.")

    def get_sandbox_tablenames(self):
        return []


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()
    clean_engine.add_console_logging(ARGS.console_log)
    date_shifter = DateShiftFitbitFieldsRule(ARGS.project_id, ARGS.dataset_id,
                                             ARGS.sandbox_dataset_id)
    query_list = date_shifter.get_query_specs()

    print(query_list)
    #if ARGS.list_queries:
    #    rdr_cleaner.log_queries()
    #else:
    #clean_engine.clean_dataset(ARGS.project_id, query_list)
