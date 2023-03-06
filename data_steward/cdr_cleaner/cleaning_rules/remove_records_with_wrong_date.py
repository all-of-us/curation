"""
The Data Science team has identified erroneous dates (e.g. years of 206 and 9999) in the procedures and medications tables.
These dates are causing pandas to break, preventing queries from running in the Notebooks.

All date fields apart from DOB and death date, which have their own year-limiting cleaning rule, should have similar erroneous dates removed prior to de-id.

Original Issues: DC-489, DC-828
"""
# Python imports
import logging

# Project imports
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule, query_spec_list
from constants.cdr_cleaner.clean_cdr import COMBINED, QUERY, UNIONED
from common import CONDITION_OCCURRENCE, DEVICE_EXPOSURE, DRUG_EXPOSURE, JINJA_ENV, MEASUREMENT, OBSERVATION, \
    OBSERVATION_PERIOD, PROCEDURE_OCCURRENCE, SPECIMEN, VISIT_OCCURRENCE, VISIT_DETAIL
from resources import fields_for, validate_date_string
from utils import pipeline_logging

LOGGER = logging.getLogger(__name__)

JIRA_ISSUE_NUMBERS = ['DC489', 'DC828']

DOMAIN_TABLES = [
    CONDITION_OCCURRENCE, DEVICE_EXPOSURE, DRUG_EXPOSURE, MEASUREMENT,
    OBSERVATION, OBSERVATION_PERIOD, PROCEDURE_OCCURRENCE, SPECIMEN,
    VISIT_DETAIL, VISIT_OCCURRENCE
]

OBSERVATION_DEFAULT_YEAR_THRESHOLD = 1900
DEFAULT_YEAR_THRESHOLD = 1980

SANDBOX_TEMPLATE = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project_id}}.{{sandbox_dataset_id}}.{{sandbox_table_id}}` AS (
    SELECT * FROM `{{project_id}}.{{dataset_id}}.{{table_id}}`
    WHERE
    {% for col in cols if col['type'] in ['date', 'datetime', 'timestamp'] %}
        (EXTRACT(YEAR FROM {{col['name']}}) <= {{year_threshold}} OR DATE('{{cutoff_date}}') < CAST({{col['name']}} AS DATE))
        {% if not loop.last -%}
        OR
        {% endif %}
    {% endfor %}
)
""")

DELETE_TEMPLATE = JINJA_ENV.from_string("""
DELETE FROM `{{project_id}}.{{dataset_id}}.{{table_id}}`
WHERE
{% for col in cols if col['type'] in ['date', 'datetime', 'timestamp'] and col['mode'] == 'required' %}
    (EXTRACT(YEAR FROM {{col['name']}}) <= {{year_threshold}} OR DATE('{{cutoff_date}}') < CAST({{col['name']}} AS DATE))
    {% if not loop.last -%}
    OR
    {% endif %}
{% endfor %}
""")

UPDATE_TEMPLATE = JINJA_ENV.from_string("""
UPDATE `{{project_id}}.{{dataset_id}}.{{table_id}}`
SET 
{% for col in cols if col['type'] in ['date', 'datetime', 'timestamp'] and col['mode'] == 'nullable' %}
    {{col['name']}} = IF(EXTRACT(YEAR FROM {{col['name']}}) <= {{year_threshold}} OR DATE("{{cutoff_date}}") < CAST({{col['name']}} AS DATE), NULL, {{col['name']}})
    {% if not loop.last -%}, {% endif %}
{% endfor %}
WHERE
{% for col in cols if col['type'] in ['date', 'datetime', 'timestamp'] and col['mode'] == 'nullable' %}
    EXTRACT(YEAR FROM {{col['name']}}) <= {{year_threshold}} 
    OR DATE("{{cutoff_date}}") < CAST({{col['name']}} AS DATE)
    {% if not loop.last -%} OR {% endif %}
{% endfor %}
""")


class RemoveRecordsWithWrongDate(BaseCleaningRule):

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 cutoff_date,
                 year_threshold=DEFAULT_YEAR_THRESHOLD,
                 observation_year_threshold=OBSERVATION_DEFAULT_YEAR_THRESHOLD,
                 table_namer=None):
        """
        Initialize the class with proper information.

        Set the issue numbers, description and affected datasets. As other tickets may affect
        this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = ('Sandboxes and removes erroneous dates from records.')

        super().__init__(issue_numbers=JIRA_ISSUE_NUMBERS,
                         description=desc,
                         affected_datasets=[COMBINED, UNIONED],
                         affected_tables=DOMAIN_TABLES,
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         table_namer=table_namer)

        self.cutoff_date = cutoff_date
        self.year_threshold = year_threshold
        self.observation_year_threshold = observation_year_threshold

    def get_query_specs(self, *args, **keyword_args) -> query_spec_list:
        """
        Return a list of dictionary query specifications.

        :return:  A list of dictionaries. Each dictionary contains a single query
            and a specification for how to execute that query. The specifications
            are optional but the query is required.
        """
        sandbox_queries, delete_queries, update_queries = [], [], []

        for table in self.affected_tables:

            if table == OBSERVATION:
                threshold = self.observation_year_threshold
            else:
                threshold = self.year_threshold

            cols = fields_for(table)

            sandbox_queries.append({
                QUERY:
                    SANDBOX_TEMPLATE.render(
                        project_id=self.project_id,
                        dataset_id=self.dataset_id,
                        table_id=table,
                        sandbox_dataset_id=self.sandbox_dataset_id,
                        sandbox_table_id=self.sandbox_table_for(table),
                        cols=cols,
                        year_threshold=threshold,
                        cutoff_date=self.cutoff_date)
            })

            delete_queries.append({
                QUERY:
                    DELETE_TEMPLATE.render(project_id=self.project_id,
                                           dataset_id=self.dataset_id,
                                           table_id=table,
                                           cols=cols,
                                           year_threshold=threshold,
                                           cutoff_date=self.cutoff_date)
            })

            if not any(col['type'] in ['date', 'datetime', 'timestamp'] and
                       col['mode'] == 'nullable' for col in cols):
                LOGGER.info(
                    f'No update query for {table}. {table}\'s date/datetime columns are '
                    f'all required. Only delete query is generated for {table}.'
                )
                continue

            update_queries.append({
                QUERY:
                    UPDATE_TEMPLATE.render(project_id=self.project_id,
                                           dataset_id=self.dataset_id,
                                           table_id=table,
                                           cols=cols,
                                           year_threshold=threshold,
                                           cutoff_date=self.cutoff_date)
            })

        return sandbox_queries + delete_queries + update_queries

    def setup_rule(self, client, *args, **keyword_args):
        pass

    def setup_validation(self, client):
        """
        Run required steps for validation setup
        """
        raise NotImplementedError("Please fix me.")

    def validate_rule(self, client):
        """
        Validates the cleaning rule which deletes or updates the data from the tables
        """
        raise NotImplementedError("Please fix me.")

    def get_sandbox_tablenames(self):
        return [self.sandbox_table_for(table) for table in self.affected_tables]


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ext_parser = parser.get_argument_parser()

    ext_parser.add_argument('-c',
                            '--cutoff_date ',
                            dest='cutoff_date',
                            action='store',
                            help='EHR/RDR date cutoff of format YYYY-MM-DD',
                            type=validate_date_string,
                            required=True)

    ext_parser.add_argument(
        '-y',
        '--year_threshold',
        dest='year_threshold',
        action='store',
        help='The year threshold applied to domain tables except observation',
        required=False,
        default=DEFAULT_YEAR_THRESHOLD)

    ext_parser.add_argument('-o',
                            '--observation_year_threshold',
                            dest='observation_year_threshold',
                            action='store',
                            help='The threshold applied to observation',
                            required=False,
                            default=OBSERVATION_DEFAULT_YEAR_THRESHOLD)

    ARGS = ext_parser.parse_args()
    pipeline_logging.configure(level=logging.DEBUG, add_console_handler=True)

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id,
            ARGS.dataset_id,
            ARGS.sandbox_dataset_id, [(RemoveRecordsWithWrongDate,)],
            cutoff_date=ARGS.cutoff_date,
            year_threshold=ARGS.year_threshold,
            observation_year_threshold=ARGS.observation_year_threshold)
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(
            ARGS.project_id,
            ARGS.dataset_id,
            ARGS.sandbox_dataset_id, [(RemoveRecordsWithWrongDate,)],
            cutoff_date=ARGS.cutoff_date,
            year_threshold=ARGS.year_threshold,
            observation_year_threshold=ARGS.observation_year_threshold)
