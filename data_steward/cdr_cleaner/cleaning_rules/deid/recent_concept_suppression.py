"""
Suppress Non-PPI Concepts created within past year

Original Issues: DC-1692, DC-2789
"""

# Python imports
import logging
from datetime import datetime

# Project imports
from cdr_cleaner.cleaning_rules.deid.concept_suppression import AbstractBqLookupTableConceptSuppression
from constants.cdr_cleaner import clean_cdr as cdr_consts
from common import AOU_DEATH, JINJA_ENV, CDM_TABLES, DEFAULT_CONCEPT_VALID_START_DATE
from utils.bq import validate_bq_date_string
from utils import pipeline_logging
from resources import get_concept_id_fields, get_primary_key, get_primary_date_field
from gcloud.bq import BigQueryClient

# Third party imports
from google.cloud.exceptions import GoogleCloudError
from google.cloud import bigquery

LOGGER = logging.getLogger(__name__)

SUPPRESSION_RULE_CONCEPT_TABLE = 'recent_concepts'
CONCEPT_FIRST_USE_TABLE = 'concept_first_use'

RETRIEVE_CONCEPT_FIRST_USES_QUERY = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project_id}}.{{sandbox_id}}.{{concept_first_use_table}}` AS
    SELECT
        c.concept_id,
        CASE 
            WHEN c.valid_start_date = '{{ default_valid_start_date }}' AND min_uses.min_use_date IS NOT NULL
                THEN min_uses.min_use_date
            ELSE
                c.valid_start_date
        END valid_start_date
    FROM (
        SELECT
            concept_id, MIN(min_use_date) min_use_date
        FROM (
            {% for table_info in table_infos  %}
            -- Get min date of unpivoted concept id fields --
            (
                WITH concept_id_fields AS (
                SELECT
                    {{ table_info['concept_id_fields'] | join(', ') }},
                    {{ table_info['primary_datefield'] }}
                FROM `{{project_id}}.{{dataset_id}}.{{table_info['table_name']}}` c
                ),
                unpivoted_concept_id_fields AS (
                SELECT
                    *
                FROM concept_id_fields
                UNPIVOT(concept_id FOR concept_id_field IN ({{ table_info['concept_id_fields'] | join(', ') }}))
                )
                SELECT
                concept_id, MIN({{ table_info['primary_datefield'] }}) min_use_date
                FROM unpivoted_concept_id_fields
                GROUP BY concept_id
            )
            {% if not loop.last %}
            UNION ALL
            {% endif %}
            {% endfor %}
        ) combined
        GROUP BY concept_id    
    ) min_uses
    RIGHT JOIN `{{project_id}}.{{dataset_id}}.concept` c
        ON c.concept_id = min_uses.concept_id

""")

RECENT_CONCEPT_QUERY = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project_id}}.{{sandbox_id}}.{{concept_suppression_lookup_table}}` AS
SELECT c.*
FROM `{{project_id}}.{{dataset_id}}.concept` c
JOIN `{{project_id}}.{{sandbox_id}}.{{concept_first_use_table}}` first_use
    ON first_use.concept_id = c.concept_id
WHERE
  first_use.valid_start_date >= DATE_SUB(DATE('{{cutoff_date}}'), INTERVAL 1 YEAR)
    AND vocabulary_id <> 'PPI'
""")


class RecentConceptSuppression(AbstractBqLookupTableConceptSuppression):

    def __init__(
        self,
        project_id,
        dataset_id,
        sandbox_dataset_id,
        cutoff_date=None,
        table_namer=None,
    ):
        """
        Initialize the class with proper information.

        Set the issue numbers, description and affected datasets. As other tickets may affect
        this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = "Suppress all concepts whose intial use was within 1 year"

        try:
            # set to provided date string if the date string is valid
            self.cutoff_date = validate_bq_date_string(cutoff_date)
        except (TypeError, ValueError):
            # otherwise, default to using today's date as the date string
            self.cutoff_date = str(datetime.now().date())

        super().__init__(
            issue_numbers=['DC1692', 'DC2789'],
            description=desc,
            affected_datasets=[cdr_consts.REGISTERED_TIER_DEID],
            affected_tables=CDM_TABLES + [AOU_DEATH],
            project_id=project_id,
            dataset_id=dataset_id,
            sandbox_dataset_id=sandbox_dataset_id,
            concept_suppression_lookup_table=SUPPRESSION_RULE_CONCEPT_TABLE,
            table_namer=table_namer)

    def create_suppression_lookup_table(self, client):
        self.retrieve_all_concept_first_uses(client)

        concept_suppression_lookup_query = RECENT_CONCEPT_QUERY.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            sandbox_id=self.sandbox_dataset_id,
            concept_suppression_lookup_table=self.
            concept_suppression_lookup_table,
            cutoff_date=self.cutoff_date,
            concept_first_use_table=CONCEPT_FIRST_USE_TABLE)
        query_job = client.query(concept_suppression_lookup_query)
        result = query_job.result()

        if hasattr(result, 'errors') and result.errors:
            LOGGER.error(f"Error running job {result.job_id}: {result.errors}")
            raise GoogleCloudError(
                f"Error running job {result.job_id}: {result.errors}")

    def retrieve_all_concept_first_uses(self, client):

        table_infos = []
        dataset_ref = bigquery.DatasetReference(self.project_id,
                                                self.dataset_id)
        tables_in_dataset = BigQueryClient.list_tables(client, dataset_ref)
        tables_in_dataset = [table.table_id for table in tables_in_dataset]

        for table_name in self.affected_tables:
            primary_key = get_primary_key(table_name)
            concept_id_fields = get_concept_id_fields(table_name)
            primary_date_field = get_primary_date_field(table_name)

            if concept_id_fields and primary_date_field and table_name in tables_in_dataset:
                table_infos.append({
                    'table_name': table_name,
                    'primary_key': primary_key,
                    'primary_datefield': primary_date_field,
                    'concept_id_fields': concept_id_fields
                })

        retrieve_all_concepts_first_uses = RETRIEVE_CONCEPT_FIRST_USES_QUERY.render(
            table_infos=table_infos,
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            sandbox_id=self.sandbox_dataset_id,
            concept_first_use_table=CONCEPT_FIRST_USE_TABLE,
            default_valid_start_date=DEFAULT_CONCEPT_VALID_START_DATE)

        query_job = client.query(retrieve_all_concepts_first_uses)
        result = query_job.result()

        if hasattr(result, 'errors') and result.errors:
            LOGGER.error(f"Error running job {result.job_id}: {result.errors}")
            raise GoogleCloudError(
                f"Error running job {result.job_id}: {result.errors}")

    def validate_rule(self, client, *args, **keyword_args):
        """
        Validates the cleaning rule which deletes or updates the data from the tables

        Method to run validation on cleaning rules that will be updating the values.
        For example:
        if your class updates all the datetime fields you should be implementing the
        validation that checks if the date time values that needs to be updated no
        longer exists in the table.

        if your class deletes a subset of rows in the tables you should be implementing
        the validation that checks if the count of final final row counts + deleted rows
        should equals to initial row counts of the affected tables.

        Raises RunTimeError if the validation fails.
        """

        raise NotImplementedError("Please fix me.")

    def setup_validation(self, client, *args, **keyword_args):
        """
        Run required steps for validation setup

        Method to run to setup validation on cleaning rules that will be updating or deleting the values.
        For example:
        if your class updates all the datetime fields you should be implementing the
        logic to get the initial list of values which adhere to a condition we are looking for.

        if your class deletes a subset of rows in the tables you should be implementing
        the logic to get the row counts of the tables prior to applying cleaning rule

        """
        raise NotImplementedError("Please fix me.")

    def get_sandbox_tablenames(self):
        return super().get_sandbox_tablenames() + [
            SUPPRESSION_RULE_CONCEPT_TABLE, CONCEPT_FIRST_USE_TABLE
        ]


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ext_parser = parser.get_argument_parser()
    ext_parser.add_argument(
        '-c',
        '--cutoff_date',
        dest='cutoff_date',
        action='store',
        help=
        ('Cutoff date for data based on <table_name>_date and <table_name>_datetime fields.  '
         'Should be in the form YYYY-MM-DD.'),
        required=True,
        type=validate_bq_date_string,
    )

    ARGS = ext_parser.parse_args()
    pipeline_logging.configure(level=logging.DEBUG, add_console_handler=True)

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(ARGS.project_id,
                                                 ARGS.dataset_id,
                                                 ARGS.sandbox_dataset_id,
                                                 [(RecentConceptSuppression,)],
                                                 cutoff_date=ARGS.cutoff_date)

        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id,
                                   ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(RecentConceptSuppression,)],
                                   cutoff_date=ARGS.cutoff_date)
