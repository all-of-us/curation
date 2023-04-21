"""
Sandbox and suppress the monkeypox concepts from registered tier for 1 year holdout from start of the epidemic (same rules as covid).
The start date for the monkeypox epidemic is 5/17/2022.

This cleaning rule runs in the registered tier after date-shifting.
Dates are unshifted using _deid_map table to decide if each record is within the holdout or not.

Original Issues: DC-2711
"""

# Python imports
import logging

# Project imports
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from constants.bq_utils import WRITE_EMPTY
from constants.cdr_cleaner import clean_cdr as cdr_consts
from common import AOU_DEATH, CATI_TABLES, JINJA_ENV
from resources import get_concept_id_fields, get_date_fields, get_datetime_fields, MONKEYPOX_CONCEPTS_PATH
from utils import pipeline_logging

# Third party imports

LOGGER = logging.getLogger(__name__)

SUPPRESSION_RULE_CONCEPT_TABLE = 'monkeypox_concepts'
SUPPRESSION_START_DATE_STR = '2022-05-17'
SUPPRESSION_END_DATE_STR = '2023-05-17'

SANDBOX_QUERY = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project_id}}.{{sandbox_dataset_id}}.{{sandbox_table_id}}` AS (
    SELECT t.* 
    FROM `{{project_id}}.{{dataset_id}}.{{table}}` t
    JOIN `{{project_id}}.{{mapping_dataset_id}}._deid_map` m
    ON t.person_id = m.person_id
    WHERE (
    {% for concept_field in concept_fields %}
        {{concept_field}} IN (
            SELECT concept_id 
            FROM `{{project_id}}.{{sandbox_dataset_id}}.{{suppression_concept_table}}`
        )
        {% if not loop.last -%} 
        OR 
        {% endif %}
    {% endfor %}
    )
    AND (
    {% if date_fields -%}
        {% for date_field in date_fields %}
        (
            {{date_field}} IS NOT NULL 
            AND DATE_ADD({{date_field}}, INTERVAL m.shift DAY) 
                BETWEEN DATE('{{suppression_start_date}}') 
                AND DATE('{{suppression_end_date}}')
        )
        {% if not loop.last -%} OR {% endif %}
        {% endfor %}
    {% endif %}
    {% if date_fields and datetime_fields -%} OR {% endif %}
    {% if datetime_fields -%}
        {% for datetime_field in datetime_fields %}
        (
            {{datetime_field}} IS NOT NULL 
            AND TIMESTAMP_ADD({{datetime_field}}, INTERVAL m.shift DAY)
                BETWEEN TIMESTAMP('{{suppression_start_date}} 00:00:00') 
                AND TIMESTAMP('{{suppression_end_date}} 23:59:59')
        )
        {% if not loop.last -%} OR {% endif %}
        {% endfor %}
    {% endif %}
    )
)
""")

DELETE_QUERY = JINJA_ENV.from_string("""
DELETE FROM `{{project_id}}.{{dataset_id}}.{{table}}`
{% if table == 'death' %}
WHERE person_id IN (SELECT person_id FROM `{{project_id}}.{{sandbox_dataset_id}}.{{sandbox_table_id}}`)
{% else %}
WHERE {{table}}_id IN (SELECT {{table}}_id FROM `{{project_id}}.{{sandbox_dataset_id}}.{{sandbox_table_id}}`)
{% endif %}
""")


class MonkeypoxConceptSuppression(BaseCleaningRule):

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 mapping_dataset_id,
                 table_namer=None):
        """
        Initialize the class with proper information.

        :param mapping_dataset_id: Dataset identifier. Identifies a dataset that
            contains _deid_map table.
        """
        desc = (
            'Sandbox and removes records for monkeypox concepts from registered '
            'tier for 1 year holdout from start of the epidemic.')
        super().__init__(issue_numbers=['DC2711'],
                         description=desc,
                         affected_datasets=[cdr_consts.REGISTERED_TIER_DEID],
                         affected_tables=CATI_TABLES + [AOU_DEATH],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         table_namer=table_namer)

        self.mapping_dataset_id = mapping_dataset_id

    def setup_rule(self, client):
        """
        Responsible for grabbing and storing deactivated participant data.

        :param client: a BiQueryClient passed to store the data
        """

        # Create suppression lookup table
        client.upload_csv_data_to_bq_table(self.sandbox_dataset_id,
                                           SUPPRESSION_RULE_CONCEPT_TABLE,
                                           MONKEYPOX_CONCEPTS_PATH, WRITE_EMPTY)

    def setup_validation(self, client, *args, **keyword_args):
        """
        Run required steps for validation setup
        """
        raise NotImplementedError("Please fix me.")

    def validate_rule(self, client, *args, **keyword_args):
        """
        Validates the cleaning rule which deletes or updates the data from the tables
        """
        raise NotImplementedError("Please fix me.")

    def get_sandbox_tablenames(self):
        return [
            self.sandbox_table_for(affected_table)
            for affected_table in self.affected_tables
        ]

    def get_query_specs(self):
        """
        Return a list of dictionary query specifications.

        :return:  A list of dictionaries. Each dictionary contains a single query
            and a specification for how to execute that query. The specifications
            are optional but the query is required.
        """
        sandbox_queries, delete_queries = [], []
        for table in self.affected_tables:

            date_fields = get_date_fields(table)
            datetime_fields = get_datetime_fields(table)

            if not date_fields and not datetime_fields:
                LOGGER.info(
                    f'Skipping {table}. {table} has no date or datetime fields.'
                )
                continue

            sandbox_queries.append({
                cdr_consts.QUERY:
                    SANDBOX_QUERY.render(
                        project_id=self.project_id,
                        sandbox_dataset_id=self.sandbox_dataset_id,
                        sandbox_table_id=self.sandbox_table_for(table),
                        dataset_id=self.dataset_id,
                        table=table,
                        mapping_dataset_id=self.mapping_dataset_id,
                        suppression_concept_table=SUPPRESSION_RULE_CONCEPT_TABLE,
                        suppression_start_date=SUPPRESSION_START_DATE_STR,
                        suppression_end_date=SUPPRESSION_END_DATE_STR,
                        concept_fields=get_concept_id_fields(table),
                        date_fields=date_fields,
                        datetime_fields=datetime_fields)
            })

            delete_queries.append({
                cdr_consts.QUERY:
                    DELETE_QUERY.render(
                        project_id=self.project_id,
                        sandbox_dataset_id=self.sandbox_dataset_id,
                        sandbox_table_id=self.sandbox_table_for(table),
                        dataset_id=self.dataset_id,
                        table=table)
            })

        return sandbox_queries + delete_queries


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    mapping_dataset_arg = {
        parser.SHORT_ARGUMENT: '-m',
        parser.LONG_ARGUMENT: '--mapping_dataset_id',
        parser.ACTION: 'store',
        parser.DEST: 'mapping_dataset_id',
        parser.HELP: 'Identifies the dataset containing _deid_map table',
        parser.REQUIRED: True
    }

    ARGS = parser.default_parse_args([mapping_dataset_arg])
    pipeline_logging.configure(level=logging.DEBUG, add_console_handler=True)

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id,
            ARGS.dataset_id,
            ARGS.sandbox_dataset_id, [(MonkeypoxConceptSuppression,)],
            mapping_dataset_id=ARGS.mapping_dataset_id)

        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id,
                                   ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(MonkeypoxConceptSuppression,)],
                                   mapping_dataset_id=ARGS.mapping_dataset_id)
