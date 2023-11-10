"""
Sandbox and record suppress all records with a concept_id or concept_code relating to delivery or birthing concepts.

Original Issue: DC-1977

suppress all records (specifically in the CT) if a concept is associated with a delivery or birth concept
and the record date/start_date is on a participant's birthday.
and also covers all the mapped standard concepts for non standard concepts that the regex filters.
"""

# Python Imports
import logging

# Third Party Imports
from google.cloud.exceptions import GoogleCloudError

# Project Imports
import constants.cdr_cleaner.clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from common import AOU_DEATH, AOU_REQUIRED, JINJA_ENV, DEATH, PERSON, FITBIT_TABLES, EHR_CONSENT_VALIDATION

LOGGER = logging.getLogger(__name__)

ISSUE_NUMBERS = ['DC1977', 'DC2205', 'DC3545']
BIRTH_DELIVERY_SUPPRESSION_CONCEPT_TABLE = '_birth_concepts'

EXCLUDED_CONCEPTS = [4013886, 4135376, 4271761]
all_concept_ids = [str(x) for x in EXCLUDED_CONCEPTS]
all_concept_ids = ",".join(all_concept_ids)

LOOKUP_TABLE = 'birth_columns_lookup'


class YearOfBirthRecordsSuppression(BaseCleaningRule):

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 table_namer=''):
        """
        Initialize the class with proper info.

        Set the issue numbers, description and affected datasets.  As other
        tickets may affect this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = ('Sandbox and record suppress all records within one year of a '
                'participant\'s birth.')
        super().__init__(issue_numbers=ISSUE_NUMBERS,
                         description=desc,
                         affected_datasets=[cdr_consts.CONTROLLED_TIER_DEID],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         affected_tables=['observation'],
                         table_namer=table_namer)

        self.tables_and_columns = {}
        self.observation_concept_id_columns = []

    def get_sandbox_tablenames(self):
        return [self.sandbox_table_for(table) for table in self.affected_tables]

    def setup_rule(self, client):
        self._get_time_columns(client)
        self._get_observation_concept_id_columns(client)

    def _get_observation_concept_id_columns(self, client):
        tables_columns_query_template = JINJA_ENV.from_string("""
        SELECT 
           column_name
         FROM
           `{{project}}.{{dataset}}.INFORMATION_SCHEMA.COLUMNS`
         WHERE
            (lower(data_type) in ("int64") and REGEXP_CONTAINS(column_name, r'(?i)(concept_id)'))
            -- tables we are not interested in cleaning --
           and lower(table_name) = 'observation'
        """)
        tables_columns_query = tables_columns_query_template.render(
            project=self.project_id, dataset=self.dataset_id)

        try:
            response = client.query(tables_columns_query,
                                    job_id_prefix='ct_yob_setup_')
        except GoogleCloudError as exc:
            raise exc
        else:
            response_list = list(response.result())

            # move all the columns to key/value dictionary list for JINJA templating
            new_response_list = []
            for item in response_list:
                new_response_list.append(item[0])

        self.observation_concept_id_columns = new_response_list

    def _get_time_columns(self, client):
        tables_columns_query_template = JINJA_ENV.from_string("""
        CREATE OR REPLACE TABLE `{{project}}.{{sandbox_dataset}}.{{lookup_table}}` AS (
            SELECT 
               table_name, column_name
             FROM
               `{{project}}.{{dataset}}.INFORMATION_SCHEMA.COLUMNS`
             WHERE
                (lower(data_type) in ("date", "datetime", "time", "timestamp") and not REGEXP_CONTAINS(column_name, r'(?i)(partitiontime)'))
                AND table_name = 'observation'
            ORDER BY 1,2
        )
        """)
        tables_columns_query = tables_columns_query_template.render(
            project=self.project_id,
            dataset=self.dataset_id,
            sandbox_dataset=self.sandbox_dataset_id,
            lookup_table=self.sandbox_table_for(LOOKUP_TABLE),
            fitbit_tables_str='"' + '", "'.join(FITBIT_TABLES) + '"',
        )

        try:
            response = client.query(tables_columns_query,
                                    job_id_prefix='ct_yob_setup_')
            # wait on the result before continuing
            response.result()
        except GoogleCloudError as exc:
            LOGGER.exception(
                f'Error reading time aware columns in dataset, {self.__class__.__name__}'
            )
            raise exc
        else:
            hold_response_template = JINJA_ENV.from_string("""
                SELECT table_name, column_name
                FROM `{{project}}.{{sandbox_dataset}}.{{lookup_table}}`""")

            hold_response_query = hold_response_template.render(
                project=self.project_id,
                sandbox_dataset=self.sandbox_dataset_id,
                lookup_table=self.sandbox_table_for(LOOKUP_TABLE))

            try:
                response = client.query(hold_response_query,
                                        job_id_prefix='ct_yob_setup_get_')
            except GoogleCloudError as exc:
                LOGGER.exception(
                    f'Error setting tables_and_columns variable, {self.__class__.__name__}'
                )
                raise exc
            else:
                response_list = list(response.result())

                # move all the columns to key/value dictionary list for JINJA templating
                response_dict = {}
                for item in response_list:
                    table_name = item[0]
                    column_name = item[1]
                    current_value = response_dict.get(table_name, [])
                    current_value.append(column_name)
                    response_dict[table_name] = current_value

        self.tables_and_columns = response_dict

    def get_sandbox_queries(self):
        """
        Sandbox records in the given table whose concept id fields contain any concepts in the
        suppression concept table

        :param table_name:
        :return:
        """
        bq_lookup_table_sandbox_query_template = JINJA_ENV.from_string("""
            CREATE OR REPLACE TABLE `{{project}}.{{sandbox_dataset}}.{{sandbox_table}}` AS (
                SELECT
                  d.*
                FROM `{{project}}.{{dataset}}.{{domain_table}}` AS d
                JOIN `{{project}}.{{dataset}}.person` AS p
                USING (person_id)
                WHERE ( 
                {% for column in columns_list %}
                    {% if loop.index > 1 %}
                        OR
                    {% endif %}
                    DATE(d.{{column}}) < DATE(CONCAT(p.year_of_birth + 2, '-01-01'))
                {% endfor %}
                )
                {% if domain_table == 'observation' %}
                    AND (
                    {% for column in obs_columns %}
                        {% if loop.index > 1 %}
                         AND
                        {% endif %}
                        ({{column}} not in ({{exceptions}})
                        OR {{column}} is null)
                    {% endfor %}
                    )
                {% endif %}
            )
        """)

        sandbox_queries = []
        for table_name, columns_list in self.tables_and_columns.items():
            suppression_record_sandbox_query = bq_lookup_table_sandbox_query_template.render(
                project=self.project_id,
                dataset=self.dataset_id,
                sandbox_dataset=self.sandbox_dataset_id,
                sandbox_table=self.sandbox_table_for(table_name),
                domain_table=table_name,
                columns_list=columns_list,
                obs_columns=self.observation_concept_id_columns,
                exceptions=all_concept_ids)

            sandbox_queries.append(
                {cdr_consts.QUERY: suppression_record_sandbox_query})

        return sandbox_queries

    def get_suppression_queries(self):
        suppression_record_query_template = JINJA_ENV.from_string("""
        DELETE
        FROM `{{project}}.{{dataset}}.{{domain_table}}` d 
        WHERE d.{{identifier}} in (
            SELECT
                distinct {{identifier}}
            FROM
                `{{project}}.{{sandbox_dataset}}.{{sandbox_table}}`)
        """)

        suppression_queries = []
        for table_name, _ in self.tables_and_columns.items():
            identifier = f'{table_name}_id' if table_name not in [
                DEATH, EHR_CONSENT_VALIDATION
            ] else f'{PERSON}_id'
            suppression_record_query = suppression_record_query_template.render(
                project=self.project_id,
                dataset=self.dataset_id,
                sandbox_dataset=self.sandbox_dataset_id,
                domain_table=table_name,
                identifier=identifier,
                sandbox_table=self.sandbox_table_for(table_name))

            suppression_queries.append({
                cdr_consts.QUERY: suppression_record_query,
            })

        return suppression_queries

    def get_query_specs(self):

        sandbox_queries = self.get_sandbox_queries()
        suppression_queries = self.get_suppression_queries()

        all_queries = sandbox_queries + suppression_queries

        return all_queries

    def setup_validation(self, client, *args, **keyword_args):
        pass

    def validate_rule(self, client, *args, **keyword_args):
        pass


if __name__ == '__main__':
    from utils import pipeline_logging
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    pipeline_logging.configure(level=logging.DEBUG, add_console_handler=True)

    additional_args = [{
        parser.SHORT_ARGUMENT: '-a',
        parser.LONG_ARGUMENT: '--data-stage',
        parser.DEST: 'data_stage',
        parser.ACTION: 'store',
        parser.HELP: 'The data stage you will execute on.',
        parser.REQUIRED: True
    }]
    ARGS = parser.default_parse_args(additional_args)

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id, ARGS.dataset_id, ARGS.sandbox_dataset_id,
            [(YearOfBirthRecordsSuppression,)], ARGS.data_stage)
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(YearOfBirthRecordsSuppression,)],
                                   ARGS.data_stage)
