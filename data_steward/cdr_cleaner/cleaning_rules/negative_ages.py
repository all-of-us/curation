"""
Age should not be negative for the person at any dates/start dates.
Using rule 20, 21 in Achilles Heel for reference.
Also ensure ages are not beyond 150.
"""
import logging

# Project imports
import common
from constants import bq_utils as bq_consts
from constants.cdr_cleaner import clean_cdr as cdr_consts
from common import AOU_DEATH, DEATH, JINJA_ENV, PERSON
from utils import pipeline_logging
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule, query_spec_list

LOGGER = logging.getLogger(__name__)

JIRA_ISSUE_NUMBERS = ['DC811', 'DC393', 'DC2633']

# tables to consider, along with their date/start date fields
date_fields = {
    common.OBSERVATION_PERIOD: 'observation_period_start_date',
    common.VISIT_OCCURRENCE: 'visit_start_date',
    common.CONDITION_OCCURRENCE: 'condition_start_date',
    common.PROCEDURE_OCCURRENCE: 'procedure_date',
    common.DRUG_EXPOSURE: 'drug_exposure_start_date',
    common.OBSERVATION: 'observation_date',
    common.DRUG_ERA: 'drug_era_start_date',
    common.CONDITION_ERA: 'condition_era_start_date',
    common.MEASUREMENT: 'measurement_date',
    common.SURVEY_CONDUCT: 'survey_start_date',
    common.DEVICE_EXPOSURE: 'device_exposure_start_date'
}

MAX_AGE = 150
affected_tables = list(date_fields.keys())
affected_tables.extend([AOU_DEATH, DEATH])

#sandbox rows to be removed
SANDBOX_NEGATIVE_AND_MAX_AGE_QUERY = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project_id}}.{{sandbox_id}}.{{intermediary_table}}` AS (
SELECT
  *
FROM
  `{{project_id}}.{{dataset_id}}.{{table}}`
WHERE
  {{table}}_id IN (
  SELECT
    t.{{table}}_id
  FROM
    `{{project_id}}.{{dataset_id}}.{{table}}` t
  JOIN
    `{{project_id}}.{{dataset_id}}.{{person_table}}` p
  ON
    t.person_id = p.person_id
  WHERE
    DATE(t.{{table_date}}) < DATE(p.birth_datetime)
  UNION DISTINCT
  SELECT
  t.{{table}}_id
  FROM
    `{{project_id}}.{{dataset_id}}.{{table}}` t
  JOIN
    `{{project_id}}.{{dataset_id}}.{{person_table}}` p
  ON
    t.person_id = p.person_id
  WHERE
    EXTRACT(YEAR
    FROM
      t.{{table_date}}) - EXTRACT(YEAR
    FROM
      p.birth_datetime) > {{MAX_AGE}})
)
""")

SANDBOX_NEGATIVE_AGE_DEATH_QUERY = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project_id}}.{{sandbox_id}}.{{intermediary_table}}` AS (
SELECT
  *
FROM
  `{{project_id}}.{{dataset_id}}.{{table}}`
WHERE
  {% if table == 'death' %}
  person_id IN (SELECT d.person_id
  {% elif table == 'aou_death' %}
  aou_death_id IN (SELECT d.aou_death_id
  {% endif %}
  FROM
    `{{project_id}}.{{dataset_id}}.{{table}}` d
  JOIN
    `{{project_id}}.{{dataset_id}}.{{person_table}}` p
  ON
    d.person_id = p.person_id
  WHERE
    d.death_date < DATE(p.birth_datetime))
)
""")

# negative age at recorded time in table
NEGATIVE_AGES_QUERY = JINJA_ENV.from_string("""
SELECT
  *
FROM
  `{{project_id}}.{{dataset_id}}.{{table}}`
WHERE
  {{table}}_id NOT IN (
  SELECT
    t.{{table}}_id
  FROM
    `{{project_id}}.{{dataset_id}}.{{table}}` t
  JOIN
    `{{project_id}}.{{dataset_id}}.{{person_table}}` p
  ON
    t.person_id = p.person_id
  WHERE
    DATE(t.{{table_date}}) < DATE(p.birth_datetime))
""")

# age > MAX_AGE (=150) at recorded time in table
MAX_AGE_QUERY = JINJA_ENV.from_string("""
SELECT
  *
FROM
  `{{project_id}}.{{dataset_id}}.{{table}}`
WHERE
  {{table}}_id NOT IN (
  SELECT
    t.{{table}}_id
  FROM
    `{{project_id}}.{{dataset_id}}.{{table}}` t
  JOIN
    `{{project_id}}.{{dataset_id}}.{{person_table}}` p
  ON
    t.person_id = p.person_id
  WHERE
    EXTRACT(YEAR
    FROM
      t.{{table_date}}) - EXTRACT(YEAR
    FROM
      p.birth_datetime) > {{MAX_AGE}})
""")

# negative age at death
NEGATIVE_AGE_DEATH_QUERY = JINJA_ENV.from_string("""
SELECT
  *
FROM
  `{{project_id}}.{{dataset_id}}.{{table}}`
WHERE
  {% if table == 'death' %}
  person_id NOT IN (SELECT d.person_id
  {% elif table == 'aou_death' %}
  aou_death_id NOT IN (SELECT d.aou_death_id
  {% endif %}
  FROM
    `{{project_id}}.{{dataset_id}}.{{table}}` d
  JOIN
    `{{project_id}}.{{dataset_id}}.{{person_table}}` p
  ON
    d.person_id = p.person_id
  WHERE
    d.death_date < DATE(p.birth_datetime))
""")


class NegativeAges(BaseCleaningRule):

    def __init__(self, project_id, dataset_id, sandbox_dataset_id=None):
        """
        Initialize the class with proper information.

        Set the issue numbers, description and affected datasets. As other tickets may affect
        this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = (
            'Returns queries to remove table records which are prior to the persons birth date or 150 years past the '
            'birth date from a dataset.')

        super().__init__(issue_numbers=JIRA_ISSUE_NUMBERS,
                         description=desc,
                         affected_datasets=[cdr_consts.COMBINED],
                         affected_tables=affected_tables,
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id)

    def get_query_specs(self, *args, **keyword_args) -> query_spec_list:
        """
        Return a list of dictionary query specifications.

        :return:  A list of dictionaries. Each dictionary contains a single query
            and a specification for how to execute that query. The specifications
            are optional but the query is required.
        """

        queries = []
        for table in date_fields:
            sandbox_query = dict()
            query_na = dict()
            query_ma = dict()
            sandbox_query[
                cdr_consts.QUERY] = SANDBOX_NEGATIVE_AND_MAX_AGE_QUERY.render(
                    project_id=self.project_id,
                    dataset_id=self.dataset_id,
                    sandbox_id=self.sandbox_dataset_id,
                    intermediary_table=self.sandbox_table_for(table),
                    table=table,
                    person_table=PERSON,
                    table_date=date_fields[table],
                    MAX_AGE=MAX_AGE)
            queries.append(sandbox_query)
            query_na[cdr_consts.QUERY] = NEGATIVE_AGES_QUERY.render(
                project_id=self.project_id,
                dataset_id=self.dataset_id,
                table=table,
                person_table=PERSON,
                table_date=date_fields[table])
            query_na[cdr_consts.DESTINATION_TABLE] = table
            query_na[cdr_consts.DISPOSITION] = bq_consts.WRITE_TRUNCATE
            query_na[cdr_consts.DESTINATION_DATASET] = self.dataset_id
            query_ma[cdr_consts.QUERY] = MAX_AGE_QUERY.render(
                project_id=self.project_id,
                dataset_id=self.dataset_id,
                table=table,
                person_table=PERSON,
                table_date=date_fields[table],
                MAX_AGE=MAX_AGE)
            query_ma[cdr_consts.DESTINATION_TABLE] = table
            query_ma[cdr_consts.DISPOSITION] = bq_consts.WRITE_TRUNCATE
            query_ma[cdr_consts.DESTINATION_DATASET] = self.dataset_id
            queries.extend([query_na, query_ma])

        # query for death before birthdate
        for table in [AOU_DEATH, DEATH]:
            sandbox_query = dict()
            query = dict()
            sandbox_query[
                cdr_consts.QUERY] = SANDBOX_NEGATIVE_AGE_DEATH_QUERY.render(
                    project_id=self.project_id,
                    dataset_id=self.dataset_id,
                    sandbox_id=self.sandbox_dataset_id,
                    intermediary_table=self.sandbox_table_for(table),
                    table=table,
                    person_table=PERSON)
            queries.append(sandbox_query)
            query[cdr_consts.QUERY] = NEGATIVE_AGE_DEATH_QUERY.render(
                project_id=self.project_id,
                dataset_id=self.dataset_id,
                table=table,
                person_table=PERSON)
            query[cdr_consts.DESTINATION_TABLE] = table
            query[cdr_consts.DISPOSITION] = bq_consts.WRITE_TRUNCATE
            query[cdr_consts.DESTINATION_DATASET] = self.dataset_id
            queries.append(query)

        return queries

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

    ARGS = parser.parse_args()
    pipeline_logging.configure(level=logging.DEBUG, add_console_handler=True)

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(ARGS.project_id,
                                                 ARGS.dataset_id,
                                                 ARGS.sandbox_dataset_id,
                                                 [(NegativeAges,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id, [(NegativeAges,)])
