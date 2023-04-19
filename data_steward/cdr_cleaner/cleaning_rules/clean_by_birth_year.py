"""
Year of birth should not be in the future, before 1800, or indicate
the participant is less than 18 years old.
Using rule 18, 19 in Achilles Heel for reference.
"""
import logging

# Project imports
import constants.cdr_cleaner.clean_cdr as cdr_consts
import resources
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule, query_spec_list
from common import AOU_DEATH, CATI_TABLES, JINJA_ENV, PERSON

LOGGER = logging.getLogger(__name__)

ISSUE_NUMBERS = ['DC392', 'DC809']
MIN_YEAR_OF_BIRTH = 1800
MAX_YEAR_OF_BIRTH = '(EXTRACT(YEAR FROM CURRENT_DATE()) - 17)'

LIST_PERSON_ID_TABLES = JINJA_ENV.from_string("""
  SELECT
  table_name
  from `{{project_id}}.{{dataset_id}}.INFORMATION_SCHEMA.COLUMNS`
  where lower(column_name) = 'person_id'
""")

SANDBOX_PERSON_TABLE_ROWS = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project_id}}.{{sandbox_dataset_id}}.{{sandbox_table}}` as (
    SELECT *
    FROM `{{project_id}}.{{dataset_id}}.{{person_table}}` p
    WHERE p.year_of_birth < {{MIN_YEAR_OF_BIRTH}}
    OR p.year_of_birth > {{MAX_YEAR_OF_BIRTH}}
)""")

SANDBOX_TABLE_ROWS = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project_id}}.{{sandbox_dataset_id}}.{{sandbox_table}}` as (
    SELECT *
    FROM `{{project_id}}.{{dataset_id}}.{{table}}`
    WHERE person_id
    IN
      (SELECT distinct person_id
       FROM `{{project_id}}.{{sandbox_dataset_id}}.{{sandbox_person_table}}`)
)""")

DELETE_RECORDS_BY_SANDBOX = JINJA_ENV.from_string("""
DELETE
FROM `{{project_id}}.{{dataset_id}}.{{table}}`
WHERE person_id
IN
  (SELECT distinct person_id
   FROM `{{project_id}}.{{sandbox_dataset_id}}.{{sandbox_table}}`)
    """)


class CleanByBirthYear(BaseCleaningRule):

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 table_namer=None):
        """
        Initialize the class.
        Set the issue numbers, description and affected datasets. As other tickets may affect
        this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = (
            'Sandbox and remove records when the participant\'s year of birth '
            'indicates he/she was born before 1800, in the last 17 years, or in '
            'the future.')

        person_id_tables = resources.get_person_id_tables(CATI_TABLES +
                                                          [AOU_DEATH])

        super().__init__(issue_numbers=ISSUE_NUMBERS,
                         description=desc,
                         affected_datasets=[cdr_consts.RDR, cdr_consts.UNIONED],
                         affected_tables=person_id_tables,
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         table_namer=table_namer)

    def get_sandbox_tablenames(self):
        return [self.sandbox_table_for(table) for table in self.affected_tables]

    def setup_rule(self, client, *args, **keyword_args):
        """
        This will reset the affected_tables list.

        For thoroughness, this will query to get the table names
        of all tables in the dataset containing a person_id.  This
        will then be used to create the queries.  If setup_rule is not run,
        then the list will default to the set of (1) AoU Required tables with a
        person_id column, (2) survey_conduct, and (3) aou_death.
        """
        columns_query = LIST_PERSON_ID_TABLES.render(project_id=self.project_id,
                                                     dataset_id=self.dataset_id)
        result = client.query(columns_query).result()

        person_tables = []
        for row in result:
            person_tables.append(row.get('table_name'))

        self.affected_tables = person_tables

    def get_query_specs(self, *args, **keyword_args) -> query_spec_list:
        queries_list = []

        # Sandbox queries
        # sandboxing data for person needs to be done first
        queries_list.append({
            cdr_consts.QUERY:
                SANDBOX_PERSON_TABLE_ROWS.render(
                    project_id=self.project_id,
                    dataset_id=self.dataset_id,
                    sandbox_dataset_id=self.sandbox_dataset_id,
                    sandbox_table=self.sandbox_table_for(PERSON),
                    person_table=PERSON,
                    MIN_YEAR_OF_BIRTH=MIN_YEAR_OF_BIRTH,
                    MAX_YEAR_OF_BIRTH=MAX_YEAR_OF_BIRTH)
        })

        omop_tables = set(self.affected_tables) - set([PERSON])
        for table in omop_tables:
            queries_list.append({
                cdr_consts.QUERY:
                    SANDBOX_TABLE_ROWS.render(
                        project_id=self.project_id,
                        sandbox_dataset_id=self.sandbox_dataset_id,
                        sandbox_table=self.sandbox_table_for(table),
                        dataset_id=self.dataset_id,
                        table=table,
                        sandbox_person_table=self.sandbox_table_for(PERSON))
            })

        # Delete queries
        for table in self.affected_tables:
            queries_list.append({
                cdr_consts.QUERY:
                    DELETE_RECORDS_BY_SANDBOX.render(
                        project_id=self.project_id,
                        dataset_id=self.dataset_id,
                        table=table,
                        sandbox_dataset_id=self.sandbox_dataset_id,
                        sandbox_table=self.sandbox_table_for(table))
            })

        return queries_list

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


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(ARGS.project_id,
                                                 ARGS.dataset_id,
                                                 ARGS.sandbox_dataset_id,
                                                 [(CleanByBirthYear,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(CleanByBirthYear,)])