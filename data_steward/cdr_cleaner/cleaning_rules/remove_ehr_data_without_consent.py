"""
All EHR data associated with a participant if their EHR consent
(observation_source_value = 'EHRConsentPII_ConsentPermission') is not present in the observation table is to be
sandboxed and dropped from the combined dataset

Original Issues: DC-1644
"""

# Python imports
import logging

# Third party imports
from google.cloud.exceptions import GoogleCloudError

# Project imports
import common
import constants.cdr_cleaner.clean_cdr as cdr_consts
from resources import get_person_id_tables
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from cdr_cleaner.clean_cdr_utils import get_tables_in_dataset

LOGGER = logging.getLogger(__name__)

JIRA_ISSUE_NUMBERS = ['DC1644', 'DC3355', 'DC3434']

EHR_UNCONSENTED_PARTICIPANTS_LOOKUP_TABLE = '_ehr_unconsented_pids'

AFFECTED_TABLES = [
    table for table in get_person_id_tables(common.AOU_REQUIRED)
    if table not in [common.PERSON, common.DEATH]
]

UNCONSENTED_PID_QUERY = common.JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE
    `{{project}}.{{sandbox_dataset}}.{{unconsented_lookup}}` AS (
WITH
  ordered_response AS (
  SELECT
    person_id,
    value_source_concept_id,
    observation_datetime,
    ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY observation_datetime DESC, value_source_concept_id ASC) AS rn
  FROM
    `{{project}}.{{dataset}}.observation`
  WHERE
    observation_source_value = 'EHRConsentPII_ConsentPermission')
SELECT
  person_id
FROM
  `{{project}}.{{dataset}}.person`
WHERE
  person_id NOT IN (
  SELECT
    person_id
  FROM
    ordered_response
  WHERE
    rn = 1
    AND value_source_concept_id = 1586100)
OR
  person_id IN ( -- persons without valid consent status --
  SELECT
    person_id
  FROM
    `{{project}}.{{dataset}}.consent_validation`
  WHERE
    consent_for_electronic_health_records IS NULL
  OR
    UPPER(consent_for_electronic_health_records) != 'SUBMITTED'
)
  OR
    person_id IN ( -- dup accounts --
  SELECT DISTINCT participant_id FROM
    `{{project}}.{{duplicates_dataset}}.{{duplicates_table}}`
)
)
""")

SANDBOX_ROWS = common.JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE
  `{{project}}.{{sandbox_dataset}}.{{sandbox_table}}` AS (
  SELECT
    *
  FROM
    `{{project}}.{{dataset}}.{{domain_table}}` d
  JOIN
    `{{project}}.{{dataset}}.{{mapping_domain_table}}` md
  USING
    ({{domain_table}}_id)
  WHERE
    person_id IN (
    SELECT
      person_id
    FROM
      `{{project}}.{{sandbox_dataset}}.{{unconsented_lookup}}`)
    AND src_dataset_id LIKE '%ehr%'
  )
""")

DROP_ROWS = common.JINJA_ENV.from_string("""
DELETE
FROM
  `{{project}}.{{dataset}}.{{domain_table}}`
WHERE
  {{domain_table}}_id IN (
  SELECT
    {{domain_table}}_id
  FROM
    `{{project}}.{{sandbox_dataset}}.{{sandbox_table}}`)
""")


class RemoveEhrDataWithoutConsent(BaseCleaningRule):
    """
    All EHR data associated with a participant if their EHR consent is not present in the observation table is to be
    sandboxed and dropped from the CDR.
    """

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 table_namer=None,
                 ehr_duplicates_dataset=None,
                 ehr_duplicates_table=None):
        """
        Initialize the class with proper information.

        Set the issue numbers, description and affected datasets. As other tickets may affect
        this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!

        :params: truncation_date: the last date that should be included in the
            dataset
        """
        desc = (
            'All EHR data associated with a participant if their EHR consent is not present in the observation '
            'table will be sandboxed and dropped from the CDR.  This includes duplicate records'
        )

        if not ehr_duplicates_table or not ehr_duplicates_table:
            raise RuntimeError('duplicate data is not present')

        self.ehr_duplicates_dataset = ehr_duplicates_dataset
        self.ehr_duplicates_table = ehr_duplicates_table

        super().__init__(
            issue_numbers=JIRA_ISSUE_NUMBERS,
            description=desc,
            affected_datasets=[cdr_consts.COMBINED],
            affected_tables=AFFECTED_TABLES,
            project_id=project_id,
            dataset_id=dataset_id,
            sandbox_dataset_id=sandbox_dataset_id,
        )

    def get_query_specs(self):
        """
        Return a list of dictionary query specifications.

        :return:  A list of dictionaries. Each dictionary contains a single query
            and a specification for how to execute that query. The specifications
            are optional but the query is required.
        """
        lookup_queries = []
        sandbox_queries = []
        drop_queries = []
        unconsented_lookup_query = {
            cdr_consts.QUERY:
                UNCONSENTED_PID_QUERY.render(
                    project=self.project_id,
                    dataset=self.dataset_id,
                    sandbox_dataset=self.sandbox_dataset_id,
                    unconsented_lookup=EHR_UNCONSENTED_PARTICIPANTS_LOOKUP_TABLE,
                    duplicates_dataset=self.ehr_duplicates_dataset,
                    duplicates_table=self.ehr_duplicates_table,
                )
        }
        lookup_queries.append(unconsented_lookup_query)

        for table in self.affected_tables:

            sandbox_query = {
                cdr_consts.QUERY:
                    SANDBOX_ROWS.render(
                        project=self.project_id,
                        dataset=self.dataset_id,
                        sandbox_dataset=self.sandbox_dataset_id,
                        sandbox_table=self.sandbox_table_for(table),
                        domain_table=table,
                        unconsented_lookup=
                        EHR_UNCONSENTED_PARTICIPANTS_LOOKUP_TABLE,
                        mapping_domain_table=f'_mapping_{table}')
            }

            sandbox_queries.append(sandbox_query)

            drop_query = {
                cdr_consts.QUERY:
                    DROP_ROWS.render(
                        project=self.project_id,
                        dataset=self.dataset_id,
                        sandbox_dataset=self.sandbox_dataset_id,
                        domain_table=table,
                        sandbox_table=self.sandbox_table_for(table))
            }

            drop_queries.append(drop_query)

        return lookup_queries + sandbox_queries + drop_queries

    def setup_rule(self, client):
        """
        Function to run any data upload options before executing a query.
        """
        try:
            self.affected_tables = get_tables_in_dataset(
                client, self.project_id, self.dataset_id, self.affected_tables)
        except GoogleCloudError as error:
            LOGGER.error(error)
            raise

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
        """
        Generates list of sandbox table names created by this rule.
        """
        return [self.sandbox_table_for(table) for table in self.affected_tables]


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ext_parser = parser.get_argument_parser()
    ARGS = ext_parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id,
            ARGS.dataset_id,
            ARGS.sandbox_dataset_id,
            [(RemoveEhrDataWithoutConsent,)],
            duplicates_dataset=ARGS.duplicates_dataset,
            duplicates_table=ARGS.duplicates_table,
        )

        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id,
                                   ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   ARGS.cutoff_date,
                                   [(RemoveEhrDataWithoutConsent,)],
                                   duplicates_dataset=ARGS.duplicates_dataset,
                                   duplicates_table=ARGS.duplicates_table)
