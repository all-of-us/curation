"""
Bad end dates:
End dates should not be prior to start dates in any table
* If end date is nullable, it will be nulled
* If end date is required,
    * If visit type is inpatient(id 9201)
        * If other tables have dates for that visit, end date = max(all dates from other tables for that visit)
        * Else, end date = start date.
    * Else, If visit type is ER(id 9203)/Outpatient(id 9202), end date = start date

NOTE: TemporalConsistency is known to update date columns but not datetime columns.
    EnsureDateDatetimeConsistency must run after it to fix the inconsistent datetimes.
"""
import logging

# Project imports
from constants.cdr_cleaner import clean_cdr as cdr_consts
import resources
import common
from common import JINJA_ENV
from utils import pipeline_logging
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule, query_spec_list

LOGGER = logging.getLogger(__name__)

JIRA_ISSUE_NUMBERS = ['DC813', 'DC400', 'DC2634']

table_dates = {
    common.CONDITION_OCCURRENCE: ['condition_start_date', 'condition_end_date'],
    common.DRUG_EXPOSURE: [
        'drug_exposure_start_date', 'drug_exposure_end_date'
    ],
    common.DEVICE_EXPOSURE: [
        'device_exposure_start_date', 'device_exposure_end_date'
    ],
    common.SURVEY_CONDUCT: ['survey_start_date', 'survey_end_date']
}

visit_occurrence = common.VISIT_OCCURRENCE
visit_occurrence_dates = {
    visit_occurrence: ['visit_start_date', 'visit_end_date']
}
placeholder_date = '1900-01-01'

SANDBOX_BAD_END_DATES = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project_id}}.{{sandbox_id}}.{{intermediary_table}}` AS (
  SELECT
    *
  FROM
    `{{project_id}}.{{dataset_id}}.{{table}}`
  WHERE
    {{table_end_date}} < {{table_start_date}}
)
""")

NULL_BAD_END_DATES = JINJA_ENV.from_string("""
UPDATE `{{project_id}}.{{dataset_id}}.{{table}}`
SET {{table_end_date}} = NULL
WHERE
    {{table_end_date}} < {{table_start_date}}
""")

POPULATE_VISIT_END_DATES = JINJA_ENV.from_string("""
UPDATE `{{project_id}}.{{dataset_id}}.visit_occurrence` t
SET visit_end_date = 
    CASE
        WHEN t.visit_concept_id = 9201 AND max_end_date != "{{placeholder_date}}" THEN max_end_date
    ELSE
        t.visit_start_date
    END
FROM (
  SELECT
    GREATEST(
      COALESCE(MAX(co.condition_end_date), "{{placeholder_date}}"),
      COALESCE(MAX(dre.drug_exposure_end_date), "{{placeholder_date}}"),
      COALESCE(MAX(dve.device_exposure_end_date), "{{placeholder_date}}")
      ) AS max_end_date,
    vo.*
  FROM
    `{{project_id}}.{{dataset_id}}.visit_occurrence` vo
  LEFT JOIN
    `{{project_id}}.{{dataset_id}}.condition_occurrence` co
  ON
    vo.visit_occurrence_id = co.visit_occurrence_id
  LEFT JOIN
    `{{project_id}}.{{dataset_id}}.drug_exposure` dre
  ON
    vo.visit_occurrence_id = dre.visit_occurrence_id
  LEFT JOIN
    `{{project_id}}.{{dataset_id}}.device_exposure` dve
  ON
    vo.visit_occurrence_id = dve.visit_occurrence_id
  WHERE
    vo.visit_end_date < vo.visit_start_date
  GROUP BY
    visit_occurrence_id,
    person_id,
    visit_concept_id,
    visit_start_date,
    visit_start_datetime,
    visit_end_date,
    visit_end_datetime,
    visit_type_concept_id,
    provider_id,
    care_site_id,
    visit_source_value,
    visit_source_concept_id,
    admitting_source_concept_id,
    admitting_source_value,
    discharge_to_concept_id,
    discharge_to_source_value,
    preceding_visit_occurrence_id) v
WHERE
  t.visit_occurrence_id = v.visit_occurrence_id
""")


class TemporalConsistency(BaseCleaningRule):

    def __init__(self, project_id, dataset_id, sandbox_dataset_id=None):
        """
        Initialize the class with proper information.

        Set the issue numbers, description and affected datasets. As other tickets may affect
        this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = (
            'Returns queries to update end dates, end dates should not be prior to any start date'
        )

        super().__init__(issue_numbers=JIRA_ISSUE_NUMBERS,
                         description=desc,
                         affected_datasets=[cdr_consts.COMBINED],
                         affected_tables=[
                             common.CONDITION_OCCURRENCE, common.DRUG_EXPOSURE,
                             common.DEVICE_EXPOSURE, common.SURVEY_CONDUCT,
                             common.VISIT_OCCURRENCE
                         ],
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
        sandbox_queries = []

        for table in table_dates:
            # create sandbox queries
            sandbox_query = dict()
            sandbox_query[cdr_consts.QUERY] = SANDBOX_BAD_END_DATES.render(
                project_id=self.project_id,
                sandbox_id=self.sandbox_dataset_id,
                intermediary_table=self.sandbox_table_for(table),
                dataset_id=self.dataset_id,
                table=table,
                table_end_date=table_dates[table][1],
                table_start_date=table_dates[table][0])
            sandbox_queries.append(sandbox_query)

            fields = resources.fields_for(table)
            # Generate column expressions for select
            col_exprs = [
                'r.' + field['name'] if field['name'] == table_dates[table][1]
                else 'l.' + field['name'] for field in fields
            ]
            cols = ', '.join(col_exprs)
            query = dict()
            query[cdr_consts.QUERY] = NULL_BAD_END_DATES.render(
                project_id=self.project_id,
                dataset_id=self.dataset_id,
                cols=cols,
                table=table,
                table_start_date=table_dates[table][0],
                table_end_date=table_dates[table][1])
            queries.append(query)
        # Sandbox query for visit_occurrence
        sandbox_vo_query = dict()
        sandbox_vo_query[cdr_consts.QUERY] = SANDBOX_BAD_END_DATES.render(
            project_id=self.project_id,
            sandbox_id=self.sandbox_dataset_id,
            intermediary_table=self.sandbox_table_for(visit_occurrence),
            dataset_id=self.dataset_id,
            table=visit_occurrence,
            table_end_date=visit_occurrence_dates[visit_occurrence][1],
            table_start_date=visit_occurrence_dates[visit_occurrence][0])
        sandbox_queries.append(sandbox_vo_query)
        vo_query = dict()
        vo_query[cdr_consts.QUERY] = POPULATE_VISIT_END_DATES.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            placeholder_date=placeholder_date)
        queries.append(vo_query)
        return sandbox_queries + queries

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
        pass


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
                                                 [(TemporalConsistency,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(TemporalConsistency,)])
