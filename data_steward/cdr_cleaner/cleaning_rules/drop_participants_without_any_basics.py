"""
Run the drop_participants_without_any_basics validation clean rule.

Drops all data for participants who:
  1. have not completed any question in The Basics
  2. do not have any EHR data (- excluded as of DC-706 as noted below)

(1) is achieved by checking the observation table for children of TheBasics
module.

As part of DC-696, several participants were found with no basics survey still persisting in the CDR
These records were removed, and DC-706 was created to remove such participants in the future. DC-2551
was created to update DropParticipantsWithoutPPI to DropParticipantsWithoutAnyBasics to
reflect what the rule is actually doing, dropping participants who have not completed any question in The Basics.
It also moves the cleaning rule to RDR_CLEANING_CLASSES list to allow curation to deliver a list of
participants expected to exist in the CT dataset when the RDR dataset is cleaned.

As part of this effort, the condition for dropping has been modified to participants who:
  1. have not completed "The Basics" PPI module, via the RDR
"""
import logging

from cdr_cleaner.cleaning_rules.drop_rows_for_missing_persons import DropMissingParticipants
from common import JINJA_ENV, PERSON
from constants.cdr_cleaner import clean_cdr as cdr_consts

LOGGER = logging.getLogger(__name__)

ISSUE_NUMBERS = ["DC584", "DC696", "DC706", "DC2551"]

BASICS_MODULE_CONCEPT_ID = 1586134

SELECT_QUERY = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project}}.{{sandbox_dataset}}.{{sandbox_table}}` AS
SELECT p.*
""")

PERSON_WITH_NO_BASICS = JINJA_ENV.from_string("""
{{query_type}}
FROM `{{project}}.{{dataset}}.person` p
WHERE person_id NOT IN
(SELECT
    person_id
  FROM `{{project}}.{{dataset}}.concept_ancestor`
  INNER JOIN `{{project}}.{{dataset}}.observation` o ON observation_concept_id = descendant_concept_id
  INNER JOIN `{{project}}.{{dataset}}.concept` d ON d.concept_id = descendant_concept_id
  WHERE ancestor_concept_id = {{basics_concept_id}}

  UNION DISTINCT

  SELECT
    person_id
  FROM `{{project}}.{{dataset}}.concept`
  JOIN `{{project}}.{{dataset}}.concept_ancestor`
    ON (concept_id = ancestor_concept_id)
  JOIN `{{project}}.{{dataset}}.observation`
    ON (descendant_concept_id = observation_concept_id)
  WHERE concept_class_id = 'Module'
    AND concept_name IN ('The Basics')
    AND questionnaire_response_id IS NOT NULL)
""")


class DropParticipantsWithoutAnyBasics(DropMissingParticipants):
    """
    Drops participants who have not completed any of the "The Basics" survey
    """

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 table_namer=None):
        desc = (f'Sandbox and remove PIDs with no PPI basics.'
                f'Use drop missing participants CR to remove their records.')

        super().__init__(issue_numbers=ISSUE_NUMBERS,
                         description=desc,
                         affected_datasets=[cdr_consts.RDR],
                         affected_tables=[PERSON],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         table_namer=table_namer,
                         run_for_synthetic=False)

    def get_query_specs(self):
        """
        Return a list of queries to remove data-poor participant rows.

        The removal criteria is for participants is as follows:
        1. They have not completed "The Basics" PPI module, via the RDR
        These participants are not particularly useful for analysis, so remove them
        here.
        :return:  A list of string queries that can be executed to sandbox and delete data-poor
            participants and corresponding rows from relevant tables in the dataset.
        """

        queries = []
        select_stmt = SELECT_QUERY.render(
            project=self.project_id,
            sandbox_dataset=self.sandbox_dataset_id,
            sandbox_table=self.sandbox_table_for(PERSON))

        select_query = PERSON_WITH_NO_BASICS.render(
            query_type=select_stmt,
            project=self.project_id,
            dataset=self.dataset_id,
            basics_concept_id=BASICS_MODULE_CONCEPT_ID)
        queries.append({cdr_consts.QUERY: select_query})

        delete_query = PERSON_WITH_NO_BASICS.render(
            query_type="DELETE",
            project=self.project_id,
            dataset=self.dataset_id,
            basics_concept_id=BASICS_MODULE_CONCEPT_ID)
        queries.append({cdr_consts.QUERY: delete_query})

        # drop from the person table, then delete all corresponding data for the now missing persons
        queries.extend(super().get_query_specs())
        return queries


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id, ARGS.dataset_id, ARGS.sandbox_dataset_id,
            [(DropParticipantsWithoutAnyBasics,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(DropParticipantsWithoutAnyBasics,)])
