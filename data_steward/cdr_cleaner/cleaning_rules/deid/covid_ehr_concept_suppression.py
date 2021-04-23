import logging
from google.cloud.exceptions import GoogleCloudError

import resources
from common import JINJA_ENV, AOU_REQUIRED
import constants.cdr_cleaner.clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.deid.concept_suppression import \
    AbstractBqLookupTableConceptSuppression
from utils import pipeline_logging

LOGGER = logging.getLogger(__name__)

ISSUE_NUMBERS = ['DC1557']

SUPPRESSION_RULE_CONCEPT_TABLE = 'covid_suppression_concept'

COVID_CONCEPT_QUERY = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project}}.{{sandbox_dataset}}.{{concept_suppression_lookup}}` AS
WITH 
concept_text_lookup AS
(SELECT
 c.concept_id
,c.concept_name || ' ' || STRING_AGG(s.concept_synonym_name, ' ') AS concept_text
FROM `{{project}}.{{dataset}}.concept` c
LEFT JOIN `{{project}}.{{dataset}}.concept_synonym` s
 USING (concept_id) 
GROUP BY concept_id, concept_name)

-- Concepts created on or after Dec 2019 -- 
-- (COVID outbreak) --
,
recent_concept AS
(SELECT
 concept_id
,concept_code
,concept_name
,vocabulary_id
,valid_start_date
,valid_end_date
,invalid_reason
,standard_concept
,concept_text
FROM `{{project}}.{{dataset}}.concept` 
 JOIN concept_text_lookup t
  USING (concept_id)
WHERE valid_start_date >= '2019-12-01')

-- Get seed covid concepts --
-- using string match on concept text --
,
covid_seed AS
(SELECT 
 rc.concept_id
,rc.concept_code
,rc.concept_name
,rc.vocabulary_id
,rc.valid_start_date
,rc.valid_end_date
,rc.invalid_reason
,rc.standard_concept
 FROM recent_concept rc
 WHERE 
  vocabulary_id <> 'PPI'
AND (LOWER(concept_text) LIKE '%covid%'
 OR LOWER(concept_text) LIKE '%2019%corona%'
 OR LOWER(concept_text) LIKE '%corona%2019%'
 OR LOWER(concept_text) LIKE '%sars%cov%2')
)

-- Descendants of seed concepts above --
-- Note: it may be preferable to use concept_relationship --
-- rather than concept_ancestor here. Any deeper  --
-- investigation into this will be added as an appendix. --
,
covid_descendant AS
(SELECT 
 rc.concept_id
,rc.concept_code
,rc.concept_name
,rc.vocabulary_id
,rc.valid_start_date
,rc.valid_end_date
,rc.invalid_reason
,rc.standard_concept
 FROM `{{project}}.{{dataset}}.concept_ancestor` ca
  JOIN covid_seed cs
   ON ca.ancestor_concept_id = cs.concept_id
  JOIN recent_concept rc
   ON ca.descendant_concept_id = rc.concept_id)

-- Complete set includes seed  --
-- and descendant concepts --
SELECT * FROM covid_seed
 UNION DISTINCT 
SELECT * FROM covid_descendant
""")


class CovidEhrConceptSuppression(AbstractBqLookupTableConceptSuppression):

    def __init__(self, project_id, dataset_id, sandbox_dataset_id):
        """
        Initialize the class with proper info.

        Set the issue numbers, description and affected datasets.  As other
        tickets may affect this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = (
            'Sandbox and record suppress all records with a concept_id or concept_code '
            'relating to a COVID EHR concept. ')
        super().__init__(
            issue_numbers=ISSUE_NUMBERS,
            description=desc,
            affected_datasets=[cdr_consts.CONTROLLED_TIER_DEID],
            project_id=project_id,
            dataset_id=dataset_id,
            sandbox_dataset_id=sandbox_dataset_id,
            affected_tables=AOU_REQUIRED,
            concept_suppression_lookup_table=SUPPRESSION_RULE_CONCEPT_TABLE)

        concept_suppression_lookup_query = COVID_CONCEPT_QUERY.render(
            project=self.project_id,
            dataset=self.dataset_id,
            sandbox_dataset=self.sandbox_dataset_id,
            concept_suppression_lookup=self.concept_suppression_lookup_table)
        print(concept_suppression_lookup_query)

    def create_suppression_lookup_table(self, client):
        """
        
        :param client: 
        :return:
        
        raises google.cloud.exceptions.GoogleCloudError if a QueryJob fails 
        """
        concept_suppression_lookup_query = COVID_CONCEPT_QUERY.render(
            project=self.project_id,
            dataset=self.dataset_id,
            sandbox_dataset=self.sandbox_dataset_id,
            concept_suppression_lookup=self.concept_suppression_lookup_table)
        query_job = client.query(concept_suppression_lookup_query)
        result = query_job.result()

        if hasattr(result, 'errors') and result.errors:
            LOGGER.error(f"Error running job {result.job_id}: {result.errors}")
            raise GoogleCloudError(
                f"Error running job {result.job_id}: {result.errors}")

    def setup_validation(self, client, *args, **keyword_args):
        pass

    def validate_rule(self, client, *args, **keyword_args):
        pass


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.default_parse_args()
    pipeline_logging.configure(level=logging.DEBUG, add_console_handler=True)

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id, ARGS.dataset_id, ARGS.sandbox_dataset_id,
            [(CovidEhrConceptSuppression,)])
        for query in query_list:
            LOGGER.info(query['query'])
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(CovidEhrConceptSuppression,)])
