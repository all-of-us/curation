"""
Sandbox and record suppress all records with a concept_id or concept_code relating to Geo Location information.

Original Issue: DC-1385

suppress all records associated with a GeoLocation identifier concepts in PPI vocabulary 
The concept_ids to suppress can be determined from the vocabulary with the following regular expressions.
        REGEXP_CONTAINS(concept_code, r'(SitePairing)|(City)|(ArizonaSpecific)|(Michigan)|(_Country)| \
        (ExtraConsent_[A-Za-z]+((Care)|(Registered)))')AND concept_class_id = 'Question')
and also covers all the mapped standard concepts for non standard concepts that the regex filters.
"""

# Python Imports
import logging

# Third Party Imports
from google.cloud.exceptions import GoogleCloudError

# Project Imports
from common import OBSERVATION
from common import JINJA_ENV
import constants.cdr_cleaner.clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.deid.concept_suppression import \
    AbstractBqLookupTableConceptSuppression

LOGGER = logging.getLogger(__name__)

ISSUE_NUMBERS = ['DC1385']
GEO_LOCATION_SUPPRESSION_CONCEPT_TABLE = '_geolocation_identifier_concepts'

GEO_LOCATION_CONCEPT_SUPPRESSION_LOOKUP_QUERY = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project_id}}.{{sandbox_dataset_id}}.{{lookup_table}}` as(
  WITH
    geolocation_concept_ids AS (
    SELECT
      DISTINCT *
    FROM
      `{{project_id}}.{{dataset_id}}.concept`
    WHERE
      REGEXP_CONTAINS(concept_code, r'(SitePairing)|(City)|(ArizonaSpecific)|(Michigan)|(_Country)|(ExtraConsent_[A-Za-z]+((Care)|(Registered)))')AND concept_class_id = 'Question')
    SELECT
      DISTINCT *
    FROM
        geolocation_concept_ids
    UNION DISTINCT
    SELECT
      DISTINCT *
    FROM
      `{{project_id}}.{{dataset_id}}.concept`
    WHERE
      concept_id IN(
        SELECT
          cr.concept_id_2
        FROM
          geolocation_concept_ids AS c
        JOIN
          `{{project_id}}.{{dataset_id}}.concept_relationship` AS cr
        ON
          c.concept_id = cr.concept_id_1
        WHERE
          cr.relationship_id = 'Maps to')
     )
 """)


class GeoLocationConceptSuppression(AbstractBqLookupTableConceptSuppression):

    def __init__(self, project_id, dataset_id, sandbox_dataset_id):
        """
        Initialize the class with proper info.

        Set the issue numbers, description and affected datasets.  As other
        tickets may affect this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = (
            'Sandbox and record suppress all records with a concept_id or concept_code '
            'relating to Geo Location information. ')
        super().__init__(issue_numbers=ISSUE_NUMBERS,
                         description=desc,
                         affected_datasets=[cdr_consts.CONTROLLED_TIER_DEID],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         affected_tables=[OBSERVATION],
                         concept_suppression_lookup_table=
                         GEO_LOCATION_SUPPRESSION_CONCEPT_TABLE)

    def create_suppression_lookup_table(self, client):
        """
        :param client: Bigquery client
        :return: None
        raises google.cloud.exceptions.GoogleCloudError if a QueryJob fails 
        """
        concept_suppression_lookup_query = GEO_LOCATION_CONCEPT_SUPPRESSION_LOOKUP_QUERY.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            sandbox_dataset_id=self.sandbox_dataset_id,
            lookup_table=self.concept_suppression_lookup_table)
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
    from utils import pipeline_logging
    import cdr_cleaner.clean_cdr_engine as clean_engine

    pipeline_logging.configure(level=logging.DEBUG, add_console_handler=True)

    ARGS = parser.default_parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id, ARGS.dataset_id, ARGS.sandbox_dataset_id,
            [(GeoLocationConceptSuppression,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(GeoLocationConceptSuppression,)])
