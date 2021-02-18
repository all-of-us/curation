"""
suppress all records associated with a PPI vocabulary explicit identifier associated PPI vocabulary
The concept_ids to suppress can be determined from the vocabulary with the following regular expressions.

        REGEXP_CONTAINS(concept_code, 'Signature')
        (REGEXP_CONTAINS(concept_code, 'Name') AND REGEXP_CONTAINS(concept_code, r'(First)|(Last)|(Middle)|(Help)'))
        (REGEXP_CONTAINS(concept_code, '(Address)|(PhoneNumber)') and 
            NOT REGEXP_CONTAINS(concept_code, '(PIIState)|(State_)|(ZIP)') ) and concept_class_id != 'Topic'
        REGEXP_CONTAINS(concept_code, 'SecondaryContactInfo') and concept_class_id = 'Question'
        (REGEXP_CONTAINS(concept_code, 'SocialSecurity') and concept_class_id = 'Question')
        and also covers all the mapped standard concepts for non standard concepts that the regex filters.
"""
# Python imports
import logging

# Project imports
from resources import get_concept_id_fields
from utils import pipeline_logging
from common import OBSERVATION, JINJA_ENV
from constants.cdr_cleaner import clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule

LOGGER = logging.getLogger(__name__)

ISSUE_NUMBERS = ['DC1363']
EXPLICIT_IDENTIFIER_CONCEPTS = '_explicit_records_identifier_concepts'

# Creates _explicit_identifier_concepts lookup table and populates with the concept_ids that are
# from the concept table
LOOKUP_TABLE_CREATION_QUERY = JINJA_ENV.from_string("""
CREATE TABLE IF NOT EXISTS `{{project_id}}.{{sandbox_dataset}}.{{lookup_table}}` AS 
(
with explicit_concept_ids AS
(
  SELECT
    *
  FROM
    `{{project_id}}.{{dataset_id}}.concept`
  WHERE
    REGEXP_CONTAINS(concept_code, 'Signature')
    OR (REGEXP_CONTAINS(concept_code, 'Name') AND REGEXP_CONTAINS(concept_code, r'(First)|(Last)|(Middle)|(Help)'))
    OR (REGEXP_CONTAINS(concept_code, '(Address)|(PhoneNumber)')
        AND NOT REGEXP_CONTAINS(concept_code, '(PIIState)|(State_)|(ZIP)') AND concept_class_id != 'Topic') 
    OR (REGEXP_CONTAINS(concept_code, 'SecondaryContactInfo') AND concept_class_id = 'Question')
    OR (REGEXP_CONTAINS(concept_code, 'SocialSecurity') AND concept_class_id = 'Question')
)
SELECT
   DISTINCT *
FROM
  explicit_concept_ids
UNION DISTINCT
SELECT DISTINCT
  c2.*
FROM
  explicit_concept_ids AS c
JOIN
  `{{project_id}}.{{dataset_id}}.concept_relationship` AS cr
ON
  c.concept_id = cr.concept_id_1 AND cr.relationship_id = 'Maps to'
JOIN `{{project_id}}.{{dataset_id}}.concept` AS c2
  ON cr.concept_id_2 = c2.concept_id 
)
""")

# Sandbox query to identify all the records with possible explicit record identifier concepts
SANDBOX_EXPLICIT_IDENTIFIER_RECORDS = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project_id}}.{{sandbox_dataset_id}}.{{intermediary_table}}` AS
(
SELECT
      ob.*
    FROM `{{project_id}}.{{dataset_id}}.{{observation_table}}` AS ob
    {% for concept_field in concept_fields %}
    LEFT JOIN `{{project_id}}.{{sandbox_dataset_id}}.{{lookup_table}}` AS s{{loop.index}}
      ON ob.{{concept_field}} = s{{loop.index}}.concept_id 
    {% endfor %}
    WHERE COALESCE(
    {% for concept_field in concept_fields %}
        {% if loop.previtem is defined %}, {% else %}  {% endif %} s{{loop.index}}.concept_id
    {% endfor %}) IS NOT NULL
)
""")

SUPPRESS_EXPLICIT_IDENTIFIER_RECORDS = JINJA_ENV.from_string("""
 DELETE FROM
  `{{project_id}}.{{dataset_id}}.{{observation_table}}`
WHERE
observation_id 
IN( SELECT
    observation_id
    FROM `{{project_id}}.{{sandbox_dataset_id}}.{{intermediary_table}}` )
    """)


class ExplicitIdentifierSuppression(BaseCleaningRule):
    """
    suppress all records associated with a PPI vocabulary explicit identifier associated PPI vocabulary
    """

    def __init__(self, project_id, dataset_id, sandbox_dataset_id):
        """
        Initialize the class with proper info.

        Set the issue numbers, description and affected datasets.  As other
        tickets may affect this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = f'suppress all records associated with a PPI vocabulary explicit identifier associated PPI vocabulary'
        super().__init__(issue_numbers=ISSUE_NUMBERS,
                         description=desc,
                         affected_datasets=[
                             cdr_consts.CONTROLLED_TIER_DEID,
                             cdr_consts.COMBINED
                         ],
                         affected_tables=OBSERVATION,
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id)

    def get_query_specs(self, *args, **keyword_args):
        """
        Return a list of dictionary query specifications.

        :return:  A list of dictionaries.  Each dictionary contains a
            single query and a specification for how to execute that query.
            The specifications are optional but the query is required.
        """

        lookup_table_creation_query = {
            cdr_consts.QUERY:
                LOOKUP_TABLE_CREATION_QUERY.render(
                    project_id=self.project_id,
                    sandbox_dataset=self.sandbox_dataset_id,
                    dataset_id=self.dataset_id,
                    lookup_table=EXPLICIT_IDENTIFIER_CONCEPTS)
        }

        sandbox_query = {
            cdr_consts.QUERY:
                SANDBOX_EXPLICIT_IDENTIFIER_RECORDS.render(
                    project_id=self.project_id,
                    dataset_id=self.dataset_id,
                    sandbox_dataset_id=self.sandbox_dataset_id,
                    observation_table=OBSERVATION,
                    intermediary_table=self.get_sandbox_tablenames()[0],
                    lookup_table=EXPLICIT_IDENTIFIER_CONCEPTS,
                    concept_fields=get_concept_id_fields(OBSERVATION))
        }

        suppress_query = {
            cdr_consts.QUERY:
                SUPPRESS_EXPLICIT_IDENTIFIER_RECORDS.render(
                    project_id=self.project_id,
                    dataset_id=self.dataset_id,
                    sandbox_dataset_id=self.sandbox_dataset_id,
                    intermediary_table=self.get_sandbox_tablenames()[0],
                    observation_table=OBSERVATION)
        }

        return [lookup_table_creation_query, sandbox_query, suppress_query]

    def setup_rule(self, client, *args, **keyword_args):
        """
        Function to run any data upload options before executing a query.
        """
        pass

    def get_sandbox_tablenames(self):
        return [self.sandbox_table_for(OBSERVATION)]

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


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()
    pipeline_logging.configure(level=logging.DEBUG, add_console_handler=True)

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id, ARGS.dataset_id, ARGS.sandbox_dataset_id,
            [(ExplicitIdentifierSuppression,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(ExplicitIdentifierSuppression,)])
