"""
This script updates observation with the given new set of the basics survey
responses. The input table must be located in the sandbox_dataset.
This update is performed for each dataset based on its data stage.

This is a one-off cleaning rule for DC-3016 hot-fix. It is not expected to
run for every CDR release.

Original Issues: DC-3016 and its subtasks.
"""

#Python imports
import logging

# Project imports
from common import JINJA_ENV, OBSERVATION
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from constants.cdr_cleaner.clean_cdr import (COMBINED,
                                             CONTROLLED_TIER_DEID_BASE,
                                             CONTROLLED_TIER_DEID_CLEAN, QUERY,
                                             REGISTERED_TIER_DEID_BASE,
                                             REGISTERED_TIER_DEID_CLEAN)
from retraction.retract_utils import is_combined_dataset, is_ct_dataset, is_deid_dataset, is_rt_dataset

LOGGER = logging.getLogger(__name__)

ISSUE_NUMBERS = ['DC3016']

SANDBOX_MEANINGLESS_SURVEY_RESPONSE_QUERY = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project}}.{{sandbox_dataset}}.{{sandbox_table}}` AS (
    SELECT *
    FROM `{{project}}.{{dataset}}.observation` o
    WHERE EXISTS (
        SELECT 1 FROM `{{project}}.{{lookup_dataset}}.{{lookup_table}}` l
{% if is_combined_dataset %}
        WHERE l.person_id = o.person_id
{% elif is_deid_dataset %}
        JOIN `{{project}}.{{deid_map_dataset_id}}.{{deid_map_table_id}}` d
        ON l.person_id = d.person_id
        WHERE d.research_id = o.person_id
{% endif %}
        AND l.observation_source_concept_id = o.observation_source_concept_id
    )
)
""")

DELETE_MEANINGLESS_SURVEY_RESPONSE_QUERY = JINJA_ENV.from_string("""
DELETE FROM `{{project}}.{{dataset}}.observation`
WHERE observation_id IN (
    SELECT observation_id 
    FROM `{{project}}.{{sandbox_dataset}}.{{sandbox_table}}` 
)
""")

INSERT_CORRECT_SURVEY_RESPONSE_QUERY = JINJA_ENV.from_string("""
INSERT INTO `{{project}}.{{dataset}}.observation`
    (observation_id, person_id, observation_concept_id,
     observation_date, observation_datetime, observation_type_concept_id,
     value_as_number, value_as_string, value_as_concept_id,
     qualifier_concept_id, unit_concept_id, provider_id,
     visit_occurrence_id, visit_detail_id,
     observation_source_value, observation_source_concept_id,
     unit_source_value, qualifier_source_value,
     value_source_concept_id, value_source_value,
     questionnaire_response_id)
SELECT
    l.observation_id,
{% if is_combined_dataset %}
    l.person_id,
{% elif is_ct_dataset or is_rt_dataset %}
    d.research_id AS person_id,
{% endif %}
    l.observation_concept_id,
{% if is_combined_dataset or is_ct_dataset %}
    l.observation_date,
    l.observation_datetime,
{% elif is_rt_dataset %}
    DATE_SUB(l.observation_date, INTERVAL d.shift DAY) AS observation_date,
    DATE_SUB(l.observation_datetime, INTERVAL d.shift DAY) AS observation_datetime,
{% endif %}
    l.observation_type_concept_id,
    l.value_as_number,
    l.value_as_string,
    l.value_as_concept_id,
    l.qualifier_concept_id,
    l.unit_concept_id,
    l.provider_id,
    l.visit_occurrence_id,
    l.visit_detail_id,
    l.observation_source_value,
    l.observation_source_concept_id,
    l.unit_source_value,
    l.qualifier_source_value,
    l.value_source_concept_id,
    l.value_source_value,
{% if is_rt_dataset or is_ct_dataset %}
    q.research_response_id,
{% elif is_combined_dataset %}
    l.questionnaire_response_id
{% endif %}
FROM `{{project}}.{{lookup_dataset}}.{{lookup_table}}` l
{% if is_ct_dataset or is_rt_dataset %}
JOIN `{{project}}.{{deid_map_dataset_id}}.{{deid_map_table_id}}` d
ON l.person_id = d.person_id
JOIN `{{project}}.{{deid_qrid_dataset_id}}.{{deid_qrid_table_id}}` q
ON l.questionnaire_response_id = q.questionnaire_response_id
{% endif %}
""")


class RemediateBasics(BaseCleaningRule):

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 lookup_dataset_id=None,
                 lookup_table_id=None,
                 deid_map_dataset_id=None,
                 deid_map_table_id=None,
                 deid_qrid_dataset_id=None,
                 deid_qrid_table_id=None,
                 table_namer=None):
        """
        Initialize the class with proper information.

        Set the issue numbers, description and affected datasets. As other tickets may affect
        this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = (
            'Replace missing/meaningless records of the basics with the correct ones.'
        )
        super().__init__(issue_numbers=ISSUE_NUMBERS,
                         description=desc,
                         affected_datasets=[
                             COMBINED, CONTROLLED_TIER_DEID_BASE,
                             CONTROLLED_TIER_DEID_CLEAN,
                             REGISTERED_TIER_DEID_BASE,
                             REGISTERED_TIER_DEID_CLEAN
                         ],
                         affected_tables=[OBSERVATION],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         table_namer=table_namer)

        self.lookup_dataset_id = lookup_dataset_id
        self.lookup_table_id = lookup_table_id
        self.deid_map_dataset_id = deid_map_dataset_id
        self.deid_map_table_id = deid_map_table_id
        self.deid_qrid_dataset_id = deid_qrid_dataset_id
        self.deid_qrid_table_id = deid_qrid_table_id

    def get_query_specs(self):
        """
        Return a list of dictionary query specifications.

        :return:  A list of dictionaries.  Each dictionary contains a
            single query and a specification for how to execute that query.
            The specifications are optional but the query is required.
        """
        sandbox_query = {
            QUERY:
                SANDBOX_MEANINGLESS_SURVEY_RESPONSE_QUERY.render(
                    project=self.project_id,
                    dataset=self.dataset_id,
                    sandbox_dataset=self.sandbox_dataset_id,
                    sandbox_table=self.get_sandbox_tablenames()[0],
                    lookup_dataset=self.lookup_dataset_id,
                    lookup_table=self.lookup_table_id,
                    deid_map_dataset_id=self.deid_map_dataset_id,
                    deid_map_table_id=self.deid_map_table_id,
                    is_combined_dataset=is_combined_dataset(self.dataset_id),
                    is_deid_dataset=is_deid_dataset(self.dataset_id))
        }

        delete_query = {
            QUERY:
                DELETE_MEANINGLESS_SURVEY_RESPONSE_QUERY.render(
                    project=self.project_id,
                    dataset=self.dataset_id,
                    sandbox_dataset=self.sandbox_dataset_id,
                    sandbox_table=self.get_sandbox_tablenames()[0])
        }

        insert_query = {
            QUERY:
                INSERT_CORRECT_SURVEY_RESPONSE_QUERY.render(
                    project=self.project_id,
                    dataset=self.dataset_id,
                    lookup_dataset=self.lookup_dataset_id,
                    lookup_table=self.lookup_table_id,
                    deid_map_dataset_id=self.deid_map_dataset_id,
                    deid_map_table_id=self.deid_map_table_id,
                    deid_qrid_dataset_id=self.deid_qrid_dataset_id,
                    deid_qrid_table_id=self.deid_qrid_table_id,
                    is_combined_dataset=is_combined_dataset(self.dataset_id),
                    is_ct_dataset=is_ct_dataset(self.dataset_id),
                    is_rt_dataset=is_rt_dataset(self.dataset_id))
        }

        return [sandbox_query, delete_query, insert_query]

    def setup_rule(self, client):
        pass

    def setup_validation(self, client):
        raise NotImplementedError("Please fix me.")

    def validate_rule(self):
        raise NotImplementedError("Please fix me.")

    def get_sandbox_tablenames(self):
        return [self.sandbox_table_for(table) for table in self.affected_tables]


if __name__ == '__main__':
    import cdr_cleaner.args_parser as ap
    import cdr_cleaner.clean_cdr_engine as clean_engine

    parser = ap.get_argument_parser()
    parser.add_argument('--lookup_dataset_id',
                        action='store',
                        dest='lookup_dataset_id',
                        help=('Dataset that has the lookup table.'),
                        required=True)
    parser.add_argument(
        '--lookup_table_id',
        action='store',
        dest='lookup_table_id',
        help=
        ('Observation table that has the set of correct responses for the basics.'
        ),
        required=True)
    parser.add_argument('--deid_map_dataset_id',
                        action='store',
                        dest='deid_map_dataset_id',
                        help=('Dataset that has deid mapping table.'),
                        required=True)
    parser.add_argument(
        '--deid_map_table_id',
        action='store',
        dest='deid_map_table_id',
        help=
        ('deid mapping table that has pid-rid association and dateshift values.'
        ),
        required=True)

    ARGS = parser.parse_args()

    clean_engine.add_console_logging(ARGS.console_log)

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id,
            ARGS.dataset_id,
            ARGS.sandbox_dataset_id, [(RemediateBasics,)],
            lookup_dataset_id=ARGS.lookup_dataset_id,
            lookup_table_id=ARGS.lookup_table_id,
            deid_map_dataset_id=ARGS.deid_map_dataset_id,
            deid_map_table_id=ARGS.deid_map_table_id)
        for query_dict in query_list:
            LOGGER.info(query_dict.get(QUERY))
    else:
        clean_engine.clean_dataset(ARGS.project_id,
                                   ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(RemediateBasics,)],
                                   lookup_dataset_id=ARGS.lookup_dataset_id,
                                   lookup_table_id=ARGS.lookup_table_id,
                                   deid_map_dataset_id=ARGS.deid_map_dataset_id,
                                   deid_map_table_id=ARGS.deid_map_table_id)
