"""
Run the drop_participants_without_ppi_or_ehr validation clean rule.

Drops all data for participants who:
  1. have not completed "The Basics" PPI module, via the RDR
  2. do not have any EHR data

(1) is achieved by checking the observation table for children of TheBasics
module. (2) is achieved by checking all mapping tables for all person_id tables,
to confirm whether any data is sourced from EHR per participant.
"""
import logging

import common
import resources
from cdr_cleaner.cleaning_rules import drop_rows_for_missing_persons
from constants.cdr_cleaner import clean_cdr as clean_consts

LOGGER = logging.getLogger(__name__)

DELETE_PERSON_WITH_NO_BASICS = common.JINJA_ENV.from_string("""
DELETE
FROM `{{project}}.{{dataset}}.person` person
WHERE person_id NOT IN
(SELECT
    person_id
  FROM `{{project}}.{{dataset}}.concept_ancestor`
  INNER JOIN `{{project}}.{{dataset}}.observation` o ON observation_concept_id = descendant_concept_id
  INNER JOIN `{{project}}.{{dataset}}.concept` d ON d.concept_id = descendant_concept_id
  WHERE ancestor_concept_id = 1586134

  UNION DISTINCT

  SELECT
    person_id
  FROM `{{project}}.{{dataset}}.concept`
  JOIN `{{project}}.{{dataset}}.concept_ancestor`
    ON (concept_id = ancestor_concept_id)
  JOIN `{{project}}.{{dataset}}.observation`
    ON (descendant_concept_id = observation_concept_id)
  JOIN `{{project}}.{{dataset}}._mapping_observation`
    USING (observation_id)
  WHERE concept_class_id = 'Module'
    AND concept_name IN ('The Basics')
    AND src_hpo_id = 'rdr'
    AND questionnaire_response_id IS NOT NULL)
""")

DELETE_PERSON_WITH_NO_EHR = common.JINJA_ENV.from_string("""
DELETE
FROM `{{project}}.{{dataset}}.person` person
WHERE person_id NOT IN (
{% for table, config in mapped_clinical_data_configs.items() %}
  SELECT
    person_id
  FROM `{{project}}.{{dataset}}.{{table}}` t
  JOIN `{{project}}.{{dataset}}._mapping_{{table}}` m
  USING ({{table}}_id)
  -- The source HPO is either the "rdr", or a site ID; we only want to capture sites here. --
  WHERE m.src_hpo_id != "rdr"
  {% if loop.nextitem is defined %}UNION DISTINCT{% endif %}
{% endfor %}
)
""")


def get_queries(project=None, dataset=None, sandbox_dataset_id=None):
    """
    Return a list of queries to remove data-poor participant rows.

    The removal criteria is for participants is as follows:
    1. They have not completed "The Basics" PPI module, via the RDR
    2. They do not have any EHR data

    These participants are not particularly useful for analysis, so remove them
    here.
    :param sandbox_dataset_id: Identifies the sandbox dataset to store rows
    #TODO use sandbox_dataset_id for CR

    :return:  A list of string queries that can be executed to delete data-poor
        participants and corresponding rows from the dataset.
    """
    mapped_clinical_data_configs = {
        t: {
            'id_column': resources.get_domain_id_field(t)
        } for t in common.MAPPED_CLINICAL_DATA_TABLES
    }

    delete_no_basics_query = DELETE_PERSON_WITH_NO_BASICS.render(
        project=project, dataset=dataset)

    delete_no_ehr_query = DELETE_PERSON_WITH_NO_EHR.render(
        project=project,
        dataset=dataset,
        mapped_clinical_data_configs=mapped_clinical_data_configs)

    # drop from the person table, then delete all corresponding data for the now missing persons
    return [{
        clean_consts.QUERY: delete_no_basics_query
    }, {
        clean_consts.QUERY: delete_no_ehr_query
    }] + drop_rows_for_missing_persons.get_queries(project, dataset)


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(ARGS.project_id,
                                                 ARGS.dataset_id,
                                                 ARGS.sandbox_dataset_id,
                                                 [(get_queries,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id, [(get_queries,)])
