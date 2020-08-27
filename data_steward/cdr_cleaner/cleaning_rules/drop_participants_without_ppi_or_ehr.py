"""
Run the drop_participants_without_ppi_or_ehr validation clean rule.

Drops all data for participants who:
  1. have not completed "The Basics" PPI module, via the RDR
  2. do not have any EHR data

(1) is achieved by checking the observation table for children of TheBasics
module. (2) is achieved by checking all mapping tables for all person_id tables,
to confirm whether any data is sourced from EHR per participant.
"""
from jinja2 import Template
import logging

from cdr_cleaner.cleaning_rules import drop_rows_for_missing_persons
import common
from constants import bq_utils as bq_consts
from constants.cdr_cleaner import clean_cdr as clean_consts
import resources

LOGGER = logging.getLogger(__name__)

BASICS_MODULE_CONCEPT_ID = 1586134

SELECT_PERSON_WITH_BASICS_OR_EHR_TMPL = Template("""
SELECT {{ fields | join(", ") }}
FROM `{{ project }}.{{ dataset }}.person` person
LEFT JOIN (
  SELECT DISTINCT o.person_id
  FROM `{{ project }}.{{ dataset }}.concept_ancestor`
  INNER JOIN `{{ project }}.{{ dataset }}.observation` o ON observation_concept_id = descendant_concept_id
  INNER JOIN `{{ project }}.{{ dataset }}.concept` d ON d.concept_id = descendant_concept_id
  WHERE ancestor_concept_id = {{ basics_module_concept_id }}) basics
ON
  person.person_id = basics.person_id
{% for table, config in mapped_clinical_data_configs.items() %}
  LEFT JOIN (
    SELECT DISTINCT t.person_id AS person_id
    FROM `{{ project }}.{{ dataset }}.{{ table }}` t
    LEFT JOIN `{{ project }}.{{ dataset }}._mapping_{{ table }}` m
    ON t.{{ config["id_column"] }} = m.{{ config["id_column"] }}
    # The source HPO is either the "rdr", or a site ID; we only want to capture sites here.
    WHERE m.src_hpo_id != "rdr"
  ) {{ table }}_ehr
  ON person.person_id = {{ table }}_ehr.person_id
{% endfor %}
WHERE
  basics.person_id IS NOT NULL
  {% for table in mapped_clinical_data_configs.keys() %}
    OR {{ table }}_ehr.person_id IS NOT NULL
  {% endfor %}
""")


def get_queries(project=None, dataset=None):
    """
    Return a list of queries to remove data-poor participant rows.

    The removal criteria is for participants is as follows:
    1. They have not completed "The Basics" PPI module, via the RDR
    2. They do not have any EHR data

    These participants are not particularly useful for analysis, so remove them
    here.

    :return:  A list of string queries that can be executed to delete data-poor
        participants and corresponding rows from the dataset.
    """
    fields = [
        'person.' + field['name'] for field in resources.fields_for('person')
    ]
    mapped_clinical_data_configs = {
        t: {
            'id_column': resources.get_domain_id_field(t)
        } for t in common.MAPPED_CLINICAL_DATA_TABLES
    }

    delete_query = SELECT_PERSON_WITH_BASICS_OR_EHR_TMPL.render(
        fields=fields,
        project=project,
        dataset=dataset,
        basics_module_concept_id=BASICS_MODULE_CONCEPT_ID,
        mapped_clinical_data_configs=mapped_clinical_data_configs)

    # drop from the person table, then delete all corresponding data for the now missing persons
    return [{
        clean_consts.QUERY: delete_query,
        clean_consts.DESTINATION_TABLE: 'person',
        clean_consts.DESTINATION_DATASET: dataset,
        clean_consts.DISPOSITION: bq_consts.WRITE_TRUNCATE,
    }] + drop_rows_for_missing_persons.get_queries(project, dataset)


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(ARGS.project_id,
                                                 ARGS.dataset_id,
                                                 [(get_queries,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   [(get_queries,)])
