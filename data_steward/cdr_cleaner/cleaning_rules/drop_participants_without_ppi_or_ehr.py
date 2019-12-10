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

from cdr_cleaner.cleaning_rules import drop_rows_for_missing_persons
import common
from constants import bq_utils as bq_consts
from constants.cdr_cleaner import clean_cdr as clean_consts
import resources

LOGGER = logging.getLogger(__name__)

BASICS_MODULE_CONCEPT_ID = 1586134

SELECT_PERSON_WITH_BASICS_OR_EHR = """
SELECT {fields}
FROM `{project}.{dataset}.{table}` person
LEFT JOIN (
  SELECT DISTINCT o.person_id
  FROM `{project}.{dataset}.concept_ancestor`
  INNER JOIN `{project}.{dataset}.observation` o ON observation_concept_id = descendant_concept_id
  INNER JOIN `{project}.{dataset}.concept` d ON d.concept_id = descendant_concept_id
  WHERE ancestor_concept_id = {basics_module_concept_id}) basics
ON
  person.person_id = basics.person_id
{ehr_joins}
WHERE
  basics.person_id IS NOT NULL
  OR ({has_ehr_predicate})
"""

# Template for a JOIN against the person table against a table which may contain
# EHR data.
JOIN_EHR_PERSON_IDS_TEMPLATE = """
LEFT JOIN (
  SELECT DISTINCT v.person_id AS person_id
  FROM `{project}.{dataset}.{table}` t
  LEFT JOIN `{project}.{dataset}._mapping_{table}` m ON at.{id_column} = m.{id_column}
  # The source HPO is either the "rdr", or a site ID; we only want to capture sites here.
  WHERE m.src_hpo_id != "rdr"
) {table}_ehr
ON
  person.person_id = {table}_ehr.person_id
"""

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
    field_names = ['entry.' + field['name'] for field in resources.fields_for('person')]
    fields = ', '.join(field_names)

    ehr_joins = ""
    for t in common.MAPPED_VALIDATION_TABLES:
        ehr_joins += JOIN_EHR_PERSON_IDS_TEMPLATE.format(
            project=project, dataset=dataset, table=t,
            id_column=resources.get_domain_id_field(t))
    has_ehr_predicate = ' OR '.join(
        ['{}.person_id IS NOT NULL'.format(t) for t in MAPPED_VALIDATION_TABLES])

    delete_query = SELECT_EXISTING_PERSON_IDS.format(
        project=project, dataset=dataset, table=table, fields=fields,
        ehr_joins=ehr_joins, has_ehr_predicate=has_ehr_predicate,
        basics_module_concept_id=BASICS_MODULE_CONCEPT_ID
    )

    # drop from the person table, then delete all corresponding data for the now missing persons
    return [{
        clean_consts.QUERY: delete_query,
        clean_consts.DESTINATION_TABLE: 'person',
        clean_consts.DESTINATION_DATASET: dataset,
        clean_consts.DISPOSITION: bq_consts.WRITE_TRUNCATE,
    }] + drop_rows_for_missing_persons.get_queries(project, dataset)
