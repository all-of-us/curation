"""
A utility to add concept_ids that need to be suppressed in DEID, to the _concept_ids_suppression lookup table

If collecting concept_ids by query, add query as a global variable below and add the variable to the queries list in
get_concepts_query

If collecting concept_ids by file, upload file to `data_steward/deid/config/internal_tables`
"""
# Python Imports
import logging

# Third party imports
import pandas as pd
from jinja2 import Environment

LOGGER = logging.getLogger(__name__)
LOGS_PATH = '../logs'

jinja_env = Environment(
    # help protect against cross-site scripting vulnerabilities
    autoescape=True,
    # block tags on their own lines
    # will not cause extra white space
    trim_blocks=True,
    lstrip_blocks=True,
    # syntax highlighting should be better
    # with these comment delimiters
    comment_start_string='--',
    comment_end_string=' --')

COVID_CONCEPT_IDS_QUERY = """
SELECT
  vocabulary_id, concept_code, concept_name, concept_id, domain_id
FROM
  `{{input_dataset}}.concept_ancestor` ca
JOIN
  `{{input_dataset}}.concept` c
ON
  ca.descendant_concept_id = c.concept_id
WHERE
  ca.ancestor_concept_id IN (756055, 4100065, 37311061, 439676, 37311060, 45763724)
"""


def get_additional_concepts_query(input_dataset, client):
    """
    function to collect and run queries to populate the _concept_ids_suppression lookup table

    :param input_dataset: input dataset where lookup table will live
    :param client: BQ client
    :return: dataframe of results from query
    """

    # Add queries from above to queries list, to append return values to _concept_ids_suppression lookup table
    queries = [COVID_CONCEPT_IDS_QUERY]
    concept_id_df = pd.DataFrame()
    LOGGER.info("Checking for additional concept_ids to be suppressed")
    for q in queries:
        concept_id_df = concept_id_df.append(
            client.query(
                jinja_env.from_string(q).render(
                    input_dataset=input_dataset)).to_dataframe())

    LOGGER.info(
        f"Adding {len(concept_id_df.index)} rows to dataframe to create _concept_ids_suppression lookup table"
    )
    return concept_id_df