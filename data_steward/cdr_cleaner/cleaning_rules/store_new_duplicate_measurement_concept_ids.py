"""
Background

when selecting lab results in the cohort browser, there are often indistinguishable results; there are multiple
 “Positives”, “Detected”, etc., and they’re often not next to each other when building queries or viewing counts.
SNOMED has introduced some new concepts for lab results which some sites are using, while others are using the
previously available LOINC concepts. They are not connected in a structured way in the OMOP vocabulary and both are
considered Standard, so there is no easy method for a user to manage this.
In order to improve the user experience, we need to essentially “de-duplicate” the vocabulary we are using in the
“clean” dataset by identifying a set of “All of Us Standard Value Concept IDs” that harmonizes the possible values.
After we make our changes, a user should be able to provide a single value into queries and see a single summary when
grouping by value_as_concept_id for a given concept.

NAACCR duplicated concept ids are ignored in this rule as NAACCR is vocabulary for cancer registry and cannot be
rolled up to standard LOINC concept ids.

These records will be appended to the pipeline_tables.IDENTICAL_LABS_LOOKUP_TABLE table in BigQuery.
Duplicate mappings are not allowed.
"""
# Python imports
import logging

# Project imports
import constants.cdr_cleaner.clean_cdr as cdr_consts
from common import (JINJA_ENV, PIPELINE_TABLES, IDENTICAL_LABS_LOOKUP_TABLE)
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule

LOGGER = logging.getLogger(__name__)

ALL_DUPLICATES_SANDBOX_QUERY = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE
  `{{project_id}}.{{sandbox_dataset_id}}.{{sandbox_table}}` AS (
  WITH
    data_concepts_totals AS(
      -- get total record count for each value_as_concept_id --
    SELECT
      value_as_concept_id,
      COUNT(*) AS n_total_records
    FROM
      `{{project_id}}.{{dataset_id}}.measurement`
    WHERE
      value_as_concept_id IS NOT NULL
      AND value_as_concept_id > 0
    GROUP BY
      1
    ORDER BY
      1 ),
    num_mc_per_vaci AS (
    SELECT
      value_as_concept_id,
      COUNT(DISTINCT measurement_concept_id) AS n_distinct_measurement_concept_ids
    FROM
      `{{project_id}}.{{dataset_id}}.measurement`
    WHERE
      value_as_concept_id IS NOT NULL
      AND value_as_concept_id > 0
    GROUP BY
      1 ),
    data_concept_info AS(
    SELECT
      dc.value_as_concept_id,
      dc.n_total_records,
      mc.n_distinct_measurement_concept_ids,
      c.*
    FROM
      data_concepts_totals dc
    JOIN
      num_mc_per_vaci mc
    USING
      (value_as_concept_id)
    JOIN
      `{{project_id}}.{{dataset_id}}.concept` c
    ON
      value_as_concept_id = concept_id ),
    and_cn_dupes AS(
    SELECT
      dci.value_as_concept_id,
      dci.n_total_records,
      dci.n_distinct_measurement_concept_ids,
      dci.concept_id,
      dci.concept_name AS concept_name_in_table,
      dci.vocabulary_id AS concept_vocab_in_table,
      dci.domain_id,
      c.concept_id AS dupe_concept_id,
      c.concept_name,
      c.vocabulary_id,
      c.domain_id
    FROM
      data_concept_info dci
    JOIN
      `{{project_id}}.{{dataset_id}}.concept` c
    ON
      lower(dci.concept_name) = lower(c.concept_name)
      AND dci.concept_id <> c.concept_id
    ORDER BY
      dci.value_as_concept_id ),
    dedup_values AS (
    SELECT
      DISTINCT dupes.value_as_concept_id,
      dupes.concept_name_in_table AS vac_name,
      dupes.concept_vocab_in_table AS vac_vocab,
      COALESCE(CASE
          WHEN dupes.concept_vocab_in_table NOT IN ('LOINC', 'NAACCR') THEN
           ( SELECT MAX(value_as_concept_id) FROM and_cn_dupes AS t2 WHERE 
           t2.concept_name_in_table = dupes.concept_name_in_table AND t2.concept_vocab_in_table = 'LOINC' )
        ELSE
        dupes.value_as_concept_id
      END
        , dupes.value_as_concept_id) AS aou_standard_vac,
      dupes.n_distinct_measurement_concept_ids AS n_measurement_concept_id,
      dupes.n_total_records AS n_measurement
    FROM
      and_cn_dupes dupes
    WHERE
      dupes.dupe_concept_id IN (
      SELECT
        DISTINCT value_as_concept_id
      FROM
        `{{project_id}}.{{dataset_id}}.measurement`)
    ORDER BY
      dupes.concept_name_in_table DESC,
      dupes.n_total_records DESC,
      dupes.n_distinct_measurement_concept_ids DESC)
  SELECT
    value_as_concept_id,
    vac_name,
    vac_vocab,
    aou_standard_vac,
    n_measurement_concept_id,
    n_measurement,
    CURRENT_DATE() AS date_added
  FROM
    dedup_values )
""")

STORE_NEW_MAPPINGS = JINJA_ENV.from_string("""
INSERT INTO
  `{{project_id}}.{{pipeline_dataset}}.{{primare_lookup_table}}` 
  (value_as_concept_id,
    vac_name,
    vac_vocab,
    aou_standard_vac,
    date_added)
SELECT
  value_as_concept_id,
  vac_name,
  vac_vocab,
  aou_standard_vac,
  date_added
FROM
    `{{project_id}}.{{sandbox_dataset_id}}.{{sandbox_table}}`
WHERE value_as_concept_id not in (
  SELECT value_as_concept_id
  FROM `{{project_id}}.{{pipeline_dataset}}.{{primare_lookup_table}}`)
""")


class StoreNewDuplicateMeasurementConceptIds(BaseCleaningRule):
    """
    Store only new occurrences of duplicate lab measurement values.
    """

    def __init__(self, project_id, dataset_id, sandbox_dataset_id, namer=None):
        desc = (
            f'All new duplicate measurement concept_ids will be identified via SQL and '
            f'stored in a sandbox table.  '
            f'The table will be read to load into the primary pipeline table,'
            f'pipeline_tables.IDENTICAL_LABS_LOOKUP_TABLE.')

        super().__init__(issue_numbers=['DC2716'],
                         description=desc,
                         affected_datasets=[cdr_consts.COMBINED],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         table_namer=namer)

    def get_query_specs(self):
        """
        “de-duplicate” the vocabulary we are using by identifying a set of “All of Us Standard Value Concept IDs”
        that harmonizes the possible values.

        :return: a list of SQL strings to run
        """

        sandbox_query = ALL_DUPLICATES_SANDBOX_QUERY.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            sandbox_dataset_id=self.sandbox_dataset_id,
            sandbox_table=IDENTICAL_LABS_LOOKUP_TABLE)

        insert_query = STORE_NEW_MAPPINGS.render(
            project_id=self.project_id,
            sandbox_dataset_id=self.sandbox_dataset_id,
            pipeline_dataset=PIPELINE_TABLES,
            sandbox_table=IDENTICAL_LABS_LOOKUP_TABLE,
            primare_lookup_table=IDENTICAL_LABS_LOOKUP_TABLE)

        sandbox_query_dict = {cdr_consts.QUERY: sandbox_query}
        insert_query_dict = {cdr_consts.QUERY: insert_query}

        return [sandbox_query_dict, insert_query_dict]

    def get_sandbox_tablenames(self):
        return [IDENTICAL_LABS_LOOKUP_TABLE]

    def setup_rule(self, client):
        pass

    def setup_validation(self):
        pass

    def validate_rule(self):
        pass


if __name__ == '__main__':
    from utils import pipeline_logging

    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.get_argument_parser().parse_args()
    pipeline_logging.configure(level=logging.DEBUG, add_console_handler=True)

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id, ARGS.dataset_id, ARGS.sandbox_dataset_id,
            [(StoreNewDuplicateMeasurementConceptIds,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(StoreNewDuplicateMeasurementConceptIds,)])
