import argparse
import logging

import common
from utils import bq
from utils import pipeline_logging

LOGGER = logging.getLogger(__name__)

COPE_ANCESTOR_TABLE = 'concept_ancestor_cope'
COPE_CONCEPTS_TABLE = 'cope_concepts'

COPE_CONCEPT_QUERY = common.JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE
  `{{project_id}}.{{pipeline_tables_dataset}}.{{cope_concepts_table}}` AS
SELECT
  *
FROM
  `{{project_id}}.{{vocabulary_dataset}}.concept`
WHERE
  concept_id IN(
  SELECT
    descendant_concept_id
  FROM
    `{{project_id}}.{{pipeline_tables_dataset}}.{{concept_ancestor_for_cope}}`)
""")

COPE_DESCENDANT_QUERY = common.JINJA_ENV.from_string("""
DECLARE
  num_of_new_records INT64;
CREATE OR REPLACE TABLE
  `{{project_id}}.{{pipeline_tables_dataset}}.{{concept_ancestor_for_cope}}` ( ancestor_concept_id INT64,
    descendant_concept_id INT64,
    levels_of_separation INT64 );
INSERT
  `{{project_id}}.{{pipeline_tables_dataset}}.{{concept_ancestor_for_cope}}`
SELECT
  DISTINCT cr.concept_id_1 AS ancestor_concept_id,
  cr.concept_id_1 AS descendant_concept_id,
  0 AS levels_of_separation
FROM
  `{{project_id}}.{{vocabulary_dataset}}.concept_relationship` AS cr
WHERE
  cr.concept_id_1 = 1333342
  AND cr.relationship_id = 'PPI parent code of' ;
LOOP
  CREATE OR REPLACE TEMP TABLE descendants_next_iteration AS (
  SELECT
    cae.ancestor_concept_id,
    cr.concept_id_2 AS descendant_concept_id,
    cae.levels_of_separation + 1 AS levels_of_separation
  FROM
    `{{project_id}}.{{pipeline_tables_dataset}}.concept_ancestor_cope` AS cae
  JOIN
    `{{project_id}}.{{vocabulary_dataset}}.concept_relationship` AS cr
  ON
    cae.descendant_concept_id = cr.concept_id_1
    AND relationship_id = 'PPI parent code of' );
SET
  num_of_new_records = (
  SELECT
    COUNT(*)
  FROM
    descendants_next_iteration AS cae_new
  LEFT JOIN
    `{{project_id}}.{{pipeline_tables_dataset}}.{{concept_ancestor_for_cope}}` AS cae
  ON
    cae_new.ancestor_concept_id = cae.ancestor_concept_id
    AND cae_new.descendant_concept_id = cae.descendant_concept_id
    AND cae_new.levels_of_separation = cae.levels_of_separation
  WHERE
    cae.ancestor_concept_id IS NULL );
IF
  num_of_new_records = 0 THEN
LEAVE
  ;
END IF
  ;
INSERT
  `{{project_id}}.{{pipeline_tables_dataset}}.{{concept_ancestor_for_cope}}`
SELECT
  cae_new.*
FROM
  descendants_next_iteration AS cae_new
LEFT JOIN
  `{{project_id}}.{{pipeline_tables_dataset}}.{{concept_ancestor_for_cope}}` AS cae
ON
  cae_new.ancestor_concept_id = cae.ancestor_concept_id
  AND cae_new.descendant_concept_id = cae.descendant_concept_id
  AND cae_new.levels_of_separation = cae.levels_of_separation
WHERE
  cae.ancestor_concept_id IS NULL;
END LOOP
  ;
CREATE OR REPLACE TABLE
  `{{project_id}}.{{pipeline_tables_dataset}}.{{concept_ancestor_for_cope}}` AS
SELECT
  ancestor_concept_id,
  descendant_concept_id,
  MIN(levels_of_separation) AS min_levels_of_separation,
  MAX(levels_of_separation) AS max_levels_of_separation
FROM
  `{{project_id}}.{{pipeline_tables_dataset}}.{{concept_ancestor_for_cope}}`
GROUP BY
  ancestor_concept_id,
  descendant_concept_id;
""")


def update_cope_concepts(project_id, pipeline_dataset_id, vocabulary_dataset):
    """

    :param project_id:
    :param pipeline_dataset_id:
    :param vocabulary_dataset:
    :return:
    """
    client = bq.get_client(project_id)

    queries = []
    results = []
    # recreate concept_ancestor_cope table
    queries.append(
        COPE_DESCENDANT_QUERY.render(
            project_id=project_id,
            pipeline_tables_dataset=pipeline_dataset_id,
            vocabulary_dataset=vocabulary_dataset,
            concept_ancestor_for_cope=COPE_ANCESTOR_TABLE))

    # repopulate Cope concepts table
    queries.append(
        COPE_CONCEPT_QUERY.render(
            project_id=project_id,
            pipeline_tables_dataset=pipeline_dataset_id,
            vocabulary_dataset=vocabulary_dataset,
            cope_concepts_table=COPE_CONCEPTS_TABLE,
            concept_ancestor_for_cope=COPE_ANCESTOR_TABLE))

    for q in queries:
        LOGGER.info(f'Running query -- {q}')
        query_job = client.query(q)
        if query_job.errors:
            raise RuntimeError(
                f"Job {query_job.job_id} failed with error {query_job.errors} for query"
                f"{q}")
        else:
            LOGGER.info(f'{query_job.result()}')


def get_args_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-p',
        '--project_id',
        dest='project_id',
        action='store',
        help='Identifies the project containing the pipeline tables dataset',
        required=True)
    parser.add_argument('-d',
                        '--pipeline_dataset',
                        dest='pipeline_dataset',
                        action='store',
                        help='Identifies the pipeline tables dataset',
                        required=True)
    parser.add_argument('-v',
                        '--vocabulary_dataset',
                        dest='vocabulary_dataset',
                        action='store',
                        help='Identifies the latest vocabulary dataset',
                        required=True)

    return parser


if __name__ == '__main__':
    pipeline_logging.configure(logging.INFO, add_console_handler=True)
    args_parser = get_args_parser()
    args = args_parser.parse_args()
    update_cope_concepts(args.project_id, args.pipeline_dataset,
                         args.vocabulary_dataset)
