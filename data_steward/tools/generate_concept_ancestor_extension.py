"""
concept_relationship stores two types of relationships 
1) hierarchical relationships: 'is_a' / 'subsume' that defines the hierarchy of the vocabulary;
2) associative relationships:  relationships across the hierarchy such as Drug A
'is an indication of' Condition B. The concept_ancestor table is built based on 'Is A' and 'Subsume' relationships
recursively so any ancestor/descendent pairs (regardless of the levels of separation) are pre-computed for us.

The relationship for LOINC component concepts (e.g. Triglycerides) used to be in the subsumption relationship with lab
concepts (e.g. Triglyceride [Mass or Moles/volume] in Serum or Plasma) in the previous version of vocab, however, the
OMOP vocab team changed this relationship from 'subsume' to 'component of '  to align with the LOINC system. As a
consequence, concept_ancestor missed all of ancestor/descendent relationships involving LOINC component concepts.

This script generates the concept_ancestor_ext table for all the concepts in measurement domain using loinc hierarchy.
"""

import argparse
import logging

from common import CONCEPT_ANCESTOR_EXTENSION
from utils import bq

LOGGER = logging.getLogger(__name__)

CONCEPT_ANCESTOR_EXT_QUERY = '''
DECLARE
  num_of_new_records INT64;
  -- Instantiate concept_ancestor_extension with all LONIC measurement concepts and direct descendant concepts
CREATE OR REPLACE TABLE
  `{project}.{dataset}.{ancestor_extension}` ( ancestor_concept_id INT64,
    descendant_concept_id INT64,
    levels_of_separation INT64 ) AS (
  SELECT
    DISTINCT cr.concept_id_1 AS ancestor_concept_id,
    cr.concept_id_2 AS descendant_concept_id,
    1 AS levels_of_separation
  FROM (
    SELECT
      concept_id AS ancestor_concept_id
    FROM
      `{project}.{dataset}.concept` AS c
    WHERE
      c.vocabulary_id = 'LOINC'
      AND domain_id = 'Measurement' ) AS loinc_ids
  JOIN
    `{project}.{dataset}.concept_relationship` AS cr
  ON
    loinc_ids.ancestor_concept_id = cr.concept_id_1
    AND relationship_id IN ('Subsumes',
      'Component of')
    AND cr.concept_id_1 <> cr.concept_id_2
  JOIN
    `{project}.{dataset}.concept` AS c2
  ON
    cr.concept_id_2 = c2.concept_id
    AND c2.domain_id = 'Measurement' );
LOOP
  CREATE OR REPLACE TEMP TABLE descendants_next_iteration AS (
  SELECT
    DISTINCT cae.ancestor_concept_id,
    cr.concept_id_2 AS descendant_concept_id,
    cae.levels_of_separation + 1 AS levels_of_separation
  FROM
    `{project}.{dataset}.{ancestor_extension}` AS cae
  JOIN
    `{project}.{dataset}.concept_relationship` AS cr
  ON
    cae.descendant_concept_id = cr.concept_id_1
    AND relationship_id IN ('Subsumes',
      'Component of')
    AND cr.concept_id_1 <> cr.concept_id_2
  JOIN
    `{project}.{dataset}.concept` AS c2
  ON
    cr.concept_id_2 = c2.concept_id
    AND c2.domain_id = 'Measurement' );
SET
  num_of_new_records = (
  SELECT
    COUNT(*)
  FROM
    descendants_next_iteration AS cae_new
  LEFT JOIN
    `{project}.{dataset}.{ancestor_extension}` AS cae
  ON
    cae_new.ancestor_concept_id = cae.ancestor_concept_id
    AND cae_new.descendant_concept_id = cae.descendant_concept_id
    AND cae_new.levels_of_separation = cae.levels_of_separation
    AND cae.ancestor_concept_id <> cae_new.descendant_concept_id
  WHERE
    cae.ancestor_concept_id IS NULL );
IF
  num_of_new_records = 0 THEN
LEAVE
  ;
END IF
  ;
INSERT
  `{project}.{dataset}.{ancestor_extension}`
SELECT
  cae_new.*
FROM
  descendants_next_iteration AS cae_new
LEFT JOIN
  `{project}.{dataset}.{ancestor_extension}` AS cae
ON
  cae_new.ancestor_concept_id = cae.ancestor_concept_id
  AND cae_new.descendant_concept_id = cae.descendant_concept_id
  AND cae_new.levels_of_separation = cae.levels_of_separation
  AND cae.ancestor_concept_id <> cae_new.descendant_concept_id
WHERE
  cae.ancestor_concept_id IS NULL;
END LOOP
  ;
CREATE OR REPLACE TABLE
  `{project}.{dataset}.{ancestor_extension}` AS
SELECT
  ancestor_concept_id,
  descendant_concept_id,
  MIN(levels_of_separation) AS min_levels_of_separation,
  MAX(levels_of_separation) AS max_levels_of_separation
FROM
  `{project}.{dataset}.{ancestor_extension}`
GROUP BY
  ancestor_concept_id,
  descendant_concept_id;
'''


def generate_concept_ancestor_extension(project_id, dataset_id):
    """
    generates concept ancestor extension table from the concept relationship table for LOINC hierarchy
    :param project_id: identifier for project id
    :param dataset_id: identifier for dataset
    :return: Bq job result
    """

    client = bq.get_client(project_id)
    query = CONCEPT_ANCESTOR_EXT_QUERY.format(
        project=project_id,
        dataset=dataset_id,
        ancestor_extension=CONCEPT_ANCESTOR_EXTENSION)
    query_job = client.query(query)
    res = query_job.result()
    return res


def get_args_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-p',
        '--project_id',
        dest='project_id',
        action='store',
        help=
        'Identifies the project containing the ehr dataset and lookup dataset',
        required=True)
    parser.add_argument(
        '-d',
        '--dataset_id',
        dest='ehr_dataset_id',
        action='store',
        help=
        'Identifies the dataset where the concept_ancestor_ext table is to be created.',
        required=True)
    return parser


if __name__ == '__main__':
    args_parser = get_args_parser()
    args = args_parser.parse_args()
    concept_ancestor_ext = generate_concept_ancestor_extension(
        args.project_id, args.ehr_dataset_id)
    LOGGER.info(concept_ancestor_ext)
