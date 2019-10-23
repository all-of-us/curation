import argparse
import logging

from google.appengine.api.app_identity import app_identity

import bq_utils

CLEAN_PPI_NUMERIC_FIELDS = """
UPDATE
  {project}.{dataset}.observation u1
SET
  u1.value_as_number = NULL,
  u1.value_as_concept_id = 2000000010
FROM
  (
  SELECT
  *
FROM
  {project}.{dataset}.observation
WHERE
  observation_concept_id = 1585889 AND (value_as_number < 0 OR value_as_number > 20)

UNION ALL

SELECT
  *
FROM
  {project}.{dataset}.observation
WHERE
  observation_concept_id = 1585890 AND (value_as_number < 0 OR value_as_number > 20)

UNION ALL

SELECT
  *
FROM
  {project}.{dataset}.observation
WHERE
  observation_concept_id = 1585795 AND (value_as_number < 0 OR value_as_number > 99)

UNION ALL

SELECT
  *
FROM
  {project}.{dataset}.observation
WHERE
  observation_concept_id = 1585802 AND (value_as_number < 0 OR value_as_number > 99)

UNION ALL

SELECT
  *
FROM
  {project}.{dataset}.observation
WHERE
  observation_concept_id = 1585820 AND (value_as_number < 0 OR value_as_number > 255)

UNION ALL

SELECT
  *
FROM
  {project}.{dataset}.observation
WHERE
  observation_concept_id = 1585864 AND (value_as_number < 0 OR value_as_number > 99)

UNION ALL

SELECT
  *
FROM
  {project}.{dataset}.observation
WHERE
  observation_concept_id = 1585870 AND (value_as_number < 0 OR value_as_number > 99)

UNION ALL

SELECT
  *
FROM
  {project}.{dataset}.observation
WHERE
  observation_concept_id = 1585873 AND (value_as_number < 0 OR value_as_number > 99)

UNION ALL

SELECT
  *
FROM
  {project}.{dataset}.observation
WHERE
  observation_concept_id = 1586159 AND (value_as_number < 0 OR value_as_number > 99)

UNION ALL

SELECT
  *
FROM
  {project}.{dataset}.observation
WHERE
  observation_concept_id = 1586162 AND (value_as_number < 0 OR value_as_number > 99) ) a
WHERE
  u1.observation_id = a.observation_id
"""

REMOVE_ADDITIONAL_RESPONSES_OTHER_THAN_NOT = """
DELETE
FROM
  `{project}.{dataset}.observation`
WHERE
  person_id IN (
  SELECT
    person_id
  FROM
    `{project}.{dataset}.observation`
  WHERE
    observation_concept_id = 1586140
    AND value_source_concept_id = 1586148)
  AND (observation_concept_id = 1586140
    AND value_source_concept_id != 1586148)
"""




def clean_ppi_numeric_fields_using_parameters(project_id, dataset_id):
    """
    Applies value range restrictions given to the value_as_number field across the entire dataset.
    For values which do not meet range criteria, sets value_as_number to NULL and set value_as_concept_id and
    observation_type_concept_id to a new AoU custom concept (2000000010)

    :param project_id: Name of the project
    :param dataset_id: Name of the dataset where the queries should be run
    :return:
    """
    if project_id is None:
        project_id = app_identity.get_application_id()

    q = CLEAN_PPI_NUMERIC_FIELDS.format(project=project_id,
                                        dataset=dataset_id
                                        )
    logging.debug('Query to clean numeric free text values is {q}'.format(q=q))
    bq_utils.query(q=q)


def remove_additional_responses(project_id, dataset_id):
    """
    identifies all participants who have a value_source_concept_id = 1586148.
    For these participants, drops any additional rows for this observation_source_concept_id
    (i.e. all participants should have ONLY one row and that row should have value_source_concept_id = 1586148)
    :param project_id: Name of the project
    :param dataset_id: Name of the dataset where the queries should be run
    :return:
    """

    if project_id is None:
        project_id = app_identity.get_application_id()

    q = REMOVE_ADDITIONAL_RESPONSES_OTHER_THAN_NOT.format(project=project_id,
                                                          dataset=dataset_id)
    logging.debug('Query for removing_additional_responses is {q}'.format(q=q))
    bq_utils.query(q=q)


def main(project_id, dataset_id):
    """

    :param project_id: Name of the project
    :param dataset_id: Name of the dataset where the queries should be run
    :return:
    """

    logging.info('Applying clean ppi numeric value ranges fix')
    clean_ppi_numeric_fields_using_parameters(project_id, dataset_id)

    logging.info('Applying Removing additional responses for sex/gender fix')
    remove_additional_responses(project_id, dataset_id)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-p', '--project_id',
                        action='store', dest='project_id',
                        help='Identifies the project to fix the data in.',
                        required=True)
    parser.add_argument('-d', '--dataset_id',
                        action='store', dest='dataset_id',
                        help='Identifies the dataset to apply the fix on.',
                        required=True)
    args = parser.parse_args()
    if args.dataset_id:
        main(args.project_id, args.dataset_id)
