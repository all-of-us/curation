import argparse
import logging

from google.appengine.api.app_identity import app_identity

import bq_utils

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
