import argparse
import logging

import app_identity

import bq_utils
import constants.bq_utils as bq_consts

OBSERVATION_TABLE_NAME = 'observation'
VOCABULARY_DATASET = 'vocabulary20190423'
PMI_SKIP_FIX_QUERY = \
    ('SELECT'
     '  coalesce(obs.observation_id,'
     '    ques.observation_id) AS observation_id,'
     '  coalesce(obs.person_id,'
     '    ques.person_id) AS person_id,'
     '  coalesce(obs.observation_concept_id,'
     '    ques.observation_concept_id) AS observation_concept_id,'
     '  coalesce(obs.observation_date,'
     '    ques.default_observation_date) AS observation_date,'
     '  coalesce(obs.observation_datetime,'
     '    ques.default_observation_datetime) AS observation_datetime,'
     '  coalesce(obs.observation_type_concept_id,'
     '    ques.observation_type_concept_id) AS observation_type_concept_id,'
     '  value_as_number,'
     '  value_as_string,'
     '  coalesce(obs.value_as_concept_id,'
     '    903096) AS value_as_concept_id,'
     '  coalesce(obs.qualifier_concept_id,'
     '    0) AS qualifier_concept_id,'
     '  coalesce(obs.unit_concept_id,'
     '    0) AS unit_concept_id,'
     '  provider_id,'
     '  visit_occurrence_id,'
     '  coalesce(obs.observation_source_value,'
     '    ques.observation_source_value) AS observation_source_value,'
     '  coalesce(obs.observation_source_concept_id,'
     '    ques.observation_source_concept_id) AS observation_source_concept_id,'
     '  unit_source_value,'
     '  qualifier_source_value,'
     '  coalesce(obs.value_source_concept_id,'
     '    903096) AS value_source_concept_id,'
     'case '
     'when obs.value_source_concept_id = 903096 Then \'PMI_Skip\' '
     'when value_source_concept_id = 903096 Then \'PMI_Skip\' '
     'else obs.value_source_value '
     'end as value_source_value, '
     '  questionnaire_response_id '
     'FROM'
     '  `{project}.{dataset}.observation` AS obs'
     ' FULL OUTER JOIN ('
     '  WITH'
     '    per AS ('
     '    SELECT'
     '      DISTINCT obs.person_id,'
     '      per.gender_concept_id'
     '    FROM'
     '      `{project}.{dataset}.observation` obs'
     '    JOIN'
     '      `{project}.{dataset}.person` AS per'
     '    ON'
     '      obs.person_id = per.person_id'
     '    WHERE'
     '      observation_source_concept_id IN (1586135, 1586140, 1585838, 1585899, 1585940, 1585892, 1585889,'
     '     1585890, 1585386, 1585389, 1585952, 1585375, 1585370, 1585879, 1585886, 1585857, 1586166, 1586174,'
     '     1586182, 1586190, 1586198, 1585636, 1585766, 1585772, 1585778, 1585711, 1585717, 1585723, 1585729,'
     '     1585735, 1585741, 1585747, 1585748, 1585754, 1585760, 1585803, 1585815, 1585784)'
     '      AND observation_date < \'2018-04-10\' ),'
     '    obs AS ('
     '    SELECT'
     '      DISTINCT observation_concept_id,'
     '      observation_source_concept_id,'
     '      observation_source_value,'
     '      observation_type_concept_id'
     '    FROM'
     '      `{project}.{dataset}.observation`'
     '    WHERE'
     '      observation_source_concept_id IN (1586135, 1586140, 1585838, 1585899, 1585940, 1585892, 1585889,'
     '     1585890, 1585386, 1585389, 1585952, 1585375, 1585370, 1585879, 1585886, 1585857, 1586166, 1586174,'
     '     1586182, 1586190, 1586198, 1585636, 1585766, 1585772, 1585778, 1585711, 1585717, 1585723, 1585729,'
     '     1585735, 1585741, 1585747, 1585748, 1585754, 1585760, 1585803, 1585815, 1585784)'
     '      AND observation_date < \'2018-04-10\'),'
     '    dte AS ('
     '    SELECT'
     '      person_id,'
     '      MAX(observation_date) AS default_observation_date,'
     '      MAX(observation_datetime) AS default_observation_datetime'
     '    FROM'
     '      `{project}.{dataset}.observation`'
     '    WHERE'
     '      observation_source_concept_id IN (1586135, 1586140, 1585838, 1585899, 1585940, 1585892, 1585889,'
     '     1585890, 1585386, 1585389, 1585952, 1585375, 1585370, 1585879, 1585886, 1585857, 1586166, 1586174,'
     '     1586182, 1586190, 1586198, 1585636, 1585766, 1585772, 1585778, 1585711, 1585717, 1585723, 1585729,'
     '     1585735, 1585741, 1585747, 1585748, 1585754, 1585760, 1585803, 1585815, 1585784)'
     '      AND observation_date < \'2018-04-10\''
     '    GROUP BY'
     '      person_id)'
     '  SELECT'
     '    ROW_NUMBER() OVER() + 1000000000000 AS observation_id,'
     '    cartesian.*,'
     '    dte.default_observation_date,'
     '    dte.default_observation_datetime'
     '  FROM ('
     '    SELECT'
     '      per.person_id,'
     '      observation_concept_id,'
     '      observation_source_concept_id,'
     '      observation_source_value,'
     '      observation_type_concept_id'
     '    FROM'
     '      per,'
     '      obs'
     '    WHERE'
     '      (observation_source_concept_id != 1585784)'
     '      OR (per.gender_concept_id = 8532'
     '        AND observation_source_concept_id = 1585784) '
     '      ) cartesian'
     '  JOIN'
     '    dte'
     '  ON'
     '    cartesian.person_id = dte.person_id'
     '  ORDER BY'
     '    cartesian.person_id ) AS ques'
     ' ON'
     '  obs.person_id = ques.person_id'
     '  AND obs.observation_concept_id = ques.observation_concept_id')

UPDATE_PPI_QUERY = (
    'UPDATE '
    '  `{project}.{dataset}.observation` a '
    'SET '
    '  a.value_as_concept_id = b.concept_id '
    'FROM ( '
    '  SELECT '
    '    * '
    '  FROM ( '
    '    SELECT '
    '      c2.concept_name, '
    '      c2.concept_id, '
    '      o.*, '
    '      RANK() OVER (PARTITION BY o.observation_id, o.value_source_concept_id ORDER BY c2.concept_id ASC) AS rank '
    '    FROM '
    '      `{project}.{dataset}.observation` o '
    '    JOIN '
    '      `{project}.{ehr_dataset}.concept_relationship` cr '
    '    ON '
    '      o.value_source_concept_id = cr.concept_id_1 '
    '      AND cr.relationship_id = \'Maps to value\' '
    '    JOIN '
    '      `{project}.{ehr_dataset}.concept` c2 '
    '    ON '
    '      c2.concept_id = cr.concept_id_2 ) AS x '
    '  WHERE '
    '    rank=1 ) AS b '
    'WHERE '
    '  a.observation_id = b.observation_id'
)


def run_pmi_fix(project_id, dataset_id):
    """

    runs the query which adds skipped rows in survey before 2019-04-10 as PMI_Skip

    :param project_id: Name of the project
    :param dataset_id: Name of the dataset where the queries should be run

    :return:
    """
    if project_id is None:
        project_id = app_identity.get_application_id()

    q = PMI_SKIP_FIX_QUERY.format(project=project_id, dataset=dataset_id)
    logging.debug('Query for PMI_Skip fix is {q}'.format(q=q))
    bq_utils.query(q=q, destination_table_id=OBSERVATION_TABLE_NAME, destination_dataset_id=dataset_id,
                   write_disposition=bq_consts.WRITE_TRUNCATE)


def run_ppi_vocab_update(project_id, dataset_id):
    """
    runs the query which updates the ppi vocabulary in observation table

    :param project_id: Name of the project
    :param dataset_id: Name of the dataset where the queries should be run
    :return:
    """
    if project_id is None:
        project_id = app_identity.get_application_id()

    q = UPDATE_PPI_QUERY.format(project=project_id, dataset=dataset_id, ehr_dataset=bq_utils.get_dataset_id())
    logging.debug('Query for PMI_Skip fix is {q}'.format(q=q))
    bq_utils.query(q=q)


def main(project_id, dataset_id):
    """

    :param project_id: Name of the project
    :param dataset_id: Name of the dataset where the queries should be run
    :return:
    """

    logging.info('Applying PPi Vocabulary update')
    run_ppi_vocab_update(project_id, dataset_id)

    logging.info('Applying PMI_Skip fix')
    run_pmi_fix(project_id, dataset_id)


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
