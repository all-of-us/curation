"""
In our internal pipeline, aou_death table has death records while death table 
is empty. We must populate death table using aou_death table when making the 
final outputs (=combined_release and CT/RT base/clean). This script is 
referenced by create_combined_dataset.py and copy_dataset_to_output_prod.py so 
these .py scripts can populate death table before publishing datasets.

Original issue: DC-3208, DC-3209
"""

# Python imports
import logging

# Project imports
from gcloud.bq import BigQueryClient
from common import AOU_DEATH, DEATH, JINJA_ENV
from utils import pipeline_logging

LOGGER = logging.getLogger(__name__)

SELECT_DEATH = JINJA_ENV.from_string("""
SELECT 1 FROM `{{project}}.{{dataset}}.{{death}}`
""")

POPULATE_DEATH = JINJA_ENV.from_string("""
INSERT INTO `{{project}}.{{dataset}}.{{death}}`
SELECT 
    person_id, death_date, death_datetime, death_type_concept_id,
    cause_concept_id, cause_source_value, cause_source_concept_id
FROM `{{project}}.{{dataset}}.{{aou_death}}`
WHERE primary_death_record = True
""")


def death_is_empty(client, project_id, dataset_id) -> bool:
    """
    :return: True if death table is empty. False if not.
    """
    select_death = SELECT_DEATH.render(project=project_id,
                                       dataset=dataset_id,
                                       death=DEATH)
    death_record_count = len(list(client.query(select_death).result()))

    return death_record_count == 0


def populate_death(client, project_id, dataset_id):
    """
    Populate death table with the aou_death records that are primary_death_record==True. 
    It also ensures death table is empty before populating it with the data.
    :return:
    """
    if not death_is_empty(client, project_id, dataset_id):
        raise AssertionError("DEATH table must be empty.")

    populate_death = POPULATE_DEATH.render(project=project_id,
                                           dataset=dataset_id,
                                           death=DEATH,
                                           aou_death=AOU_DEATH)
    query_job = client.query(query=populate_death)
    query_job.result()


if __name__ == '__main__':
    from argparse import ArgumentParser, RawDescriptionHelpFormatter

    pipeline_logging.configure(logging.DEBUG, add_console_handler=True)

    parser = ArgumentParser(description='Parse project_id and dataset_id',
                            formatter_class=RawDescriptionHelpFormatter)
    parser.add_argument('-p',
                        '--project_id',
                        action='store',
                        dest='project_id',
                        help='Project associated with the dataset',
                        required=True)
    parser.add_argument('-d',
                        '--dataset_id',
                        action='store',
                        dest='dataset_id',
                        help='Dataset to populate death table',
                        required=True)

    ARGS = parser.parse_args()
    bq_client = BigQueryClient(ARGS.project_id)
    populate_death(bq_client, ARGS.project_id, ARGS.dataset_id)
