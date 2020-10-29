# Python imports
import logging

# Project imports
from utils import bq
from common import JINJA_ENV

LOGGER = logging.getLogger(__name__)

ADD_COLUMNS_QUERY = JINJA_ENV.from_string("""
ALTER TABLE `{{person.project}}.{{person.dataset_id}}.{{person.table_id}}`
ADD COLUMN IF NOT EXISTS state_of_residence_concept_id INT64 
    OPTIONS(description="[All of Us OMOP extension] A foreign key for the state of residence"),
ADD COLUMN IF NOT EXISTS state_of_residence_source_value STRING 
    OPTIONS(description="[All of Us OMOP extension] The source code for the state of residence")
""")

UPDATE_PERSON_QUERY = JINJA_ENV.from_string("""
UPDATE `{{person.project}}.{{person.dataset_id}}.{{person.table_id}}` p
SET
    state_of_residence_concept_id = ext.state_of_residence_concept_id,
    state_of_residence_source_value = ext.state_of_residence_source_value
FROM
    `{{person_ext.project}}.{{person_ext.dataset_id}}.{{person_ext.table_id}}` ext
WHERE p.person_id = ext.person_id
""")


def update_schema(client, person):
    add_cols_query = ADD_COLUMNS_QUERY.render(person=person)
    query_job = client.query(query=add_cols_query)
    query_job.result()
    person = client.get_table(person)
    LOGGER.info(f'Updated person table schema to {person.schema}')


def update_person(client, project_id, dataset_id):
    """
    Populates person table with two additional columns from the ext table

    :param client: bigquery client
    :param project_id: identifies the project
    :param dataset_id: identifies the dataset
    :return:
    """
    person_table_id = f'{project_id}.{dataset_id}.person'
    person_ext_table_id = f'{project_id}.{dataset_id}.person_ext'
    person = client.get_table(person_table_id)
    person_ext = client.get_table(person_ext_table_id)

    update_schema(client, person)

    update_person_query = UPDATE_PERSON_QUERY.render(person=person,
                                                     person_ext=person_ext)
    query_job = client.query(query=update_person_query)
    query_job.result()


if __name__ == '__main__':
    from argparse import ArgumentParser, RawDescriptionHelpFormatter

    parser = ArgumentParser(description='Parse project_id and dataset_id',
                            formatter_class=RawDescriptionHelpFormatter)
    parser.add_argument('-p',
                        '--project_id',
                        action='store',
                        dest='project_id',
                        help='Project associated with the input dataset',
                        required=True)
    parser.add_argument('-d',
                        '--dataset_id',
                        action='store',
                        dest='dataset_id',
                        help='Dataset to modify person table',
                        required=True)

    ARGS = parser.parse_args()
    bq_client = bq.get_client(ARGS.project_id)
    update_person(bq_client, ARGS.project_id, ARGS.dataset_id)
