import logging

from utils import bq

LOGGER = logging.getLogger(__name__)

NEW_PERSON_QUERY = """
SELECT
    p.* EXCEPT (state_of_residence_concept_id, state_of_residence_source_value),
    ext.state_of_residence_concept_id,
    ext.state_of_residence_source_value
FROM
    {{person.project}}.{{person.dataset_id}}.{{person.table_id}} p
LEFT JOIN
    {{person_ext.project}}.{{person_ext.dataset_id}}.{{person_ext.table_id}} ext
USING
    (person_id)
"""

NEW_PERSON_QUERY_TMPL = bq.JINJA_ENV.from_string(NEW_PERSON_QUERY)


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

    # update person schema
    person_ext_schema = person_ext.schema
    person_schema = person.schema
    new_person_schema = person_schema[:]  # Creates a copy of the schema.
    for schema_field in person_ext_schema:
        if schema_field.name == 'state_of_residence_concept_id' or schema_field.name == 'state_of_residence_source_value':
            new_person_schema.append(schema_field)

    person.schema = new_person_schema
    person = client.update_table(person, ["schema"])
    LOGGER.info(f'Updated person table schema to {person.schema}')

    query_job = client.query(query=bq.get_create_or_replace_table_ddl(
        project_id=project_id,
        dataset_id=dataset_id,
        table_id='person',
        schema=person.schema,
        as_query=NEW_PERSON_QUERY_TMPL.render(person=person,
                                              person_ext=person_ext)),)
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
