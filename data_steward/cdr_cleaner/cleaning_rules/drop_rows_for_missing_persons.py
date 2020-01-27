import common
from constants.cdr_cleaner import clean_cdr as clean_consts
from constants import bq_utils as bq_consts
import resources

# Select rows where the person_id is in the person table
SELECT_EXISTING_PERSON_IDS = (
    'SELECT {fields} FROM `{project}.{dataset}.{table}` AS entry '
    'JOIN `{project}.{dataset}.person` AS person '
    'ON entry.person_id = person.person_id')


def get_queries(project=None, dataset=None):
    """
    Return a list of queries to remove data for missing persons.

    Removes data from person_id linked tables for any persons which do not
    exist in the person table.

    :return:  A list of string queries that can be executed to delete data from
        other tables for non-person users.
    """
    query_list = []
    for table in common.CLINICAL_DATA_TABLES:
        field_names = [
            'entry.' + field['name'] for field in resources.fields_for(table)
        ]
        fields = ', '.join(field_names)

        delete_query = SELECT_EXISTING_PERSON_IDS.format(project=project,
                                                         dataset=dataset,
                                                         table=table,
                                                         fields=fields)

        query_list.append({
            clean_consts.QUERY: delete_query,
            clean_consts.DESTINATION_TABLE: table,
            clean_consts.DESTINATION_DATASET: dataset,
            clean_consts.DISPOSITION: bq_consts.WRITE_TRUNCATE,
        })

    return query_list
