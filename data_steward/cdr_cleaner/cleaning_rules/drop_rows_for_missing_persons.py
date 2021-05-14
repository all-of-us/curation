import common
from constants.cdr_cleaner import clean_cdr as clean_consts

NON_PID_TABLES = [
    common.CARE_SITE, common.LOCATION, common.FACT_RELATIONSHIP, common.PROVIDER
]

TABLES_TO_DELETE_FROM = set(common.AOU_REQUIRED +
                            [common.OBSERVATION_PERIOD]) - set(NON_PID_TABLES)

# Select rows where the person_id is in the person table
DELETE_NON_EXISTING_PERSON_IDS = common.JINJA_ENV.from_string("""
DELETE
FROM `{project}.{dataset}.{table}`
WHERE person_id NOT IN
(SELECT person_id
FROM `{project}.{dataset}.person`)
""")


def get_queries(project=None, dataset=None, sandbox_dataset_id=None):
    """
    Return a list of queries to remove data for missing persons.

    Removes data from person_id linked tables for any persons which do not
    exist in the person table.
    :param sandbox_dataset_id: Identifies the sandbox dataset to store rows
    #TODO use sandbox_dataset_id for CR

    :return:  A list of string queries that can be executed to delete data from
        other tables for non-person users.
    """
    query_list = []
    for table in TABLES_TO_DELETE_FROM:
        delete_query = DELETE_NON_EXISTING_PERSON_IDS.render(project=project,
                                                             dataset=dataset,
                                                             table=table)

        query_list.append({clean_consts.QUERY: delete_query})

    return query_list
