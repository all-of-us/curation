"""
Module to run abbreviated version of OHDSI Achilles
"""

import os
from sqlalchemy import text

from run_config import hpo_ids, multi_schema_supported, engine
import resources

achilles_sql_path = os.path.join(resources.resource_path, 'achilles.sql')


def sql_render(hpo_id, schema, sql_text):
    """
    Replace template parameters
    :param hpo_id: will be the source name in Achilles report
    :param schema: name to qualify tables (if supported)
    :param sql_text: SQL command text to render
    :return: command text with template parameters replaced
    """
    result = ''
    if schema is None:
        qualifiers_to_replace = ['@results_database_schema.', '@cdm_database_schema.']
        replace_with = ''
    else:
        qualifiers_to_replace = ['@results_database_schema', '@cdm_database_schema']
        replace_with = schema

    for qualifier in qualifiers_to_replace:
        result = sql_text.replace(qualifier, replace_with)

    return result.replace('@source_name', hpo_id)


def run_achilles(hpo_id):
    """
    Run achilles for the specified HPO
    :param hpo_id: id for the HPO
    """
    results_database_schema = hpo_id if multi_schema_supported else None

    with open(achilles_sql_path) as achilles_sql_file:
        sql_text = achilles_sql_file.read()
        t = sql_render(hpo_id, results_database_schema, sql_text)

        # run in batches to prevent parser errors regarding temp tables (see http://stackoverflow.com/a/4217017)
        batches = map(lambda s: text(s.strip()), t.split('--}'))
        conn = engine.connect().execution_options(autocommit=True)
        for batch in batches:
            conn.execute(batch)


def main():
    for hpo_id in hpo_ids:
        run_achilles(hpo_id)


if __name__ == '__main__':
    main()
