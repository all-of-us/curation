"""
Module to run abbreviated version of OHDSI Achilles
"""

import os

from run_config import hpo_ids, multi_schema_supported, engine
import resources

achilles_sql_path = os.path.join(resources.resource_path, 'achilles.sql')
achilles_heel_sql_path = os.path.join(resources.resource_path, 'achilles_heel.sql')


def sql_render(hpo_id, cdm_schema, results_schema, vocab_schema, sql_text):
    """
    Replace template parameters
    :param hpo_id: will be the source name in Achilles report
    :param cdm_schema: schema of the cdm
    :param results_schema: schema of the results tables
    :param vocab_schema: schema of the vocabulary tables
    :param sql_text: SQL command text to render
    :return: command text with template parameters replaced
    """
    result = sql_text

    replacements = {'@cdm_database_schema': cdm_schema,
                    '@results_database_schema': results_schema,
                    '@vocab_database_schema': vocab_schema}

    for raw_placeholder, schema in replacements.items():
        placeholder = raw_placeholder + '.' if schema is None else raw_placeholder
        replace_with = '' if schema is None else schema
        result = result.replace(placeholder, replace_with)

    return result.replace('@source_name', hpo_id)


def run_achilles(hpo_id):
    """
    Run achilles for the specified HPO
    :param hpo_id: id for the HPO
    """
    cdm_schema = hpo_id if multi_schema_supported else None
    results_schema = hpo_id if multi_schema_supported else None
    vocab_schema = None

    sql_paths = [achilles_sql_path, achilles_heel_sql_path]
    for sql_path in sql_paths:
        with open(sql_path) as achilles_sql_file:
            sql_text = achilles_sql_file.read()
            t = sql_render(hpo_id, cdm_schema, results_schema, vocab_schema, sql_text)

            # run in batches to prevent parser errors regarding temp tables (see http://stackoverflow.com/a/4217017)
            batches = map(lambda s: s.strip(), t.split('--}'))
            conn = engine.connect().execution_options(autocommit=True)
            for batch in batches:
                conn.execute(batch)


def main():
    for hpo_id in hpo_ids:
        print 'Processing %s...' % hpo_id
        run_achilles(hpo_id)


if __name__ == '__main__':
    main()
