"""
Utilities to configure WebAPI (backend for Atlas) to work with the database(s) loaded by reporter and achilles.

This module makes the following assumptions:
 * WebAPI section of settings is valid
 * The WebAPI database (referred to by `settings.webapi_conn_str`) already contains the application tables
"""

from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import create_engine

import run_config
import settings

engine = create_engine(settings.webapi_conn_str)
metadata = MetaData(bind=engine, reflect=True)
source_table = Table('source', metadata, autoload=True)
source_daimon_table = Table('source_daimon', metadata, autoload=True)


def delete_sources():
    """
    Remove all records from source and source_daimon tables
    """
    delete_source_daimon = source_daimon_table.delete()
    delete_source = source_table.delete()
    engine.execute(delete_source_daimon)
    engine.execute(delete_source)


def create_source(hpo_id, hpo_name):
    """
    Insert source and source_daimon records associated with an HPO
    :param hpo_id: ID of the HPO (see hpo.csv)
    :param hpo_name: Name of the HPO (see hpo.csv)
    """
    source_row = dict(SOURCE_NAME=hpo_name,
                      SOURCE_KEY=hpo_id,
                      SOURCE_CONNECTION=settings.cdm_jdbc_conn_str,
                      SOURCE_DIALECT=run_config.cdm_dialect)
    insert_source = source_table.insert().returning(source_table.c.SOURCE_ID).values(source_row)
    source_id = engine.execute(insert_source).lastrowid

    cdm_daimon_row = dict(source_id=source_id, daimon_type=0, table_qualifier=hpo_id, priority=1)
    vocab_daimon_row = dict(source_id=source_id, daimon_type=1, table_qualifier='dbo', priority=1)
    results_daimon_row = dict(source_id=source_id, daimon_type=2, table_qualifier=hpo_id, priority=1)
    source_daimon_rows = [cdm_daimon_row, vocab_daimon_row, results_daimon_row]
    insert_source_daimon = source_daimon_table.insert().values(source_daimon_rows)
    engine.execute(insert_source_daimon)


def main():
    delete_sources()
    for hpo in run_config.all_hpos.to_dict(orient='records'):
        create_source(hpo['hpo_id'], hpo['name'])


if __name__ == '__main__':
    main()
