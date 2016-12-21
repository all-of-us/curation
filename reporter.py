import os
import pandas
import glob

from sqlalchemy import Date, Float, BigInteger, String
from sqlalchemy import Table, Column
from sqlalchemy import create_engine, MetaData

import settings
import resources

INSERT_FMT = "INSERT INTO pmi_sprint_reporter_log (filename, message) VALUES (%s, %s)"
engine = create_engine(settings.conn_str)
con = engine.connect()


def drop_tables(schema):
    metadata = MetaData(bind=engine, reflect=True, schema=schema)
    metadata.drop_all()


def create_tables(schema):
    metadata = MetaData()
    cdm_df = pandas.read_csv(resources.cdm_csv_path)
    tables = cdm_df.groupby(['table_name'])

    for table_name, table_df in tables:
        columns = []
        for index, (_, column_name, is_nullable, data_type, _) in table_df.iterrows():
            if data_type in ('character varying', 'text'):
                tpe = String(100)
            elif data_type == 'integer':
                tpe = BigInteger()
            elif data_type == 'numeric':
                tpe = Float()
            elif data_type == 'date':
                tpe = Date()
            else:
                raise NotImplementedError('Unexpected data_type `%s` in cdm.csv' % data_type)
            nullable = is_nullable == 'yes'
            columns.append(Column(column_name, tpe, nullable=nullable))
        Table(table_name, metadata, *columns, schema=schema)

    Table('pmi_sprint_reporter_log',
          metadata,
          Column('log_id', BigInteger, primary_key=True, nullable=False, autoincrement=True),
          Column('message', String(500), nullable=False),
          Column('filename', String(100)),
          schema=schema)

    metadata.create_all(engine)


def process(hpo_id, schema):
    sprint_num = settings.sprint_num
    cdm_df = pandas.read_csv(resources.cdm_csv_path)
    included_tables = pandas.read_csv(resources.included_tables_csv_path).table_name.unique()
    tables = cdm_df[cdm_df['table_name'].isin(included_tables)].groupby(['table_name'])

    # allow files to be found regardless of CaSe
    def path_to_file_map_item(p):
        file_path_parts = p.split(os.sep)
        filename = file_path_parts[-1]
        return filename.lower(), p

    file_map_items = map(path_to_file_map_item, glob.glob(os.path.join(settings.csv_dir, '*.csv')))
    file_map = dict(file_map_items)

    for table_name, table_df in tables:
        csv_filename = '%(hpo_id)s_%(table_name)s_datasprint_%(sprint_num)s.csv' % locals()
        csv_path = os.path.join(settings.csv_dir, csv_filename)

        try:
            if csv_filename not in file_map:
                raise Exception('File `%s` not found' % csv_filename)

            csv_path = file_map[csv_filename]

            # get column names for this table
            column_names = table_df.column_name.unique()

            with open(csv_path) as f:
                # lowercase field names
                df = pandas.read_csv(f).rename(columns=str.lower)

                # add missing columns (with NaN values)
                df = df.reindex(columns=column_names)

                # fill in blank concept_id columns with 0
                concept_columns = filter(lambda x: x.endswith('concept_id') and 'source' not in x, column_names)
                df[concept_columns] = df[concept_columns].fillna(value=0)

                # insert
                df.to_sql(name=table_name, con=con, if_exists='append', index=False, schema=schema)

        except Exception, e:
            print e.message
            insert_fmt = INSERT_FMT
            if schema is not None:
                insert_fmt = INSERT_FMT.replace('pmi_sprint_reporter_log', hpo_id + '.' + 'pmi_sprint_reporter_log')
            con.execute(insert_fmt, [csv_filename, e.message])


def start():
    all_hpo_ids = pandas.read_csv(resources.hpo_csv_path).hpo_id.unique()
    multi_schema_supported = engine.dialect.name in ['mssql', 'postgresql', 'oracle']

    if settings.hpo_id == 'all':
        if not multi_schema_supported:
            raise Exception('Cannot run all. Multiple schemas not supported configured engine.')
        hpo_ids = all_hpo_ids
    else:
        if settings.hpo_id.lower() not in all_hpo_ids:
            raise RuntimeError('%s not a valid hpo_id' % settings.hpo_id)
        hpo_ids = [settings.hpo_id]

    if len(hpo_ids) > 1 and not multi_schema_supported:
        raise Exception('Cannot process. Multiple schemas not supported by configured engine.')

    for hpo_id in hpo_ids:
        schema = hpo_id if multi_schema_supported else None
        drop_tables(schema=schema)
        create_tables(schema=schema)
        process(hpo_id, schema=schema)


if __name__ == '__main__':
    start()
