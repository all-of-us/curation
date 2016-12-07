import glob
import os
import pandas
import re
from sqlalchemy import create_engine
import settings

DATASPRINT = 'datasprint'
INVALID_FILENAME_HPO_ID = 'Invalid filename: expected hpo_id but found "%s".'
INVALID_FILENAME_TABLE_NAME = 'Invalid filename: expected table name but found "%s".'
INVALID_FILENAME_DATASPRINT = 'Invalid filename: expected "datasprint" but found "%s".'
INVALID_FILENAME_SPRINT_NUMBER = 'Invalid filename: expected sprint number but found "%s".'
INSERT_FMT = "INSERT INTO pmi_sprint_reporter_error (filename, hpo_id, error_message) VALUES (%s, %s, %s)"
engine = create_engine(settings.conn_str)
con = engine.connect()


def script(filename):
    hpo_ids = pandas.read_csv(settings.hpo_csv_path).hpo_id.unique()
    with open(filename) as f:
        text = f.read()
        for hpo_id in hpo_ids:
            cmd_text = text.replace('hpo_schema', hpo_id)
            cmds = cmd_text.split(';')
            for cmd in cmds:
                print cmd
                con.execute(cmd)


def process_dir(d):
    metadata_df = pandas.read_csv(settings.cdm_metadata_path)
    hpo_ids = pandas.read_csv(settings.hpo_csv_path).hpo_id.unique()
    table_names = metadata_df.table_name.unique()

    for f in glob.glob(os.path.join(d, '*.csv')):
        file_path_parts = f.split(os.sep)
        filename = file_path_parts[-1]
        buff = filename.lower()

        curr_hpo_id = None
        curr_table_name = None
        curr_sprint_num = None

        print '\n\n'+filename

        try:
            gen = (hpo_id for hpo_id in sorted(hpo_ids, key=lambda x: len(x), reverse=True) if curr_hpo_id is None)

            # incrementally parse file name
            for hpo_id in gen:
                if buff.startswith(hpo_id):
                    curr_hpo_id = hpo_id
                    buff = buff[len(curr_hpo_id)+1:]
            if not curr_hpo_id:
                raise RuntimeError(INVALID_FILENAME_HPO_ID % buff)

            for table_name in table_names:
                if buff.startswith(table_name):
                    curr_table_name = table_name
                    buff = buff[len(curr_table_name)+1:]
            if not curr_table_name:
                raise RuntimeError(INVALID_FILENAME_TABLE_NAME % buff)

            if buff.startswith(DATASPRINT):
                buff = buff[len(DATASPRINT)+1:]
            else:
                raise RuntimeError(INVALID_FILENAME_DATASPRINT % buff)

            m = re.match('(\d+)\.csv', buff)
            if m:
                curr_sprint_num = int(m.group(1))
            if curr_sprint_num is None:
                raise RuntimeError(INVALID_FILENAME_SPRINT_NUMBER % buff)

            # get column names for this table
            column_names = metadata_df[metadata_df['table_name'] == curr_table_name].column_name.unique()

            # lowercase field names
            df = pandas.read_csv(f).rename(columns=str.lower)

            # add missing columns (with NaN values)
            df = df.reindex(columns=column_names)

            # fill in blank concept_id columns with 0
            concept_columns = filter(lambda x: x.endswith('concept_id') and 'source' not in x, column_names)
            df[concept_columns] = df[concept_columns].fillna(value=0)

            # insert
            df.to_sql(name=curr_table_name, con=con, if_exists='append', index=False, schema=curr_hpo_id)
        except Exception, e:
            print e.message
            insert_stmt = INSERT_FMT
            if curr_hpo_id is not None:
                insert_stmt = INSERT_FMT.replace('pmi_sprint_reporter_error', curr_hpo_id + '.pmi_sprint_reporter_error')
            con.execute(insert_stmt, [filename, curr_hpo_id, e.message])


if __name__ == '__main__':
    process_dir(settings.csv_dir)
    # TODO streamline bootstrap
    #script(os.path.join(settings.resource_path, 'truncate.sql'))
    #script(os.path.join(settings.resource_path, 'cdm.sql'))
