"""
    Health Information Privacy Lab
    Brad. Malin, Weiyi Xia, Steve L. Nyemba

This is a factory that handles rules generated into a canonical form and sends
them to a deid engine for processing
"""
import sqlite3

from google.cloud import bigquery as bq
import pandas as pd


class Meta(object):

    def sqlite(self, **args):
        """
        Will load a csv/json file with the type information of a table

        :param path:   path of the sqlite file
        :param table:  name of the table

        :return:  a list of fields
        """
        path = args['path']
        table = args['table']
        conn = sqlite3.connect(path)
        sql = " ".join(["SELECT * FROM ", table, "LIMIT 10"])
        df = pd.read_sql_query(sql, conn)
        return df.columns.tolist()

    def postgresql(self, **args):
        return None

    def bigquery(self, **args):
        """
        Get information about a bigquery table.

        This function will return the meta data associated with a table in a
        bigquery dataset

        :param path:   path of the key file
        :param table:  name of the table
        :param project:  name of the bigquery project
        :param dataset:  name of the dataset

        :return:  table meta data
        """
        table_name = args.get('table')
        private_key = args.get('path')
        project_id = args.get('project_id')
        dataset_id = args.get('dataset_id')
        schema = args.get('schema', dataset_id)

        fq_dataset_id = project_id + '.' + dataset_id
        client = bq.Client.from_service_account_json(private_key)
        dataset_ref = client.get_dataset(fq_dataset_id)
        tables = client.list_tables(dataset_ref)
        tables = [table for table in tables if table.table_id == table_name]
        return tables[0] if tables else None

    def instance(self, **args):
        """
        :path   path of the db (for sqlite), JSON service account file (bigquery)
        :table  name of the table
        :schema optional if the persistance store supports it
        """
        store = args['store']
        return getattr(Meta, store)(**args)
