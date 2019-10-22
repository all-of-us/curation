"""
    This class applies rules and meta data to yield a certain outpout
"""
import codecs
from datetime import datetime
import json
import logging
import os

import pandas as pd
import numpy as np

from rules import Deid

class Press(object):

    def __init__(self, **args):
        """
        :rules_path  to the rule configuration file
        :info_path   path to the configuration of how the rules get applied
        :pipeline   operations and associated sequence in which they should be performed
        """
        with codecs.open(args['rules'], 'r', encoding='utf-8') as config:
            self.deid_rules = json.loads(config.read(), encoding='utf-8')

        self.pipeline = args['pipeline']
        try:
            with codecs.open(args['table'], 'r', encoding='utf-8') as config:
                self.info = json.loads(config.read(), encoding='utf-8')
        except StandardError:
            # In case a table name is not provided, we will apply default rules on he table
            #   I.e physical field suppression and row filter
            #   Date Shifting
            self.info = {}

        if isinstance(self.deid_rules, list):
            cache = {}
            for row in self.deid_rules:
                _id = row['_id']
                cache[_id] = row
            self.deid_rules = cache

        self.idataset = args['idataset']
        self.tablename = args['table']

        self.tablename = os.path.basename(self.tablename).split('.json')[0].strip()

        self.store = 'sqlite' if 'store' not in args else args['store']

        if 'suppress' not in self.deid_rules:
            self.deid_rules['suppress'] = {'FILTERS': []}

        if 'FILTERS' not in self.deid_rules['suppress']:
            self.deid_rules['suppress']['FILTERS'] = []

        self.logpath = args.get('logs', 'logs')
        self.action = [term.strip()
                       for term in args['action'].split(',')] if 'action' in args else ['submit']

        output_log_path = os.path.join(self.logpath, self.idataset)
        try:
            os.makedirs(output_log_path)
        except OSError:
            # directory already exists.  move on.
            pass

        name = datetime.now().strftime('deid-%Y-%m-%d.log')
        filename = os.path.join(self.logpath, name)
        logging.basicConfig(filename=filename, level=logging.INFO, format='%(message)s')


    def meta(self, data_frame):
        return pd.DataFrame(
            {"names": list(data_frame.dtypes.to_dict().keys()), "types": list(data_frame.dtypes.to_dict().values())}
        )

    def initialize(self, **args):
        #
        # Let us update and see if the default filters apply at all
        dfilters = []
        columns = self.get(limit=1).columns.tolist()
        for row in self.deid_rules['suppress']['FILTERS']:
            if set(columns) & set(row['filter'].split(' ')):
                dfilters.append(row)
        self.deid_rules['suppress']['FILTERS'] = dfilters

    def get(self, **args):
        """
        This function will execute an SQL statement and return the meta data for a given table
        """
        return None

    def submit(self, **args):
        """
        Should be overridden by subclasses.
        """
        raise NotImplementedError()

    def update_rules(self):
        """
        Should be overridden by subclasses.
        """
        raise NotImplementedError()

    def do(self):
        """
        This function actually runs deid and using both rule specifications and application of the rules
        """
        self.update_rules()
        d = Deid(pipeline=self.pipeline, rules=self.deid_rules, parent=self)
        _info = self.info

        p = d.apply(_info, self.store, self.get_tablename())

        is_meta = np.sum([1*('on' in _item) for _item in p]) != 0
        self.log(module='do', action='table-type', table=self.get_tablename(), is_meta=int(is_meta))
        if not is_meta:
            sql = self.to_sql(p)
            _rsql = None
        else:
            #
            # Processing meta tables
            sql = []
            relational_cols = [col for col in p if 'on' not in col]
            meta_cols = [col for col in p if 'on' in col]
            _map = {}
            for col in meta_cols:
                if col['on'] not in _map:
                    _map[col['on']] = []
                _map[col['on']] += [col]
            fillter = []
            for filter_id in _map:

                _item = _map[filter_id]
                fillter.append(filter_id)

                _sql = self.to_sql(_item + relational_cols)  + ' AND ' + filter_id

                sql.append(_sql)

            _rsql = self.to_sql(relational_cols) + ' AND ' + ' AND '.join(fillter).replace(' IN ', ' NOT IN ')
            _rsql = _rsql.replace(' exists ', ' NOT EXISTS ')
            _rsql = _rsql.replace(' not NOT ', ' NOT ')
            #
            # @TODO: filters may need to be adjusted (add a conditional statement)
            #

            sql.append(_rsql)

            for index, segment in enumerate(sql):
                formatted = segment.replace(':idataset', self.idataset)
                sql[index] = formatted.replace(':join_tablename', self.tablename)

        if 'debug' in self.action:
            self.debug(p)
        else:
            # write SQL to file
            sql_filepath = os.path.join(self.logpath, self.idataset, self.tablename + '.sql')
            with open(sql_filepath, 'w') as sql_file:
                final_sql = "\n\nAppend these results to previous results\n\n".join(sql)
                sql_file.write(final_sql)

            if 'submit' in self.action:
                for index, statement in enumerate(sql):
                    self.submit(statement, not index)

            if 'simulate' in self.action:
                #
                # Make this threaded if there is a submit action that is associated with it
                self.simulate(p)

    def get_tablename(self):
        return self.idataset + "." + self.tablename if self.idataset else self.tablename

    def debug(self, info):
        for row in info:
            print()
            print(row['label'], not row['apply'])
            print()

    def log(self, **args):
        logging.info(json.dumps(args))

    def simulate(self, info):
        """
        This function will attempt to log the various transformations on every field.

        This will simulate and provide output on possible transformations.
        :info   payload of that has all the transformations applied to a given table as follows
                [{apply, label, name}] where
                    - apply is the SQL to be applied
                    - label is the flag for the operation (generalize, suppress, compute, shift)
                    - name  is the attribute name on which the rule gets applied
        """
        table_name = self.idataset + "." + self.tablename
        suppression_filters = self.deid_rules['suppress']['FILTERS']
        out = pd.DataFrame()
        counts = {}
        dirty_date = False
        filters = []

        for item in info:
            labels = item['label'].split('.')

            if not (set(labels) & set(self.pipeline)):
                self.log(module='simulate', table=table_name, action='skip', value=labels)
                continue

            if labels[0] not in counts:
                counts[labels[0].strip()] = 0

            counts[labels[0].strip()] += 1

            if 'suppress' in labels or item['name'] == 'person_id' or dirty_date:
                continue

            field = item['name']
            alias = 'original_' + field
            sql_list = ["SELECT DISTINCT ", field, 'AS ', alias, ",", item['apply'], " FROM ", table_name]

            if suppression_filters:
                sql_list.append('WHERE')

                for row in suppression_filters:
                    sql_list.append(row['filter'])

                    if suppression_filters.index(row) < len(suppression_filters) -1:
                        sql_list.append('AND')

            if 'on' in item:
                # This applies to meta tables
                filters.append(item['on'])
                if suppression_filters:
                    sql_list.extend(['AND', item['on']])
                else:
                    sql_list.extend(['WHERE ', item['on']])

            if 'shift' in labels:
                data_frame = self.get(sql=" ".join(sql_list).replace(':idataset', self.idataset), limit=5)
            else:
                data_frame = self.get(sql=" ".join(sql_list).replace(':idataset', self.idataset))

            if data_frame.shape[0] == 0:
                self.log(module="simulate", table=table_name, attribute=field, type=item['label'], status='no data-found')
                continue

            data_frame.columns = ['original', 'transformed']
            data_frame['attribute'] = field
            data_frame['task'] = item['label'].upper().replace('.', ' ')
            out = out.append(data_frame)
        #-- Let's evaluate row suppression here
        #
        out.index = range(out.shape[0])
        rdf = pd.DataFrame()
        if suppression_filters:
            filters += [item['filter'] for item in suppression_filters if 'filter' in item]
            original_sql = ' (SELECT COUNT(*) as original FROM :table) AS ORIGINAL_TABLE ,'
            original_sql = original_sql.replace(':table', table_name)
            transformed_sql = '(SELECT COUNT(*) AS transformed FROM :table WHERE :filter) AS TRANSF_TABLE'
            transformed_sql = transformed_sql.replace(':table', table_name)
            transformed_sql = transformed_sql.replace(':filter', " OR ".join(filters))
            sql_list = ['SELECT * FROM ', original_sql, transformed_sql]

            r = self.get(sql=" ".join(sql_list).replace(":idataset", self.idataset))
            table_name = self.idataset + "." + self.tablename

            rdf = pd.DataFrame({"operation": ["row-suppression"], "count": r.transformed.tolist()})

        now = datetime.now()
        flag = "-".join(np.array([now.year, now.month, now.day, now.hour]).astype(str).tolist())

        root = os.path.join(self.logpath, self.idataset, flag)
        try:
            os.makedirs(root)
        except OSError:
            # directory already exists.  move on.
            pass

        stats = pd.DataFrame({"operation": counts.keys(), "count": counts.values()})
        stats = stats.append(rdf)
        stats.index = range(stats.shape[0])
        stats.reset_index()

        _map = {os.path.join(root, 'samples-' + self.tablename + '.csv'): out,
                os.path.join(root, 'stats-' + self.tablename + '.csv'): stats}
        for path in _map:
            _data_frame = _map[path]
            _data_frame.to_csv(path, encoding='utf-8')

        self.log(module='simulation', table=table_name, status='completed', value=root)

    def to_sql(self, info):
        """
        :info   payload with information of the process to be performed
        """
        table_name = self.get_tablename()
        fields = self.get(limit=1).columns.tolist()
        columns = list(fields)
        sql_list = []
        self.log(module='to_sql', action='generating-sql', table=table_name, fields=fields)
        #
        # @NOTE:
        #   If we are dealing with a meta-table we should
        for rule_id in self.pipeline: #['generalize', 'suppress', 'shift', 'compute']:
            for row in info:
                name = row['name']

                if rule_id not in row['label'] or name not in fields:
                    continue

                index = fields.index(name)
                fields[index] = row['apply']
                self.log(module='to_sql', field=name, sql=row['apply'])

        sql_list = ['SELECT', ",".join(fields), 'FROM ', table_name]

        if 'suppress' in self.deid_rules and 'FILTERS' in self.deid_rules['suppress']:
            suppression_filters = self.deid_rules['suppress']['FILTERS']
            if suppression_filters:
                sql_list.append('WHERE')
            for row in suppression_filters:
                if not (set(columns) & set(row['filter'].split(' '))):
                    continue

                sql_list.append(row['filter'])
                if suppression_filters.index(row) < len(suppression_filters) - 1:
                    sql_list.append('AND')

        return '\t'.join(sql_list).replace(":idataset", self.idataset)
