"""
    This class applies rules and meta data to yield a certain outpout
"""
import pandas as pd
import numpy as np
import json
from rules import deid
from datetime import datetime
import os
import logging

class Press:
    """
    This class
    """
    def __init__(self,**args):
        """
        :rules_path  to the rule configuration file
        :info_path   path to the configuration of how the rules get applied
        :pipeline   operations and associated sequence in which they should be performed
        """
        self.deid_rules = json.loads((open (args['rules'])).read())
        self.pipeline = args['pipeline']
        if os.path.exists(args['table']) :
            self.info = json.loads((open (args['table'])).read())
        else:
            #
            # In case a table name is not provided, we will apply default rules on he table
            #   I.e physical field suppression and row filter
            #   Date Shifting
            self.info = {}
        if type(self.deid_rules) == list :
            cache = {}
            for row in self.deid_rules :
                _id = row['_id']
                cache[_id] = row
            self.deid_rules = cache
        self.idataset       = args['idataset']
        self.tablename      = args['table']
        if os.sep in self.tablename :
            self.tablename = self.tablename.split(os.sep)[-1].replace('.json','').strip()
        self.store = 'sqlite' if 'store' not in args else args['store']
        if 'suppress' not in self.deid_rules :
            self.deid_rules['suppress'] = {'FILTERS':[]}
        if 'FILTERS' not in self.deid_rules['suppress'] :
            self.deid_rules['suppress']['FILTERS'] = []
            
        self.logpath = 'logs' if 'logs' not in args else args['logs']        
        self.action = [term.strip() for term in args['action'].split(',')] if 'action' in args else ['submit']
        
        #
        #--
        if os.path.exists(self.logpath) == False :
            os.mkdir(self.logpath)
        if os.path.exists(self.logpath+os.sep+self.idataset) == False :
            os.mkdir(self.logpath+os.sep+self.idataset) 
        name = datetime.now().strftime('deid-%Y-%m-%d.log')
        filename = os.sep.join([self.logpath,name])
        logging.basicConfig(filename=filename,level=logging.INFO,format='%(message)s')            
        
       
    def meta(self,df):
        return pd.DataFrame( {"names":list(df.dtypes.to_dict().keys()), "types":list(df.dtypes.to_dict().values())})

    def initialize(self,**args):
        #
        # Let us update and see if the default filters apply at all
        dfilters = []
        columns = self.get(limit=1).columns.tolist()
        for row in self.deid_rules['suppress']['FILTERS'] :
            if set(columns) & set(row['filter'].split(' ')):
                dfilters.append(row)
        self.deid_rules['suppress']['FILTERS'] = dfilters
        pass
    def get(self,**args):
        """
        This function will execute an SQL statement and return the meta data for a given table
        """
        return None
    def do(self):
        """
        This function actually runs deid and using both rule specifications and application of the rules
        """
        self.update_rules()
        d = deid(pipeline = self.pipeline,rules=self.deid_rules,parent=self)
        # d.cache = self.deid_rules
        # d.deid_rules = d.cache 

        # _info = [item for item in self.info['generalize'] if self.tablename in item['table'] ]
        # _info = {"generalize":_info}
        # _info['compute'] = [ {"rules":"@compute.id","fields":["research_id"],"table":"person","key_field":"person_id","value_field":"person_id"}]
        _info = self.info

        p = d.apply(_info,self.store)
        is_meta = np.sum([ 1*('on' in _item) for _item in p]) != 0
        self.log(module='do',action='table-type',table=self.get_tablename(),is_meta= int(is_meta))
        if not is_meta :
            
            sql = self.to_sql(p)
            _rsql = None
        else:
            #
            # Processing meta tables
            sql = []
            relational_cols   = [col for col in p if 'on' not in col]
            meta_cols  = [col for col in p if 'on' in col]
            _map = {}
            for col in meta_cols :
                if col['on'] not in _map :
                    _map[col['on']] = []
                _map[col['on']] += [col]
            filter = []
            CONJUNCTION = ' AND ' if self.deid_rules['suppress']['FILTERS'] else ' WHERE '
            for filter_id in _map :

                _item = _map[filter_id]
                # if _item['on'] not in filter :
                    # filter += [_item['on'].replace(' IN ','NOT IN')]
                filter += [filter_id]

                # _sql = self.to_sql([_item]+ relational_cols ) + CONJUNCTION +_item['on']
                # _sql = self.to_sql(_item +relational_cols) + CONJUNCTION +filter_id
                
                _sql = self.to_sql(_item +relational_cols)  + ' AND ' +filter_id
                
                sql += [ _sql]
                # self.deid_rules['suppress']['FILTERS'] = self.deid_rules['suppress']['FILTERS'][:-1]
            #-- end of loop
            # if ' AND ' in CONJUNCTION :
            #     _rsql = self.to_sql(relational_cols) + ' AND ' + ' AND '.join(filter).replace(' IN ',' NOT IN ')
            # else:
            #     _rsql = self.to_sql(relational_cols) + ' WHERE ' + ' '.join(filter).replace(' IN ',' NOT IN ')
            _rsql = self.to_sql(relational_cols) + ' AND ' + ' AND '.join(filter).replace(' IN ',' NOT IN ')
            #
            # @TODO: filters may need to be adjusted (add a conditional statement)
            #

            # _rsql = _rsql +" AND "+ " AND ".join(filter).replace(' IN ',' NOT IN ')
            sql += [_rsql]
            sql = "\nUNION ALL\n".join(sql)

        # fields = self.get(limit  = 1).columns.tolist()

        if 'debug' in self.action :
            self.debug(p)
        else:

            f = open(os.sep.join([self.logpath,self.idataset,self.tablename+".sql"]),'w')
            f.write(sql)
            f.close()
            

            if 'submit' in self.action :
                self.submit(sql)
            if 'simulate' in self.action:
                #
                # Make this threaded if there is a submit action that is associated with it
                self.simulate(p)

    def get_tablename(self):
        return self.idataset+"."+self.tablename if self.idataset else self.tablename
    def debug(self,info):
        TABLE_NAME = self.idataset+"."+self.tablename
        for row in info :
            print()
            print(row['label'], not row['apply'])
            print()

    def log (self,**args):
            # print (args)       
            logging.info(json.dumps(args) )
    def simulate(self,info):
        """
        This function is not essential, but will attempt to log the various transformations on every field.
        This is because the testing team is incapable of applying adequate testing techniques and defining appropriate equivalence classes.
        :info   payload of that has all the transformations applied to a given table as follows
                [{apply,label,name}] where
                    - apply is the SQL to be applied
                    - label is the flag for the operation (generalize, suppress, compute, shift)
                    - name  is the attribute name that on which the rule gets applied
        """
        TABLE_NAME = self.idataset+"."+self.tablename
        # if 'suppress' in self.deid_rules and 'FILTERS' in self.deid_rules['suppress']:
        FILTERS = self.deid_rules['suppress']['FILTERS']
        # el?se:
            # FITLERS = []
        out = pd.DataFrame()
        counts = {}
        dirty_date = False
        filters = []
        for item in info :
            
            labels = item['label'].split('.')
            if not (set(labels) & set(self.pipeline)) :
                self.log(module='simulate',table=TABLE_NAME,action='skip',value=labels)
                continue
            if labels[0] not in counts :
                counts [ labels[0].strip()]  = 0
            counts [ labels[0].strip()]  += 1
            if 'suppress' in labels or item['name'] == 'person_id' or dirty_date == True:

                continue
            field = item['name']
            alias = 'original_' + field
            SQL = ["SELECT DISTINCT ",field,'AS ',alias,",",item['apply'], " FROM ",TABLE_NAME]
            if FILTERS :
                SQL += ['WHERE']

                for row in FILTERS :
                    SQL += [row['filter']]
                    if FILTERS.index(row) < len(FILTERS) -1 :

                        SQL += ['AND']
            if 'on' in item :
                #
                # This applies to meta tables

                filters += [item['on']] #.replace(' IN ',' NOT IN ')]
                SQL += ['AND' , item['on']] if FILTERS else ['WHERE ',item['on']]
            if 'shift' in labels:
                df = self.get(sql = " ".join(SQL).replace(':idataset',self.idataset),limit=5)
            else:
                df = self.get(sql = " ".join(SQL).replace(':idataset',self.idataset))
            if df.shape[0] == 0 :
                self.log(module="simulate",table=TABLE_NAME,attribute=field,type=item['label'],status='no data-found')
                # print '\n\t'.join(SQL).replace(':idataset',self.idataset)
                continue
            if 'shift' in item['label'] and df.shape[0] > 0 :
                dirty= True
            df.columns = ['original','transformed']
            df['attribute'] = field
            df['task'] = item['label'].upper().replace('.', ' ')
            out = out.append(df)
        #-- Let's evaluate row suppression here
        #
        out.index = range(out.shape[0])
        rdf = pd.DataFrame()
        
        if FILTERS :
            filters += [item['filter'] for item in FILTERS if 'filter' in item]
            original_sql        = ' (SELECT COUNT(*) as original FROM :table) AS ORIGINAL_TABLE ,'.replace(':table',TABLE_NAME)
            transformed_sql     = '(SELECT COUNT(*) AS transformed FROM :table WHERE :filter) AS TRANSF_TABLE'.replace(':table',TABLE_NAME).replace(':filter'," OR ".join(filters))
            SQL = ['SELECT * FROM ',original_sql,transformed_sql]

            r = self.get(sql = " ".join(SQL).replace(":idataset",self.idataset))
            TABLE_NAME = self.idataset+"."+self.tablename

            # df['attribute'] = ' * '
            rdf = pd.DataFrame({"operation":["row-suppression"],"count":r.transformed.tolist() })

        #
        # We
        now = datetime.now()
        flag = "-".join(np.array([now.year,now.month,now.day,now.hour]).astype(str).tolist())
        for folder in [self.logpath, os.sep.join([self.logpath,self.idataset]),os.sep.join([self.logpath,self.idataset,flag]) ] :
            if not os.path.exists(folder) :
                os.mkdir(folder)
        root = os.sep.join([self.logpath,self.idataset,flag])
        stats = pd.DataFrame({"operation":counts.keys(),"count":counts.values()})
        stats = stats.append(rdf)
        stats.index = range(stats.shape[0])
        stats.reset_index()
        _map = {os.sep.join([root,'samples-'+self.tablename+'.csv']):out, os.sep.join([root,'stats-'+self.tablename+'.csv']):stats}
        for path in _map :
            _df = _map[path]
            _df.to_csv(path,encoding='utf-8')
        self.log(module='simulation',table=TABLE_NAME,status='completed',value=root)

        # self.post(sample=out,stats={"counts"count,"row_suppression":rdf})
        #
        # Let's make sure this goes to a variety of formats
        # html,pdf, json, csv ...
        #
        # print out

    def to_sql (self,info) :
        """
        :info   payload with information of the process to be performed
        """

        TABLE_NAME = self.get_tablename()
        fields = self.get(limit=1).columns.tolist()
        columns = list(fields)
        jobs = {}
        SQL = []
        p = {}
        self.log(module='to_sql',action='generating-sql',table=TABLE_NAME,fields=fields)
        #
        # @NOTE:
        #   If we are dealing with a meta-table we should 
        for id in self.pipeline : #['generalize','suppress','shift','compute']:
            for row in info :
                name = row['name']

                if id not in row['label'] or name not in fields:
                    
                    continue
                # p[name] = row['apply']
                index = fields.index(name)
                fields[index] = row['apply']
                self.log(module='to_sql',field=name,sql=row['apply'])
                # print (row['on'])

        # other_fields = list( set(fields) - set(p.keys()) )

        # SQL = ['SELECT', ",".join(p.values() + other_fields),'FROM ',TABLE_NAME]
        SQL = ['SELECT', ",".join(fields),'FROM ',TABLE_NAME]

        if 'suppress' in self.deid_rules and 'FILTERS' in self.deid_rules['suppress']:
            FILTERS = self.deid_rules['suppress']['FILTERS']
            if FILTERS :
                SQL += ['WHERE']
            for row in FILTERS :
                if not (set(columns) & set(row['filter'].split(' '))) :
                    continue
                SQL += [row['filter']]
                if FILTERS.index(row) < len(FILTERS) - 1 :
                    SQL += ['AND']
        
        return '\t'.join(SQL).replace(":idataset",self.idataset)



    def to_pandas(rules,info):
        pass

