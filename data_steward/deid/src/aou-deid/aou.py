"""
AOU - De-identification Engine
Steve L. Nyemba <steve.l.nyemba@vanderbilt.edu>

This engine will run de-identificataion rules againsts a given table, certain rules are applied to all tables (if possible)
We have devided rules and application of the rules, in order to have granular visibility into what is happening (for the benefit of the testing team)

DESIGN:
   
    - The solution :
    The Application of most of the rules are handled in the SQL projection, this allows for simpler jobs with no risk of limitations around joins imposed by big-query.
    By dissecting operations in this manner it is possible to log the nature of an operation on a given attribute as well as pull some sample data to illustrate it (for the benefit of testers)

    We defined a vocabulary of rule specifications :
        -fields         Attributes upon which a given rule can be applied
        -values         conditional values that determine an out-come of a rule (can be followed by an operation like REGEXP)
                            If followed by "apply":"REGEXP" the the values are assumed to be applied using regular expressions
                            If NOT followed by anything the values are assumed to be integral values and and the IN operator in used instead
        -into           outcome related to a rule
        -key_field      attribute to be used as a filter that can be specified by value_field or values
        -value_field    value associated with a key_field
        -on             suggests a meta table and will have filter condition when generalization or a field name for row based suppression
   

   Overall there are rules that suggest what needs to happen on values, and there is a fine specifying how to apply the rule on a given table.

    - The constraints:
    
        1. There is a disturbing misuse of bigquery as a database (it's a warehouse). To grasp the grotesquery of this it's important to consider the difference between a database and a data warehouse.
        That being said I will not lecture here about the lack of data integrity support and how it lends itself to uncontrollable information redundancies and proliferation which defies which doesn't help at all.
        The point of a relational model/database is rapid information retrieval and thus an imperative to reduce redundancies via normalization.
    

        2. There is much cluelessness around testing methods/techniques of databases like using/defining equivalence classes, orthogonal arrays...
        As such we have a method "simulate" that acts as a sampler to provide some visibility into what this engine has done given an attribute and the value space of the data.
        This is by no means a silver bullet and adds to data redundancies (alas) if falls in the wrong hands shit will hit the fan
   
    LIMITATIONS:
        - The engine is not able to validate the rules without having to submit the job i.e it's only when the rubber hits the road that we know!
        Also that's the point of submitting a job
        - The engine can not simulate complex cases, it's intend is to help in providing information about basic scenarios, testers must do the rest.
        - The engine does not resolve issues of consistency with data for instance : if a record has M,F on two fields for gender ... this issue is out of the scope of deid.
        Because it relates to data-integrity.

    NOTES:
        There is an undocumented featue enabled via a hack i.e Clustering a table. The API (SDK) does NOT provide a clean way to perform clustering a table.
        The analysis of the Rest API and the source code provide a means to enable this. I should probably report/contribute this to the bigquery code base but ... time is a luxury I don't have.

    In order to try to compensate for this I developed an approach to try to look for redundancies using regular expressions and other information available.
    While I think it's the right thing to do given the constraints, I also developped a means by which identifiers can be used provided the englightened leadership decides to do the right thing in a very remote future.

    USAGE :
    
        python aou.py --rules <path.json> --idataset <name> --private_key <file> --table <table.json> --action [submit,simulate|debug] [--parition] [--log <path>]
        --rule  will point to the JSON file contianing rules
        --idataset  name of the input dataset (an output dataset with suffix _deid will be generated)
        --table     path of that specify how rules are to be applied on a table
        --private_key   service account file location
        --pipeline      specifies operations and the order in which operations are to be undertaken. Operations should be comma separated
                        By default the pipeline is generalize,suppress,shift,compute
        --age-limit     This parameter is optional and sets the age limit by default it will apply 89 years
        --action        what to do:
                        simulate    will generate simulation without creating an output table
                        submit      will create an output table
                        debug       will just print output without simulation or submit (runs alone)
    
"""
from press import Press
import pandas as pd
import numpy as np
from google.oauth2 import service_account
from google.cloud import bigquery as bq
import json 
from parser import Parse as Parser
import os
import time
class aou (Press):
    def __init__(self,**args):
        args['store'] = 'bigquery'
        Press.__init__(self,**args)
        self.credentials    = service_account.Credentials.from_service_account_file(args['private_key'])
        self.private_key = args['private_key']
        self.odataset  = self.idataset+'_deid'
        self.partition = 'cluster' in args    
        self.priority = 'BATCH' if 'interactive' not in args else 'INTERACTIVE'
        
        if 'shift' in self.deid_rules :
            #
            # Minor updates that are the result of a limitation as to how rules are specified.
            # @TODO: Improve the rule specification language
            SHIFT_DAYS = " ".join(['SELECT shift from ',self.idataset,'.deid_map WHERE deid_map.person_id = ',self.tablename,'.person_id'])
            self.deid_rules['shift'] = json.loads(json.dumps(self.deid_rules['shift']).replace(":SHIFT",SHIFT_DAYS))
    def initialize(self,**args) :
        Press.initialize(self,**args) 
        AGE_LIMIT = args['age_limit']
        MAX_DAY_SHIFT = args['max_day_shift']
        MILLION = 1000000
        MAP_TABLENAME = self.idataset+".deid_map"
        sql = " ".join(["SELECT DISTINCT person_id, EXTRACT(YEAR FROM CURRENT_DATE()) - year_of_birth as age FROM ",self.idataset+".person","ORDER BY 2"])
        personTable = self.get(sql= sql)        
        self.log(module='initialize',subject=self.get_tablename(),action='patient-count',value=personTable.shape[0])
        mapTable = pd.DataFrame()
        if personTable.shape[0] > 0:
            personTable = personTable[personTable.age < AGE_LIMIT]        
            dirty = False
            mapTable = self.get(sql = "SELECT * FROM "+MAP_TABLENAME)
            if mapTable.shape[0] > 0 and mapTable.shape[0] == personTable.shape[0] :
                #
                # There is nothing to be done here
                # @TODO: This weak post-condition is not enough nor consistent
                #   - consider using a set operation
                #
                pass
            else:
                dirty = True
                N = personTable.shape[0] 
                LOWER_BOUND = MILLION
                UPPER_BOUND = LOWER_BOUND + (10*N) 
                mapTable = pd.DataFrame({"person_id":personTable['person_id'].tolist()})
                mapTable['research_id'] =  np.random.choice( np.arange(LOWER_BOUND,UPPER_BOUND),N,replace=False )
                mapTable['shift'] = np.random.choice( np.arange(1,MAX_DAY_SHIFT),N )
                #
                # We need to write this to bigquery now
                #
                
                mapTable.to_gbq(MAP_TABLENAME,credentials=self.credentials,if_exists='fail')
        else:
            print sql
            print "IT HIT THE FAN"
            
        #
        # @TODO: Make sure that what happened here is logged and made available
        #   - how many people are mapped
        #   - how many people are not mapped
        #   - was the table created or not
        #
        self.log(module='initialize',subject=self.get_tablename(),action='mapped-patients',value=mapTable.shape[0])
        return personTable.shape[0] > 0 or mapTable.shape[0] > 0
    def update_rules(self) :
        """
        This will add rules that are to be applied by default to the current table
        @TODO: Make sure there's a way to specify these in the configuration
        """
        df = self.get(limit=1)
        columns = df.columns.tolist()
        r = []        
        if 'suppress' not in self.info:
            self.info['suppress'] = []
            # self.log(module='update-rules',subject='suppress', object=self.get_tablename(),values=df.columns.tolist())
        #
        # Relational attributes that require suppression are inventoried this allows automatic suppression
        # It's done to make the engine more usable (by minimizing the needs for configuration)
        #
        rem_cols = True
        for rule in self.info['suppress'] :
            if 'rules' in rule and rule['rules'].endswith('DEMOGRAPHICS-COLUMNS') :
                rem_cols = False
                rule['table'] = self.get_tablename()
                rule['fields'] = columns
        if rem_cols :
            self.info['suppress'] += [{"rules": "@suppress.DEMOGRAPHICS-COLUMNS","table":self.get_tablename(), "fields":df.columns.tolist()}]
                    
        date_columns = [name for name in columns if set(['date','time','datetime']) & set(name.split('_'))]
        #
        # shifting date attributes can be automatically done for relational fields by looking at the field name of the data-type
        # @TODO: consider looking at the field name, changes might happen all of a sudden
        #
        
        
        
        if date_columns :
            date = {}
            datetime= {}
            
            for name in date_columns :
                if 'datetime' in name or 'time' in name :
                    if 'fields' not in datetime :
                        datetime['fields'] = []
                    datetime['fields'].append(name)                    
                    datetime['rules'] = '@shift.datetime'
                elif 'date' in name :
                    if 'fields' not in date :
                        date['fields'] = []
                    
                    date['fields'].append(name)
                    date['rules'] = '@shift.date'
            _toshift = []
            if date and datetime :
                _toshift = [date,datetime]
            elif date or datetime:
                _toshift = [date] if date else [datetime] 
            if 'shift' not in self.info :
                self.info['shift'] = []
            if _toshift :
                self.log(module='update-rules',subject=self.get_tablename(),object='shift',fields=_toshift)
                self.info['shift'] += _toshift
            
            # self.info['shift'] = _toshift if 'shift' not in self.info else self.info['shift'] + _toshift
            # print self.info['shift']
        #
        # let's check for the person_id
        has_compute_id = False
        if 'compute' not in self.info :
            self.info['compute'] = []

        else:
            index = [self.info['compute'].index(rule) for rule in self.info['compute'] if '@compute.id' in rule] 
            has_compute_id = False if index else True
        if not has_compute_id and 'person_id' in columns:
            # self.info['compute'] += [{"rules":"@compute.id","fields":["person_id"],"table":":idataset.deid_map as map_user","key_field":"map_user.person_id","value_field":":table.person_id"}]
            self.info['compute'] += [{"rules":"@compute.id","fields":["person_id"],"table":":idataset.deid_map as map_user","key_field":"map_user.person_id","value_field":self.tablename+".person_id"}]
            

        
        
    def get(self,**args):
        """
        This function will execute a query to a data-frame (for easy handling)
        """
        if 'sql' in args :        
            sql = args['sql']
        else:
            
            sql = "".join(["SELECT * FROM ",self.idataset,".",self.tablename])
        if 'limit' in args :
            sql = sql + " LIMIT "+str(args['limit'])
        try:

            df = pd.read_gbq(sql,credentials=self.credentials,dialect='standard')
            return df
        except Exception,e:
            self.log(module='get',action='error',value=e.message)
            pass
        return pd.DataFrame()
    def submit(self,sql):
        """
        """
        TABLE_NAME = self.get_tablename()        
        client = bq.Client.from_service_account_json(self.private_key)
        #
        # Let's make sure the out dataset exists
        datasets = list(client.list_datasets())
        found = np.sum([1  for dataset in datasets if dataset.dataset_id == self.odataset])
        if not found :
            dataset = bq.Dataset(client.dataset(self.odataset))
            client.create_dataset(dataset)
        # self.odataset = bq.Dataset(client.dataset(self.odataset))
        job = bq.QueryJobConfig()        
        job.destination = client.dataset(self.odataset).table(self.tablename)
        job.use_query_cache = True
        job.allow_large_results = True
        if self.partition:
            job._properties['load']['timePartitioning'] = {'type': 'DAY', 'field': 'person_id'}

        # if joined_fields not in ["",None]:
        job.time_partitioning = bq.table.TimePartitioning(type_=bq.table.TimePartitioningType.DAY)
        job.priority = 'BATCH'
        job.priority = 'INTERACTIVE'
        job.priority = self.priority
        job.dry_run = True 
        self.log(module='submit-job',subject=self.get_tablename(),action='dry-run',value={'priority':self.priority,'parition':self.partition})
        if not os.path.exists(self.logpath) :
            os.mkdir(self.logpath)
        if  not os.path.exists(os.sep.join([self.logpath,self.idataset])) :
            os.mkdir(os.sep.join([self.logpath,self.idataset]))

        #f = open(os.sep.join([self.logpath,self.idataset,self.tablename+".sql"]),'w')
        #f.write(sql)
        #f.close()
        
        r = client.query(sql,location='US',job_config=job)
        if r.errors is None and r.state == 'DONE' :
            job.dry_run = False
            #
            # NOTE: This is a hack that enables table clustering (hope it works)
            # The hack is based on an analysis of the source code of bigquery
            #

            #job._properties['query']['clustering'] = {'fields':['person_id']}
            r = client.query(sql,location='US',job_config=job)
            self.log(module='submit',subject=self.get_tablename(),action='submit-job',table=TABLE_NAME,status='pending',value=r.job_id,object='bigquery')
            self.wait(client,r.job_id)
            self.finalize(client)
            #
            # At this point we must try to partition the table
        else:
            self.log(module='submit',subject=self.get_tablename(),action='submit-job',table=TABLE_NAME,status='error',value=r.errors)
            print (r.errors)

            pass
    def wait(self,client,job_id):
        self.log(module='wait',subject=self.get_tablename(),action="sleep",value=job_id)
        STATUS = 'NONE'
        while True:
            STATUS = client.get_job(job_id).state
            if STATUS == 'DONE' :
                break
            else:
                
                time.sleep(5)
        self.log(module='wait',action='awake',status=STATUS)
        pass
    def finalize(self,client):
        i_dataset,i_table = self.get_tablename().split('.')
        ischema = client.get_table(client.dataset(i_dataset).table(i_table)).schema
        table = client.get_table(client.dataset(i_dataset+'_deid').table(i_table))
        fields = [field.name for field in table.schema]
        ischema_size = len(ischema)
        
        newfields = [bq.SchemaField(name=field.name,field_type=field.field_type,description=field.description) for field in ischema if field.name in fields]
        #oschema = table.schema + newfields
        table.schema = newfields
        #for ofield in table.schema:
        #    ofield._description = next((ifield for ifield in ischema if ifield.name == ofield.name), None)
        r = client.update_table(table,['schema'])
        #self.log(module='finalize',action='update-descriptions',value=)


if __name__ == '__main__' :
    p = ['idataset','private_key','rules','input-folder']
    # params = {"rules_path":"data/config.json","info_path":"data/observation.json","idataset":"combined20181016","table":"observation"}
    # params["private_key"] = '/home/steve/dev/google-cloud-sdk/accounts/curation-test.json'
    SYS_ARGS = Parser.sys_args()  
    if 'pipeline' not in SYS_ARGS :
        SYS_ARGS['pipeline'] = 'generalize,suppress,shift,compute' 
    
    SYS_ARGS['pipeline'] = [term.strip() for term in SYS_ARGS['pipeline'].split(',')]  
    AGE_LIMIT = int(SYS_ARGS['age-limit']) if 'age-limit' in SYS_ARGS else 89
    handle = aou(**SYS_ARGS)
    if handle.initialize(age_limit=AGE_LIMIT,max_day_shift=365) :
        handle.do()
    else:
        print ("Unable to initialize process ")
        print ("\tInsure that the parameters are correct")
    pass
