"""
    AoUS - DEID, 2018
    Steve L. Nyemba<steve.l.nyemba@vanderbilt.edu>
    
    This file implements the deidentification policy for the All of Us project. This is a rule based approach:
        - Suppression of PPI, EHR, PM
        - Date Shifting
        - Generalization of certain fields (race, gender, ...)
        
    It should be noted that the DRC database is hybrid rational & meta-database of sort (not sure why hybrid)
    The database will contain meta information about the data and the data as well (in most cases).
    More information on metamodeling https://en.wikipedia.org/wiki/Metamodeling
    
    Design:
    Performing de-identification is a "chatty" process as consisting of joins over joins over joins ... depending on the operations.
    e.g: In order to determine a person's race two joins are required, one that determines the number of races and the other if the races should be generalized.
    This chatty process to be performed for every individual and on a every field subject to de-identification.

    As a result of the nature of the de-identificatioin operations (chatty) and the limitations imposed by bigquery, we have implemented a query builder that will generate and build an SQL query that will implement the de-identification logic:
    - This keeps the communication with bigquery to the minimum
    - Once the DEID query is built it is submitted to bigquery as a job that can be monitored
    BigQuery will handle the necessary optimizations
    e.g :
        If a table (relational) has a date-field and one or more arbitrary fields to be removed:
        1. A projection will be run against the list of fields that would work minus the date fields
        2. The date fields will be shifted given separate logic
        3. The result of (1) and (2) will be joined on person_id (hopefully we don't need to specify a key)

    e.g:
        If a meta table with a date field needs to be removed the above operation is performed twice:
        1. First on a subset of records filtered by records for any date type (specified as a concept)
        2. The second subset of records contains the dates (specified by concepts) and will be shifted (row based operation)
        3. The results of (1) and (2) will unioned. Note that (1) include a join already
        
    Once the Query is put together we send it to bigquery as a job that can be monitored

    Usage :
    Requirements:
        You must install all the dependencies needed to run the code, they are found in the file requirements.txt.
        The codebase is developed with python 2.7.x platform

        pip install -r requirements.txt
    python deid.py --i_datase <input_dataset> --table <table_name> --o_dataset <output_dataset>

@TODO: 
    - Improve the logs (make sure they're expressive and usable for mining)
    - Limitations append an existing table isn't yet supported
    
    
    
"""
from __future__ import division
import sys
import json
import logging
from google.cloud import bigquery as bq
from datetime import datetime
import os

#
# Let's process the arguments passed in via the command-line
# We expect the program to be run as follows : python deid.py --i_dataset <input_dataset> --table <table_name> --config path-of-config.json --log
#
SYS_ARGS = {}
if len(sys.argv) > 1:
	
	N = len(sys.argv)
	for i in range(1,N):
		value = None
		if sys.argv[i].startswith('--'):
			key = sys.argv[i].replace('-','')
			SYS_ARGS[key] = 1
			if i + 1 < N and sys.argv[i+1].startswith('--') is False:
				value = sys.argv[i + 1] = sys.argv[i+1].strip()
			if key and value:
				SYS_ARGS[key] = value
		
		i += 2
class Logging:
    """
        This class will perform a basic logging against a file, 
        if no --log is specified in the SYS_ARGS then the log function will just print
        @TODO: 
        Logging for other areas endpoint, database, ...
    """
    @staticmethod
    def log(**args):
        name = datetime.now().strftime('deid-%Y-%m-%d.log')
        
        date = json.loads(datetime.now().strftime('{"year":%Y,"month":"%m","day":"%d","hour":"%H","min":"%M"}'))
        row = json.dumps(dict(date,**args))
        row = json.dumps(args)
        if 'log' in SYS_ARGS  :
            path = './' if SYS_ARGS['log'] == 1 else SYS_ARGS['log']
            filename = os.sep.join([path,name])
            logging.basicConfig(filename=filename,level=logging.INFO,format='%(message)s')            
            logging.info(row)
        else:
            print (row)
        
class Policy :
    """
        This function will apply Policies given the fields found on a given table
        The policy hierarchy will be applied as an iterator design pattern.
    """
    META_TABLES = ['observation']
    class TERMS :
        SEXUAL_ORIENTATION_STRAIGHT     = 'SexualOrientation_Straight'
        SEXUAL_ORIENTATION_NOT_STRAIGHT = 'SexualOrientation_None'
        OBSERVATION_FILTERS = {"race":'Race_WhatRace',"gender":'Gender',"orientation":'Orientation',"employment":'_EmploymentStatus',"sex_at_birth":'BiologicalSexAtBirth_SexAtBirth',"language":'Language_SpokenWrittenLanguage',"education":'EducationLevel_HighestGrade'}
        BEGIN_OF_TIME = '1980-07-21'

    @staticmethod
    def get_dropped_fields(fields):
        """ 
            In order to preserve structural integrity of the database we must empty the values of the fields systematically
            @NOTE:
        """
        r = []
        for name in fields :
            r.append("""'' as :name """.replace(":name",name))
        
        return r
    def __init__(self,**args):
        """
            This function loads basic specifications for a policy
        """
        self.fields = []
        self.policies = {}
        self.cache = {}
        if 'client' in args :
            self.client = args['client']
        elif 'path' in args :
            self.client = bq.Client.from_service_account_json(args['path'])
        self.vocabulary_id = args['vocabulary_id'] if 'vocabulary_id' in args else 'PPI'
        self.concept_class_id = args['concept_class_id'] if 'concept_class_id' in args else ['Question','PPI Modifier']
        if isinstance(self.concept_class_id,str):
            self.concept_class_id = self.concept_class_id.split(",")            
        Logging.log(subject=self.name(),action='init',object=self.client.project,value=[])

    def can_do(self,id,meta):
        return False
    def get(self,dataset,table) :
        return None
    def name(self):
        return self.__class__.__name__.lower()


class Shift (Policy):
    """
        This class will generate the SQL queries that will perform date-shifting against either a meta-table, relational table or a hybrid table.
        The way in which they are composed will be decided by the calling code that will serve as an "orchestrator". 
        for example:
            A hybrid table will perform the following operations given the nature of the fields:
                - For physical date fields a JOIN
                - For meta fields a UNION
                
        
    """
    def __init__(self,**args):
        Policy.__init__(self,**args)
        
        self.concept_sql = """
                SELECT concept_code from :dataset.concept
                WHERE vocabulary_id = ':vocabulary_id' AND REGEXP_CONTAINS(concept_code,'(Date|DATE|date)') is TRUE

            """
    def can_do(self,dataset,table):
        """
            This function determines if the a date shift is possible i.e :
            - The table has physical date fields 
            - The table has concept codes that are of date type
            @param table    table identifier
            @param dataset  dataset identifier
        """
        p = False
        q = False
        name = ".".join([dataset,table])
        if name not in self.cache :
            try:
                ref = self.client.dataset(dataset).table(table)
                info = self.client.get_table(ref).schema
                fields = [field for field in info if field.field_type in ('DATE','TIMESTAMP','DATETIME')]
                p = len(fields) > 0 #-- do we have physical fields as concepts
                q = table in Policy.META_TABLES
                sql_fields = self.__get_shifted_fields(fields,dataset,table)
                #
                # In the case we have something we should store it
                self.cache[name] = p or q
                joined_fields = [field.name for field in fields]
                if self.cache[name] == True :
                    self.policies[name] = {"join":{"sql":None,"fields":joined_fields,"shifted_values":sql_fields}}

                if q :
                   
                    #
                    # q == True implicitly means that self.cache[name] is True (propositional logic)
                    # We are dealing with a meta table (observation), We need to shift the dates of the observation_source_value that are of type date
                    #
                    # I assume the only meta-table is the observation table in the case of another one more parameters need to be passed
                    #
                    union_fields = ['value_as_string']+joined_fields +['person_id']
                    #
                    # @NOTE: The date-shifting here is redundant, but it's an artifact of mixing relational & meta-model in the database
                    #
                    # begin_of_time = datetime.strptime(Policy.TERMS.BEGIN_OF_TIME,'%Y-%m-%d')
                    # year = int(datetime.now().strftime("%Y"))  - begin_of_time.year
                    # month = begin_of_time.month
                    # day = begin_of_time.day
                    # shifted_date = """CAST(DATE_SUB( DATE_SUB(DATE_SUB( CAST(:name AS DATE),INTERVAL :year YEAR),INTERVAL :month MONTH),INTERVAL :day DAY) AS STRING) as :name""".replace(':name',"value_as_string").replace(":year",str(year)).replace(":month",str(month)).replace(":day",str(day))
                    
                    # shifted_field = """
                    #     CAST(
                    #     date_sub((SELECT CAST(value_as_string as DATE) FROM :i_dataset.observation ii where ii.person_id = person_id and observation_source_value='ExtraConsent_TodaysDate' limit 1) , INTERVAL 
                    #     date_diff(:name, (SELECT seed from :i_dataset.people_seed ii where ii.person_id = person_id), DAY) DAY) AS STRING) as :name
                    # """.replace(":name","value_as_string")
                    shifted_date = """CAST( DATE_SUB( CAST(:name AS DATE), INTERVAL (SELECT seed from :i_dataset.people_seed xii WHERE xii.person_id = :table.person_id) DAY) AS STRING) as :name"""
                    shifted_date = shifted_date.replace(":name","value_as_string").replace(":i_dataset",dataset).replace(":table","x")
                    sql_fields = self.__get_shifted_fields(fields,dataset,"x")
                    #--AND person_id = 562270
                    sql_filter = "|".join(Policy.TERMS.OBSERVATION_FILTERS.values())
                    _sql = """
                    SELECT :shifted_date,person_id, :shifted_fields :fields
                    FROM :i_dataset.observation x where observation_source_value in (
                        SELECT concept_code from :i_dataset.concept WHERE REGEXP_CONTAINS(concept_code,'Date|DATE|date') IS TRUE
                        AND REGEXP_CONTAINS(concept_code,'(:filter)') IS FALSE
                    )
                    """.replace(":i_dataset",dataset).replace(":shifted_fields",",".join(sql_fields)).replace(":shifted_date",shifted_date).replace(":filter",sql_filter)
                    
                    # _sql = """
                    
                    #     SELECT CAST (DATE_DIFF(CAST(x.value_as_string AS DATE),CAST(y.value_as_string AS DATE),DAY) as STRING) as value_as_string, x.person_id, :shifted_fields :fields
                    #     FROM :i_dataset.observation x INNER JOIN (
                    #         SELECT MAX(value_as_string) as value_as_string, person_id
                    #         FROM :i_dataset.observation
                    #         WHERE observation_source_value = 'ExtraConsent_TodaysDate'
                            
                    #         GROUP BY person_id
                    #     ) y ON x.person_id = y.person_id 
                        
                    #     WHERE observation_source_value in (
                            
                    #         SELECT concept_code from :i_dataset.concept 
                    #         WHERE REGEXP_CONTAINS(concept_code,'(DATE|Date|date)') IS TRUE
                            
                    #     )
                         
                    # """.replace(":i_dataset",dataset).replace(":shifted_fields",",".join(sql_fields))
                    
                    self.policies[name]["union"] = {"sql":_sql,"fields":union_fields,"shifted_values":sql_fields}
                    
                    # self.policies[name]['meta'] = 'foo'
                #
                # @TODO: Log the results of the propositional logic operation (summarized)
                Logging.log(subject=self.name(),object=name,action='can_do',value={"relational":p,"meta":q})
            except Exception, e:
                # @TODO
                # We need to log this stuff ...
                print e
                Logging.log(subject=self.name(),object=name,action='error.can_do',value=e.message)
        
        return self.cache[name]
    def __get_shifted_fields(self,fields,dataset,table):
        """
            This function should be used for relational fields only !!
            @param fields   a list of SchemaField objects
        """
        # begin_of_time = datetime.strptime(Policy.TERMS.BEGIN_OF_TIME,'%Y-%m-%d')
        # year = int(datetime.now().strftime("%Y"))  - begin_of_time.year
        # month = begin_of_time.month
        # day = begin_of_time.day
        r = []
        
        for field in fields :
            # shifted_field =  """
            #     DATE_DIFF( CAST(:name AS DATE), CAST(':date' AS DATE), DAY) as :name
            # """.replace(':name',field.name).replace(":date",Policy.TERMS.BEGIN_OF_TIME)
            # shifted_field = """
            #     DATE_SUB( DATE_SUB(DATE_SUB( CAST(:name AS DATE),INTERVAL :year YEAR),INTERVAL :month MONTH),INTERVAL :day DAY) as :name
            # """.replace(':name',field.name).replace(":year",str(year)).replace(":month",str(month)).replace(":day",str(day))
           
            shifted_field = """
               DATE_SUB( CAST(:name AS DATE), INTERVAL (SELECT seed from :i_dataset.people_seed xii WHERE xii.person_id = :table.person_id) DAY) as :name
            """.replace(":name",field.name).replace(":table",table).replace(":i_dataset",dataset)
            # shifted_field = shifted_field.replace(":name",field.name).replace(":i_dataset",dataset)
            r.append(shifted_field)
        Logging.log(subject=self.name(),action='shifting.dates',object=[field.name for field in fields],value=[field.name for field in fields])   
        return r #",".join(r)
    def get(self,dataset,table):
        """
        @pre:
            can_do(dataset,table) == True

            This function will return the sql queries for for either physical fields and meta fields
            @param dataset  name of the dataset
            @param table    name of the table
        """
        name = dataset+"."+table
        
        return self.policies[name] if name in self.policies else None

class DropFields(Policy):
    """
        This class generate the SQL that will perform suppression against either a physical table and meta-table        
        By default this class will suppress all of the datefields and other fields specified by the user. 
        This will apply to both relational and meta-tables
    """
    def __init__(self,**args):
        """
            @param vocabulary_id        vocabulary identifier by default PPI
            @param concept_class_id     identifier of the category of the concept by default ['PPI', 'PPI Modifier']
            @param fields   list of fields that need to be dropped/suppressed from the database
        """
        Policy.__init__(self,**args)
        # self.fields = args['fields'] if 'fields' in args else []
        self.remove = args['remove'] if 'remove' in args else []
        
    def can_do(self,dataset,table):
        name = dataset+"."+table
        
        if name not in self.cache :
            try:
                ref     = self.client.dataset(dataset).table(table)
                schema  = self.client.get_table(ref).schema
                gsql    = None
                #
                # we have here the opportunity to have both columns removed and rows removed
                # The remove object has {columns:[],rows:[]} both of which should hold criteria for removal if true (simple logic)
                #
                
                remove_cols = self.remove['columns'] if 'columns' in self.remove else []
                date_cols = [field.name for field in schema if field.field_type in ['DATE','TIMESTAMP','DATETIME']]
                remove_cols = list(set(remove_cols) | set(date_cols))    #-- removing duplicates from the list of fields
                
                p = len(remove_cols) > 0       #-- Do we have fields to remove (for physical tables)
                q = table in Policy.META_TABLES #-- Are we dealing with a meta table               

                self.cache[name] = p or q                
                sql = """
                    SELECT :fields :shifted_date_columns
                    FROM :i_dataset.:table
                """
                
                if p :
                    # _fields = [field.name for field in schema if field.name not in self.fields] #--fields that will be part of the projection
                    _fields = [field.name for field in schema if field.name not in remove_cols] 
                    lfields = list(_fields)
                    _fields = ",".join(_fields)
                else:
                    _fields = "*"
                    lfields = [field.name for field in schema]
                #
                # @Log: We are logging here the operaton that is expected to take place
                # {"action":"drop-fields","input":self.remove,"subject":table,"object":"columns"}
                Logging.log(subject=self.name(),object=name,action='can_do',value={"drop.cols":remove_cols,"shift.cols":date_cols})
                if q :
                    #
                    # We are dealing with observation / meta table. Certain rows have to be removed due to the fact that it's a meta-table
                    #   - Date  because they will be shifted
                    #   - {Race,Gender,Ethnicity, Education, Employment, Language, Sexual Orientation} because they will be generalized
                    # As a result of filtering out the above fields, we need to run a cascading Unions of which each will have its dates shifted.
                    #
                    codes = "'"+"','".join(self.concept_class_id)+"'"
                    sql_filter = "Date|"+ "|".join(Policy.TERMS.OBSERVATION_FILTERS.values())
                    #
                    # @Log: We are logging here the operaton that is expected to take place
                    # {"action":"drop-fields","input":sql_filter,"subject":table,"object":"rows"}
                    
                    # filter = 'Date|Gender|Race|Ethnicity|Employment|Orientation|Education'
                    sql = sql + """

                        WHERE observation_source_concept_id in (
                            SELECT concept_id 
                            FROM :i_dataset.concept 
                            WHERE vocabulary_id = ':vocabulary_id' AND concept_class_id in (:code)
                            
                            AND REGEXP_CONTAINS(concept_code,'(:filter)') IS FALSE
                        )

                       
                    """.replace(":code",codes).replace(":vocabulary_id",self.vocabulary_id).replace(":filter",sql_filter)
                    #
                    #   We are now having to generalize rows that were filtered out (done in a loop)
                    #   These queries will be unioned in the end.
                    #
                    xsql = [sql]
                    args = {"client":self.client,"dataset":dataset,"table":table,"fields":_fields,"sql":"","concept_source_id":[],"vocabulary_id":"","concept_class_id":[]}
                    handler = Group(**args)
                    for key in Policy.TERMS.OBSERVATION_FILTERS :
                        
                        pointer = getattr(handler,key)
                        r = pointer()
                        
                        if len(r.keys()) > 0 :
                            ofields = [ r[fname] if fname in r else fname for fname in lfields]
                            
                            _sql_ = "SELECT :fields  :shifted_date_columns FROM :i_dataset.:table WHERE observation_source_concept_id in (SELECT concept_id FROM :i_dataset.concept WHERE REGEXP_CONTAINS(concept_code,'(?i):key')) "
                            _sql_ = _sql_.replace(":fields",",".join(ofields)).replace(":table",table).replace(":key",key).replace(":i_dataset",dataset)
                            
                            xsql.append( " UNION ALL "+_sql_ )
                    if len(date_cols) is None :
                        date_cols = ""
                    else:
                        date_cols = ","+",".join(date_cols)    
                    
                    sql =  " SELECT :fields "+date_cols+" from (" +" ".join(xsql) +")"
                    
                sql = sql.replace(":fields",_fields).replace(":i_dataset",dataset).replace(":table",table)
                
                
                    
                self.policies[name] = {"sql":sql,"fields":lfields}
                # if gsql is not None:
                #     self.policies[name]['generalized'] = gsql
               
                
          
            except Exception,e:
                print e
        
        return self.cache [name]
    def get(self,dataset,table):
        name = dataset+"."+table
        return self.policies[name] if name in self.policies else False


class Group(Policy):
    """
        This class performs generalization against the data-model on a given table
        The operations will apply on :
            - gender/sex
            - sexual orientation
            - race
            - education
            - employment
            - language
        This is an inherently inefficient operation as a result of the design of the database that unfortunately has redudancies and semantic ambiguity (alas)
        As such this code will proceed case by case

        @TODO: Talk with the database designers to improve the design otherwise this code may NOT scale nor be extensible
    """
    def __init__(self,**args):
        """
            @param path     either the path to the service account or an initialized instance of the client
            @param sql      sql query to execute
            @param dataset  dataset subject
            @param table    table (subject of the operation)
        """
        Policy.__init__(self,**args)
        self.sql        = args['sql']
        self.dataset    = args['dataset']
        self.table      = args['table']
        if isinstance(args['fields'],basestring) :
            self.fields = args['fields'].split(',')
        else:
            self.fields     = args['fields']

    def get_fields(self,p):
        """
            This function returns the field list with generalized expressions of the fields
            @param p    mapping parameter of fields and associated expressions
        """
        
        fields = list(self.fields)
        r = {}
        for name in p :
            
                     
            if name in fields:
                index = fields.index(name)    
                value = p[name]
                fields[index] = value
                r[name] = value
        # return fields
        
        return r
    def race(self):
        """
            let's generalize race as follows all non-{white,black,asian} should be grouped as Other
            @pre :
                requires concept table to exist and be populated.
        """
        #
        # For reasons I am unable to explain I noticed that the basic races were stored in concept table as integers
        # The goal of the excercise is that non {white,black,asians} are stored as others.
        # The person table has redundant information (not sure why) in race_concept_id and race_source_value
        #

        # @TODO: Multiple races (add this)
        # sql = "SELECT person_id,count(*) FROM raw.observation WHERE observation_source_concept_id in (SELECT concept_id FROM raw.concept WHERE REGEXP_CONTAINS(concept_code,'^Race_')) GROUP BY person_id HAVING count(*) > 1"
        # r = self.client.query(sql).to_dataframe()
        # m_ids = ",".join([str(value) for value in r['person_id'].tolist()])
        #
        # We retrieve the identifiers of all the known races {black,white,asian,other} and anything that doesn't belong will be other
        # @NOTE:
        #   For some unknown reason (poor design) it would appear that on tables like person concept_name holds the value of the race whereas in observation table concept_code holds the value of the race
        # This is an unacceptable inconcsistency that make make data broadly available with different representations thus increasing the risk of re-identification.
        #
        
        field_name = "concept_name" if self.table == 'person' else 'concept_code'
        fields = self.fields 
        sql = "SELECT concept_id,concept_code,concept_name from :dataset.concept WHERE REGEXP_CONTAINS(vocabulary_id,'(PPI|Race)') AND REGEXP_CONTAINS(concept_name,'(White|Black|Asian|Other Race)') is TRUE AND REGEXP_CONTAINS(concept_name,'(Native|Pacific)') is FALSE"
        sql = sql.replace(":dataset",self.dataset)
        r = self.client.query(sql)
        r = r.to_dataframe()
        other_id= r[r['concept_name'] == 'Other Race']['concept_id'].tolist()[0]
        other_name= r[r['concept_name'] == 'Other Race']['concept_name'].tolist()[0]
        _ids    = [str(value) for value in r[r['concept_name'] != 'Other Race']['concept_id'].tolist()]
        #
        # Formatting the fields to perform the generalization of the  of the a person
        #
        _ids        = ",".join(_ids)
        other_id    = str(other_id)
        
        p       = {}
        if self.table == 'person' :
            
            p["race_concept_id"] = "IF(race_concept_id not in (:_ids),:other_id,race_concept_id) as race_concept_id".replace(":_ids",_ids).replace(":other_id",other_id)
            p["race_source_value"]="IF(race_concept_id not in (:_ids),':other_name',race_source_value) as race_source_value".replace(":_ids",_ids).replace(":other_name",other_name)
            
            # return self.get_fields(p)

        else:
            #
            # Let's generalize race and everything that goes with
            # @TODO: Figure out cases for multiple races
            mr_sql="SELECT person_id from (SELECT COUNT(*), person_id FROM :dataset.:table WHERE observation_source_value like 'Race_%' GROUP BY person_id HAVING COUNT(*) > 1)".replace(":dataset",self.dataset).replace(":table",self.table)
            p['value_as_string'] = "IF( person_id in (:mr_sql),'Multi-Racial',IF(value_source_concept_id not in (:_ids),':other_name',value_as_string)) as value_as_string".replace(":_ids",_ids).replace(":other_name",other_name).replace(":mr_sql",mr_sql)
            p['observation_source_concept_id'] = "IF(person_id in (:mr_sql),2000000,IF(value_source_concept_id not in (:_ids),:other_id,observation_source_concept_id)) as observation_source_concept_id".replace(":_ids",_ids).replace(":other_id",other_id).replace(":mr_sql",mr_sql)
            # p['observation_source_value'] = "IF((SELECT COUNT(*) FROM :dataset.observation z WHERE z.observation_source_value like 'Race_%' AND z.person_id = person_id) > 1,:other_name,IF(value_source_concept_id not in (:_ids), ':other_name',observation_source_value)) as observation_source_value".replace(":_ids",_ids).replace(":other_name",other_name).replace(":dataset",self.dataset)
            # p['value_source_concept_id'] = "IF(value_source_concept_id not in (:_ids), ':other_id',value_source_concept_id) as value_source_concept_id".replace(":_ids",_ids).replace(":other_name",other_name).replace(":dataset",self.dataset)
            p['value_source_concept_id'] = "IF(person_id in (:mr_sql),2000000,IF(value_source_concept_id not in (:_ids),:other_id,value_source_concept_id)) as value_source_concept_id".replace(":_ids",_ids).replace(":other_id",other_id).replace(":mr_sql",mr_sql)
            p['value_source_value'] = "IF(person_id in (:mr_sql),'Multi-Racial',IF(value_source_concept_id not in (:_ids), ':other_name',value_source_value)) as value_source_value".replace(":_ids",_ids).replace(":other_name",other_name).replace(":mr_sql",mr_sql)

           
        return self.get_fields(p)
        #
        # let's extract the other_id
        return None
    def gender(self):
        """
            This function will generalize gender from the person table as well as the observation table
            Other if not {M,F}            
            
            @NOTE : The table has fields with redundant information (shit design) i.e gender_source_value and gender_concept_id

            @param dataset
            @param table
            @param fields
        """
        sql = "SELECT concept_id,concept_name FROM :dataset.concept WHERE (vocabulary_id= 'Gender' AND concept_name not in ('FEMALE','MALE') ) OR REGEXP_CONTAINS(concept_code,'_Man|_Woman')"
        sql = sql.replace(":dataset",self.dataset)
        r = self.client.query(sql)
        r = r.to_dataframe()
        
        other_id = str(r[r['concept_name']=='OTHER']['concept_id'].values[0])                        #--
        other_name = r[r['concept_name']=='OTHER']['concept_name'].values[0]                      #--
        _ids =",".join([str(value) for value in r[r['concept_name']!='OTHER']['concept_id'].tolist()])    #-- ids to generalize
        fields = self.fields #args['fields']
        
        p = {}
        if self.table == 'person' :
            #
            # We retrieve the identifiers of the fields to be generalized
            # The expectation is that we have {Male,Female,Other} with other having modern gender nomenclature
            #
            p ["gender_concept_id"] = "IF(gender_concept_id in ( :_ids ),:other_id,gender_concept_id) as gender_concept_id".replace(":_ids",_ids).replace(":other_id",other_id)
            p ["gender_source_value"]= "IF(gender_concept_id in (:_ids),:other_name,gender_source_value) as gender_source_value".replace(":_ids",_ids).replace(":other_name",other_name)
            # for name in p :
            #     index = fields.index(name)
            #     value = p[name].replace(":_ids",",".join(_ids)).replace(":other_id",str(other_id))
            #     if index > 0 :
            #         fields[index] = value
            # return fields
        else:
            #
            # This section will handle observations
            #
            p['value_as_string'] = "IF(value_source_concept_id not in (:_ids),':other_name',value_as_string) as value_as_string".replace(":_ids",_ids).replace(":other_name",other_name)
            p['observation_source_concept_id'] = "IF(value_source_concept_id not in (:_ids),:other_id,observation_source_concept_id) as observation_source_concept_id".replace(":_ids",_ids).replace(":other_id",other_id)
            p['observation_source_value'] = "IF(value_source_concept_id not in (:_ids), ':other_name',observation_source_value) as observation_source_value".replace(":_ids",_ids).replace(":other_name",other_name)
            p['value_source_value'] = "IF(value_source_concept_id not in (:_ids), ':other_name',value_source_value) as value_source_value".replace(":_ids",_ids).replace(":other_name",other_name)
        return self.get_fields(p)
    def __get_formatted_observations(self,_ids,other_name,other_id) :
       
        _ids = ",".join(_ids) if isinstance(_ids,list) else _ids        
        other_id = str(other_id) if isinstance(other_id,int) else other_id
        other_name = other_name.replace("'","\\'")
        p = {}
        p['value_as_string'] = "IF(value_source_concept_id not in (:_ids),':other_name',value_as_string) as value_as_string".replace(":_ids",_ids).replace(":other_name",other_name)
        p['observation_source_concept_id'] = "IF(value_source_concept_id not in (:_ids),:other_id,observation_source_concept_id) as observation_source_concept_id".replace(":_ids",_ids).replace(":other_id",other_id)
        p['observation_source_value'] = "IF(value_source_concept_id not in (:_ids), ':other_name',observation_source_value) as observation_source_value".replace(":_ids",_ids).replace(":other_name",other_name)
        p['value_source_value'] = "IF(value_source_concept_id not in (:_ids), ':other_name',value_source_value) as value_source_value".replace(":_ids",_ids).replace(":other_name",other_name)
        return self.get_fields(p)
    def ethnicity(self):
        """
            This function generalizes the ethnicity of an individual i.e 
            an ethnicity can be {hispanic or latino, not hispnaic or latino}
            @NOTE: This will be dropped !!
        """
        return None
    def orientation(self):
        """
            This function will generalize sexual orientation on the observation table, this only applies to the observation table (for now)
            @filter    TheBasics_SexualOrientation
        """
        sql = "SELECT concept_id,concept_code,concept_name from :dataset.concept where REGEXP_CONTAINS(concept_code, 'Orientation_Straight|Orientation_None')"
        sql = sql.replace(":dataset",self.dataset)
        r = self.client.query(sql)
        r = r.to_dataframe()
       
        other_id = str(r[r['concept_code'] == Policy.TERMS.SEXUAL_ORIENTATION_NOT_STRAIGHT]['concept_id'].tolist()[0])                        #--
    
        other_name = r[r['concept_code']==Policy.TERMS.SEXUAL_ORIENTATION_NOT_STRAIGHT]['concept_code'].tolist()[0]  
        #                     #--
        _ids =[str(value) for value in r[r['concept_code'] ==Policy.TERMS.SEXUAL_ORIENTATION_STRAIGHT]['concept_id'].tolist()]    #-- ids to generalize
        
        fields = self.fields
        
        return self.__get_formatted_observations(_ids,other_name,other_id)
    def education(self):
        """
            Educattion should be in 5 categories provided by the concept_codes below. Because we do NOT have an unknown education level we will hard code it and set it's concept id to zero (No matching concept)
            @TODO:
            The data curation team should add this in the concept table (put in a request with Mark or Chun Yee)
        """
        sql = "SELECT concept_id,concept_code,concept_name from :dataset.concept WHERE concept_code in ('HighestGrade_AdvancedDegree','HighestGrade_CollegeOnetoThree','HighestGrade_TwelveOrGED','HighestGrade_NeverAttended')"        
        other_id = '0'
        other_name = 'Unknown'
        sql = sql.replace(":dataset",self.dataset)
        r = self.client.query(sql)
        r = r.to_dataframe()        
        _ids = [str(value) for value in r['concept_id'].tolist()]
        
        return self.__get_formatted_observations(_ids,other_name,other_id)
    def sex_at_birth(self):
        """
            This function will perform sex at birth generalization against the observation table
            @filter value_source_concept_id in (SELECT concept_id from :dataset.concept WHERE concept_code = 'BiologicalSexAtBirth_SexAtBirth')
        """
        sql = "SELECT concept_id,concept_code,concept_name from :dataset.concept WHERE  concept_code in ('SexAtBirth_Female', 'SexAtBirth_Male')"
        other_id = '0'
        other_name = 'Unknown'
        sql = sql.replace(":dataset",self.dataset)
        r = self.client.query(sql)
        r = r.to_dataframe()        
        _ids = [str(value) for value in r['concept_id'].tolist()]
        
        return self.__get_formatted_observations(_ids,other_name,other_id)

    def language(self):
        """
            filter by SpokenWrittenLanguage_
        """
        sql = "SELECT concept_id,concept_code,concept_name from :dataset.concept WHERE REGEXP_CONTAINS(concept_code,'Language_English')"
        other_id = '0'
        other_name = 'Unknown'
        sql = sql.replace(":dataset",self.dataset)
        r = self.client.query(sql)
        r = r.to_dataframe()        
        _ids = [str(value) for value in r['concept_id'].tolist()]
        
        return self.__get_formatted_observations(_ids,other_name,other_id)
    def employment(self):
        """
            This function will generalize employment
            This will have to be filtered by _EmploymentStatus
        """
        sql = "SELECT concept_id,concept_code,concept_name from :dataset.concept WHERE concept_code in ('EmploymentStatus_OutOfWorkOneOrMore','EmploymentStatus_EmployedForWages','EmploymentStatus_OutOfWorkLessThanOne')"
        other_id = '0'
        other_name = 'Unknown'
        sql = sql.replace(":dataset",self.dataset)
        r = self.client.query(sql)
        r = r.to_dataframe()        
        _ids = [str(value) for value in r['concept_id'].tolist()]
        
        return self.__get_formatted_observations(_ids,other_name,other_id)
def initialization(client,dataset):
    """
        This function will determine if the person_seed table needs to be destroyed and re-initialized
        I decided to make this function dummy proof to make it more user friendly

        :client     initialized big query client
        :dataset    dataset name
    """
    
    ref = client.dataset(dataset)
    has_table = len([1 for table in client.list_tables(ref) if table.table_id == 'people_seed']) > 0
    Logging.log(subject='composer',object='big.query',action='has.seed',value=(1*has_table))
    
    if has_table == True :
        #
        # The seeding table was found, we need to make sure the table has an acceptable level of consistency
        #
        # r_o = client.get_table(client.dataset(dataset).table("observation"))
        r_i = client.get_table(client.dataset(dataset).table("people_seed"))        
        sql="SELECT COUNT(DISTINCT person_id) as count FROM :i_dataset.observation ".replace(":i_dataset",dataset)
        
        r = client.query(sql).to_dataframe()
        Logging.log(subject="composer", object="make.seed",action="evaluate", value= r_i.num_rows/r['count'].values[0])
        if  r_i.num_rows / r['count'].values[0] < .9 :
            #
            # we need to destroy the seeding table and alert the user of this
            
            #
            
            ref = client.dataset(dataset).table("people_seed")            
            client.delete_table(ref)
            Logging.log(subject='composer',object='big.query',action='drop.table',value=ref.to_api_repr)
            has_table = True
    #
    # We create the table here if there was an error found
    
    if has_table == False :
        #
        # At this point we have either destroyed the table or it does NOT exist yet
        #

        sql = "SELECT person_id, DATE_DIFF(CURRENT_DATE,CAST(value_as_string as DATE) , DAY)+ CAST (700*rand() AS INT64) as seed FROM :i_dataset.observation WHERE observation_source_value = 'ExtraConsent_TodaysDate' GROUP BY person_id,value_as_string ORDER BY 1".replace(":i_dataset",dataset)
        job = bq.QueryJobConfig()
        job.destination = client.dataset(dataset).table("people_seed")
        job.use_query_cache = True
        job.allow_large_results = True
        # job.dry_run = True    
        r = client.query(sql,location='US',job_config=job)        
        Logging.log(subject='composer',object='big.query',action='create.table',value=r.job_id)

#
# The code below will implement the orchestration and parameter handling from the command line 
#             
if __name__ == '__main__' :
    #
    # Overriding config path with the actual configuration file and making sure it is available for use
    #
    path = SYS_ARGS['config']
    f = open(path)
    SYS_ARGS['config'] = json.loads(f.read())
    f.close()
    #
    # Once the configuration is available we can begin to create objects to do the work.
    #   - google cloud client
    #   - Initialize class level parameters
    #
    CONSTANTS = SYS_ARGS['config']['constants']
    account_path = CONSTANTS['service-account-path']    
    Policy.TERMS.SEXUAL_ORIENTATION_NOT_STRAIGHT= CONSTANTS['sexual-orientation']['not-straight']
    Policy.TERMS.SEXUAL_ORIENTATION_STRAIGHT    = CONSTANTS['sexual-orientation']['straight']
    Policy.TERMS.OBSERVATION_FILTERS            = CONSTANTS['observation-filter']
    Policy.TERMS.BEGIN_OF_TIME = '1980-07-21' if 'begin-of-time' not in CONSTANTS['begin-of-time'] else CONSTANTS['begin-of-time']
    client = bq.Client.from_service_account_json(account_path)

    #
    # Let's get the information about the dataset available
    i_dataset   = SYS_ARGS['i_dataset']
    table       = SYS_ARGS['table']    
    o_dataset   = SYS_ARGS['o_dataset']
    remove      = SYS_ARGS['config']['suppression'][table] if table in SYS_ARGS['config']['suppression'] else []
    
    initialization(client,i_dataset)

    #
    # The operation will be performed via the implementation of a form of iterator-design pattern
    # design information here https://en.wikipedia.org/wiki/Iterator_pattern
    #
    #
    # @TODO: perhaps vocabulary_id and constant_class_id can be removed
    #
    args = {"client":client,"vocabulary_id":'PPI',"concept_class_id":['Question','PPI Modifier'],"dataset":i_dataset,table:table,"remove":remove}
    container = [Shift(**args),DropFields(**args)]
    #
    # Let's see what we can do with the designated table, given our container of operations
    # Each item in the container is fully autonomous and will return a query that will have to be built by the calling code
    # The reason for this is because the operations are already convoluted as is: separation of concerns (https://en.wikipedia.org/wiki/Separation_of_concerns)
    #
    r       = {}
    for item in container :
        name    = item.name()
        #
        # @Log: We are logging here the operaton that is expected to take place
        # {"action":"building-sql","input":fields,"subject":table,"object":""}        
        
        p       =  item.can_do(i_dataset,table)   
        Logging.log(subject="composer",object=name,action="can.do",value= (i_dataset+"."+table) )             
        if p :
            r[name] = item.get(i_dataset,table)
        else:
            continue
    #
    # At this point we should start building the query i.e performing joins and unions
    #   - dropping fields performs a projection of a table given fields suppressed (should probably be renamed). Date/TimeStamp fields will be automatically dropped if not specified
    #   - Dates are shifted and will/should be joined against the fields of the previous step
    #   - In the advent of observation table (meta and relational) an additional union is added to the construction process
    #

    #
    # Let's get basic project of fields and provide a prefix to the query
    #
    fields  =  r['dropfields']['fields']
    # sql     = "SELECT :parent_fields FROM ("+r['dropfields']['sql']+") a"
    sql = r['dropfields']['sql']
    
    #
    # @Log: We are logging here the operaton that is expected to take place
    # {"action":"building-sql","input":fields,"subject":table,"object":""}
    

    if 'shift' in r :
        
        if 'join' in r['shift'] :
            #
            # @Log: We are logging here the operaton that is expected to take place
            # {"action":"building-sql","input":fields,"subject":table,"object":"join"}
            prefixed_fields = ['a.'+name for name in fields if name not in r['shift']['join']['fields']]            
            prefixed_fields +=['a.'+name for name in r['shift']['join']['fields'] ]
            
            prefixed_fields = ",".join(prefixed_fields)
            
            # join_sql = r['shift']['join']['sql']
            join_fields = ",".join(['']+r['shift']['join']['fields']) #-- should start with comma
            # sql = sql + " INNER JOIN (:sql) p ON p.person_id = a.person_id ".replace(":sql",join_sql)
            shifted_values = ","+ ",".join(r['shift']['join']['shifted_values'])
            
        else:
            prefixed_fields = ['a.'+name for name in fields if name not in fields ]
            join_fields = ""
            shifted_values = ""
        
        sql = sql.replace(":parent_fields",prefixed_fields).replace(":shifted_date_columns",shifted_values)
        
        if 'union' in r['shift'] :
            #
            # @Log: We are logging here the operaton that is expected to take place
            # {"action":"building-sql","input":fields,"subject":table,"object":"union"}
            union_sql = r['shift']['union']['sql']
            non_union_fields = list(set(fields) - set(r['shift']['union']['fields']))
            non_union_fields = ",".join([' ']+non_union_fields)
            union_sql = union_sql.replace(":fields",non_union_fields)
            sql = sql + " UNION ALL SELECT :fields :joined_fields FROM ( :sql ) ".replace(":sql",union_sql)    
            
            sql = sql.replace(":fields",",".join(fields)).replace(":joined_fields",join_fields)            
            
        else:
            pass
    sql = sql.replace(":shifted_date_columns",shifted_values)
    #
    # At this point we should submit the sql query with information about the target
    #
    FILTER = [ ]
    fields = ",".join(fields) + join_fields 

    if 'rows' in remove :
        #
        # The user has specified rows to be removed from the final results
        # In other words the fields that are not to be included
        FILTER = ["WHERE"]
        #
        # @Log: We are logging here the operaton that is expected to take place
        # {"action":"submit-sql","input":remove['rows'].keys(),"subject":table,"object":"bq"}        
        for field in remove['rows'] :
            #
            # @NOTE:
            # This particular filter is to express rows to be removed from the resultset
            #
            values = "|".join(remove['rows'][field])
            filter = "".join(["REGEXP_CONTAINS(",field,",'",values,"') IS FALSE"])
            if len(FILTER) > 1 :
                filter = (" AND " + filter)
            FILTER.append(filter)
    
    if 'filter' in SYS_ARGS :
        #
        # @Log: We are logging here the operaton that is expected to take place
        # {"action":"building-sql","input":fields,"subject":table,"object":"filter"}
        if 'WHERE' not in FILTER :
            FILTER += ["WHERE"]            
        else:
            FILTER += ["AND"]
        FILTER += [SYS_ARGS['filter']]
    #
    # This is not ideal but we have to remove a portion of the population given their age
    # For now we hard code this instruction and set the age as a parameter
    # @TODO: ... urgh!!
    #

    if 'exclude-age' in CONSTANTS :
        EXCLUDE_AGE_SQL = "person_id not in (SELECT person_id FROM :i_dataset.observation where observation_source_value = 'PIIBirthInformation_BirthDate' and DATE_DIFF(CURRENT_DATE, CAST(value_as_string AS DATE),YEAR) > :age)"
        EXCLUDE_AGE_SQL = EXCLUDE_AGE_SQL.replace(":age",str(CONSTANTS['exclude-age'])).replace(":i_dataset",i_dataset)
        if 'rows' in remove or 'filter' in SYS_ARGS :
            EXCLUDE_AGE_SQL = ['AND', EXCLUDE_AGE_SQL]
        else:
            EXCLUDE_AGE_SQL = ['WHERE', EXCLUDE_AGE_SQL]

        FILTER += EXCLUDE_AGE_SQL
    FILTER = " ".join(FILTER)
    
    sql = "SELECT * :dropped_fields FROM ("+sql+") "+FILTER
    
    #
    # Bug-fix:
    #   Insuring the tables maintain their structural integrity
    dropped_fields = Policy.get_dropped_fields(remove['columns']) if 'columns' in remove else []
    if len(dropped_fields) > 0 :
        dropped_fields = ","+",".join(dropped_fields)
    else:
        dropped_fields = ""
    # print sql
    Logging.log(subject='composer',object=table,action='formatted.removed.columns',value=remove['columns'])
    sql = sql.replace(":dropped_fields",dropped_fields)
    # print sql  
    #
    # @Log: We are logging here the operaton that is expected to take place
    # {"action":"submit-sql","input":fields,"subject":table,"object":"bq"}      
    
    #
    # @TODO: Make sure the o_dataset exists if it doesn't just create it (it's simpler)  
    #
    job = bq.QueryJobConfig()
    job.destination = client.dataset(o_dataset).table(table)
    job.use_query_cache = True
    job.allow_large_results = True
    job.priority = 'BATCH'
    # job.dry_run = True    
    r = client.query(sql,location='US',job_config=job)

    #
    # @Log: We are logging here the operaton that is expected to take place
    # {"action":"submit-sql","input":job.job_id,"subject":table,"object":{"status":job.state,"running""job.running}}     
    print r.job_id,r.state,r.running() ,r.errors
    Logging.log(subject="composer",object=r.job_id,action="submit.job",value={"from":i_dataset+"."+table,"to":o_dataset})
    # print dir(r)
    #@TODO: monitor jobs once submitted
    pass
