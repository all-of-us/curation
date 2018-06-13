"""
    This file will run de-identification using pandas big query
    Design:
        The terminology used throughout the code is that of relational theory/principles.
        We have identified two types of tables :
        - relational tables : The semantics are defined in the structure of the table
        - meta tables       : The semantics are defined in the content and sometimes the structure
    
    The obvious limitation of this is that it will load all the data into memory 
"""
from __future__ import division
from google.cloud import bigquery as bq
import pandas as pd
import json
class Policy():
    def __init__(self,**args):
        # if 'client' in args :
        #     self.client = args['client']
        # elif 'path' in args:
        #     self.client = bq.Client.from_service_account_json(args['path'])
        self.path = args['path']
        self.client = bq.Client.from_service_account_json(args['path'])
        self.project_id = args['project_id'] if 'project_id' in args else None
    def init(self,**args):
        if 'dataset' in args and 'table' in args :
            self.sql = "SELECT * FROM :dataset.:table".replace(":dataset",args['dataset']).replace(":table",args['table'])
        else:
            self.sql = args['sql']
        #
        # Loading the table at this point into a data frame 
        if self.project_id is None :
            f = open(path)
            p = json.loads(f.read())
            self.project_id = p['project_id']
            f.close()
        self.df = None
        try:
            self.df = pd.read_gbq(sql,project_id=self.project_id,private_key=self.path)            
            #
            # at this point we have the list of fields to remove and we need to add date fields
            # Datefields will be processed differently i.e they will require shifting ...
            #
            ltypes = self.df.ftype
            ref = set(['date','datetime','timestamp'])
            [ for id in  self.columns if ( set(ltypes[id].replace(':dense','').replace('64','').split('[ns]')) & set() ) or (set(id.split('_')) & ref)]
        except Exception,e:
            print (e)
            #
            #@TODO: Log the error and terminate gracefully
        #
        # @TODO: Log the status of the operation here (success|failure) i.e test self.df
        #
    def can_do(self,**args) :
        return False
    def get_tuples(self) :
        return []
    def do(self):
        pass
class Suppress(Policy):
    """
        This class will implement suppression for both relational tables and meta-tables.
        The class will additionally handle the case of meta tables that have relational attributes that need suppression

    """
    def __init__(self,**args):
        """
            Initiate suppression of attributes in a table
            @params remove     list of fields to be removed in addition Date/TimeStamps will be removed (no need to specify them)
        """
        self.remove_fields = args['remove'] if 'remove' in args else []
        
    def basic(self):
        """
            This will remove basic fields from the 
        """
        pass
    def meta(self):
        pass