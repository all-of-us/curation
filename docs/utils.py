
# author: Aahlad 
# date  : Aug 11th, 2017
# This is the utility class to help clarify the main bigquery loading code

import cloudstorage

class bqTable:
    _project = None
    
    def __init__(self, dataset_name, table_name):
        self.dataset = dataset_name
        self.name = table_name

class cloudStorageFile:
    
    # _project needs to be set before submitting a job
    _project = None

    def __init__(self, bucket_name, filename):
        # all necessary details for each file
        self.bucket = bucket_name
        self.name = filename
        self.gcs_url  = "gs://{}/{}".format(self.bucket,self.name)
        self.access = self.verify_existence()

    def verify_existence(self):
        try:
            _ = cloudstorage.open(self.gcs_url, mode='r')
            return True
        except:
            return False

class jobBody:

    _project = None

    def __init__():
        print "try to initialize with cloud_storage_file and bq_table; try."

    def __init__(self, cloud_storage_file, bq_table, schema, job_type = 'load'):
        # cloud_storage_file and bq_table are both objects from the classes
        # cloudStorageFile and bqTable
        # schema must be declared
        # this is a limited body descripttion with a lot of default settings
        
        self.bq_table = bq_table
        self.cloud_storage_file = None # <- for non-load jobs

        if job_type != 'load':
            # temporary scope of the application
            raise ValueError('job_type can only be load for now')
        #else:
        #    self.cloud_storage_file = cloud_storage_file
        #    self.body = self.load_job_body()
    
    def load_job_body(self, schema =  {"fields" : [ {"HOW HOW HOW?"} ]}):
        # load job specific body

        if schema.get("fields", None) is None:
            raise ValueError('Invalid schema; must contain key : "fields"')

        for key in ['name','mode','type']:
            if schema['fields'].get(key,None) is None:
                raise ValueError('key: {} missing'.format(key))

        job_body = {}
        job_body['configuration'] = {}
        job_body['configuration']['load'] = {
          "sourceUris": [self.cloud_storage_file.gcs_url], 
          "schema": schema, # { "fields": [ { "name": None, "type": None, "mode": None, "description": None } ] },
          "destinationTable": { 
              "projectId": self._project, 
              "datasetId": self.bq_table.dataset, 
              "tableId": self.bq_table.name },
          "maxBadRecords": 1000,
          # "schemaInlineFormat": None,
          # "schemaInline": None,
          "ignoreUnknownValues": False,
          "autodetect": False
        }       

        return job_body

empty_load_body = {
  "configuration": {
    "load": {
      "sourceUris": [ None ], 
      "schema": { "fields": [ { "name": None, "type": None, "mode": None, "description": None } ] },
      "destinationTable": { "projectId": None, "datasetId": None, "tableId": None },
      "maxBadRecords": 1000,
      "schemaInlineFormat": None,
      "schemaInline": None,
      "ignoreUnknownValues": False,
      "autodetect": False,
    }
  }
}
