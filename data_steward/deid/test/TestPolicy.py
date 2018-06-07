"""
    Steve L. Nyemba <steve.l.nyemba@vanderbilt.edu>
    Vanderbilt University Medical Center
    
    This file will test the application of a policy provided some arbitrary meta data that is from bigquery.
    
"""
from google.cloud.bigquery import SchemaField 
from google.cloud import bigquery as bq

META = {}
META['concept'] = [SchemaField(u'concept_id', u'INTEGER', u'REQUIRED', None, ()), SchemaField(u'concept_name', u'STRING', u'REQUIRED', None, ()), SchemaField(u'domain_id', u'STRING', u'REQUIRED', None, ()), SchemaField(u'vocabulary_id', u'STRING', u'REQUIRED', None, ()), SchemaField(u'concept_class_id', u'STRING', u'REQUIRED', None, ()), SchemaField(u'standard_concept', u'STRING', u'NULLABLE', None, ()), SchemaField(u'concept_code', u'STRING', u'REQUIRED', None, ()), SchemaField(u'valid_start_date', u'STRING', u'REQUIRED', None, ()), SchemaField(u'valid_end_date', u'STRING', u'REQUIRED', None, ()), SchemaField(u'invalid_reason', u'STRING', u'NULLABLE', None, ())]
META['person']  = [SchemaField(u'person_id', u'INTEGER', u'NULLABLE', None, ()), SchemaField(u'gender_concept_id', u'INTEGER', u'NULLABLE', None, ()), SchemaField(u'year_of_birth', u'INTEGER', u'NULLABLE', None, ()), SchemaField(u'month_of_birth', u'INTEGER', u'NULLABLE', None, ()), SchemaField(u'day_of_birth', u'INTEGER', u'NULLABLE', None, ()), SchemaField(u'birth_datetime', u'TIMESTAMP', u'NULLABLE', None, ()), SchemaField(u'race_concept_id', u'INTEGER', u'NULLABLE', None, ()), SchemaField(u'ethnicity_concept_id', u'INTEGER', u'NULLABLE', None, ()), SchemaField(u'location_id', u'INTEGER', u'NULLABLE', None, ()), SchemaField(u'provider_id', u'INTEGER', u'NULLABLE', None, ()), SchemaField(u'care_site_id', u'INTEGER', u'NULLABLE', None, ()), SchemaField(u'person_source_value', u'STRING', u'NULLABLE', None, ()), SchemaField(u'gender_source_value', u'STRING', u'NULLABLE', None, ()), SchemaField(u'gender_source_concept_id', u'INTEGER', u'NULLABLE', None, ()), SchemaField(u'race_source_value', u'STRING', u'NULLABLE', None, ()), SchemaField(u'race_source_concept_id', u'INTEGER', u'NULLABLE', None, ()), SchemaField(u'ethnicity_source_value', u'STRING', u'NULLABLE', None, ()), SchemaField(u'ethnicity_source_concept_id', u'INTEGER', u'NULLABLE', None, ())]
META['death']   = [SchemaField(u'person_id', u'INTEGER', u'NULLABLE', None, ()), SchemaField(u'death_date', u'DATE', u'NULLABLE', None, ()), SchemaField(u'death_datetime', u'TIMESTAMP', u'NULLABLE', None, ()), SchemaField(u'death_type_concept_id', u'INTEGER', u'NULLABLE', None, ()), SchemaField(u'cause_concept_id', u'INTEGER', u'NULLABLE', None, ()), SchemaField(u'cause_source_value', u'STRING', u'NULLABLE', None, ()), SchemaField(u'cause_source_concept_id', u'INTEGER', u'NULLABLE', None, ())]
META['observation'] = [SchemaField(u'observation_id', u'INTEGER', u'NULLABLE', None, ()), SchemaField(u'person_id', u'INTEGER', u'NULLABLE', None, ()), SchemaField(u'observation_concept_id', u'INTEGER', u'NULLABLE', None, ()), SchemaField(u'observation_date', u'DATE', u'NULLABLE', None, ()), SchemaField(u'observation_datetime', u'TIMESTAMP', u'NULLABLE', None, ()), SchemaField(u'observation_type_concept_id', u'INTEGER', u'NULLABLE', None, ()), SchemaField(u'value_as_number', u'FLOAT', u'NULLABLE', None, ()), SchemaField(u'value_as_string', u'STRING', u'NULLABLE', None, ()), SchemaField(u'value_as_concept_id', u'INTEGER', u'NULLABLE', None, ()), SchemaField(u'qualifier_concept_id', u'INTEGER', u'NULLABLE', None, ()), SchemaField(u'unit_concept_id', u'INTEGER', u'NULLABLE', None, ()), SchemaField(u'provider_id', u'INTEGER', u'NULLABLE', None, ()), SchemaField(u'visit_occurrence_id', u'INTEGER', u'NULLABLE', None, ()), SchemaField(u'observation_source_value', u'STRING', u'NULLABLE', None, ()), SchemaField(u'observation_source_concept_id', u'INTEGER', u'NULLABLE', None, ()), SchemaField(u'unit_source_value', u'STRING', u'NULLABLE', None, ()), SchemaField(u'qualifier_source_value', u'STRING', u'NULLABLE', None, ()), SchemaField(u'value_source_concept_id', u'INTEGER', u'NULLABLE', None, ()), SchemaField(u'value_source_value', u'STRING', u'NULLABLE', None, ()), SchemaField(u'questionnaire_response_id', u'INTEGER', u'NULLABLE', None, ())]
POLICY = {
        "constants":{
            "service-account-path":"",
            "sexual-orientation":{"straight":"SexualOrientation_Straight","not-straight":"SexualOrientation_None"},
            "observation-filter":{"race":"Race_WhatRace","gender":"Gender","orientation":"Orientation","employment":"_EmploymentStatus","sex_at_birth":"BiologicalSexAtBirth_SexAtBirth","language":"Language_SpokenWrittenLanguage","education":"EducationLevel_HighestGrade"},
            "begin-of-time":"2010-01-18",
            "exclude-age":89
        },
        "suppression":{
            "person":["month_of_birth","day_of_birth","day_of_birth"]
        }

    }
ACCOUNT_PATH='/home/steve/git/rdc/deid/config/account/account.json'
client = bq.Client.from_service_account_json(ACCOUNT_PATH)
import unittest
from deid import Shift, DropFields, Group
import os
import json
class TestPolicy(unittest.TestCase):
    def test_job_status(self):
        files = os.listdir('logs')

        for filename in files :
            f = open("logs"+os.sep+filename)
            for line in f :
                if 'submit' in line :
                    p = json.loads(line)
                    id =  p['object']
                    job = client.get_job(id)
                    print id,p['value'],job.state,'\t', 'FAILED' if job.errors is not None else 'PASSED'
                    # print id,'\t',job.job_type,'\t',job.running(),'\t',job.state,'\t',job.errors
            f.close()
    def test_suppression(self):
        pass                       
    def test_shifting(self):
        pass
    def test_generalization(self):
        pass
    def tes_design_pattern(self):
            pass
if __name__ == '__main__' :
    # unittest.main()
    r = client.query("select * from raw.person limit 1")
    print [name for name in dir (r) if 'to_' in name]
    # print dir(client.get_job("046a2fff-ea1a-41ab-af6a-7b3da26a0ad8"))
