# ---
# jupyter:
#   jupytext:
#     formats: py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.7.1
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# # Set Up

# parameters
rt_dataset = ""
ct_ser_dataset = ""
new_ct_dataset = ""
cur_project = ""
cur_out_project = ""

rt_dataset = 'R2020q4r1_antibody_quest'
ct_ser_dataset = 'C2021Q3R2_serology'
new_ct_dataset = 'C2021q3r2_antibody_quest'
cur_project = 'aou-res-curation-prod'
cur_out_project = 'aou-res-curation-output-prod'

# +
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')

from analytics.cdr_ops.notebook_utils import execute
from common import JINJA_ENV
from utils import auth
from utils.bq import get_client
client = get_client(project_id)


# -

# # QC

def get_table(table, cols, dataset, project):
    query = JINJA_ENV.from_string("""
            SELECT {cols} 
            FROM `{dataset}.{table}` 
            """
    df = pd.read_gbq(query, project_id = project)
    return df


def get_data_input(rt_dataset, ct_ser_dataset, new_ct_dataset, cur_out_project, cur_project):
    
    ##########
    query = JINJA_ENV.from_string("""
                    SELECT distinct serology_person_id
                            , CASE WHEN biobank_id LIKE 'PIO%' THEN 'Vanderbilt'
                                WHEN biobank_id LIKE 'ST%' THEN 'Mayo'
                                WHEN biobank_id LIKE '0%' THEN 'Boston' end AS Provider              
                    FROM `{{rt_dataset}}.person` 
                    LEFT JOIN `{{rt_dataset}}.pid_sid_map` USING(serology_person_id)
                    WHERE control_status = 'Positive' """)
    
    pos_controls_query = query.render(project_id=cur_project, rt_dataset=rt_dataset)
    pos_controls_provider =execute(client, pos_controls_query)


    ############
    query = JINJA_ENV.from_string(""" SELECT distinct biobank_id, serology_person_id
                        FROM `{{rt_dataset}}.mayo_person` 
                        JOIN `{{rt_dataset}}.pid_sid_map` USING(biobank_id)""")

    mayo_positive_query = query.render(project_id=cur_project, rt_dataset=rt_dataset)
    mayo_positive_controls =execute(client, mayo_positive_query)

    
    ############
    query = JINJA_ENV.from_string("""SELECT DISTINCT serology_person_id, person_id 
                                    FROM `{{ct_ser_dataset}}.serology_person` """)
    ct_person_query = query.render(project_id=cur_out_project, ct_ser_dataset=ct_ser_dataset)
    ct_person_table =execute(client, ct_person_query)

    ############
    query = JINJA_ENV.from_string(""" SELECT DISTINCT * 
                                    FROM `{{new_ct_dataset}}.serology_person` """)  
    person_query = query.render(project_id=cur_project, new_ct_dataset=new_ct_dataset)
    new_ct_person_table =execute(client, person_query)

    ############
    query = JINJA_ENV.from_string(""" SELECT distinct table_name, column_name
                        FROM {{new_ct_dataset}}.INFORMATION_SCHEMA.COLUMNS""")
    schema_query = query.render(project_id=cur_project, new_ct_dataset=new_ct_dataset)
    schema =execute(client, schema_query)
    
     
    return pos_controls_provider, mayo_positive_controls, ct_person_table, new_ct_person_table, schema


def serology_dataset_qc(new_ct_dataset, rt_dataset, ct_ser_dataset, cur_project, cur_out_project):
        
    
    pos_controls_provider, mayo_positive_controls, ct_person_table, new_ct_person_table, schema = \
                            get_data_input(rt_dataset = rt_dataset, ct_ser_dataset = ct_ser_dataset
                                           , new_ct_dataset = new_ct_dataset
                                           , cur_out_project=cur_out_project, cur_project = cur_project)
    
    print('QC FOR dataset '+new_ct_dataset+". If no 'Fail' is printed out, it means the QC Passed.")

    
    print('\n################################################ QC1 ####################################################')
    print("Check that there are no individual from Mayo's positive controls in the dataset at all.\n")
    
    pid_col = 'serology_person_id'
    all_tables_pids = []
    for table in schema.table_name.unique():
        if pid_col in schema[schema.table_name == table].column_name.unique():
            df = get_table(dataset = new_ct_dataset, table = table, project=cur_project, cols = 'serology_person_id')
            pids = list(set(df[pid_col]))
            all_tables_pids = all_tables_pids+pids

    n_common_pids = len(set(all_tables_pids).intersection(set(mayo_positive_controls['serology_person_id'])))
    
    if n_common_pids!=0:
        print("\033[1;31m"+"    Fail: "+str(n_common_pids)+" individual(s) from Mayo's positive controls are found in "\
              + new_ct_dataset+" ."+"\033[0;0m")


    print('\n############################################## QC2 ####################################################')
    print('Check that none of the tables in '+new_ct_dataset+' have any data for participants not included in '+ct_ser_dataset+'.')
    
    pids_not_in_ct = set(all_tables_pids) - set(ct_person_table[pid_col])

    if len(pids_not_in_ct) !=0:
        print('\n')
        print("\033[1;31m"+'   Fail! '+ str(len(pids_not_in_ct)) +' pids in '+new_ct_dataset+' are not in '+ct_ser_dataset+"\033[0;0m")

    print('\n\n############################################## QC3 ####################################################')
    print(''' In '''+new_ct_dataset+'''.serology_person, check that:
    
    - All participants regardless of control_status have a serology_person_id and a control_status
        
    - VUMC non-controls and VUMC controls (neg controls):
        - have person_id
        - do **not** have demographics. Their person_ids will be used to link to CDR person.
            
    - Positive controls (non VUMC; from Boston and Vanderbilt U.):     
        - do **not** have person_id since they are not in AoU database
        - Boston Positive Controls do **not** have demographics becasye they did not provide any
        - Vanderbilt U. have demographics (provided by Vanderbilt U.)''')
    
    print('\n')
    
    ## 0 check that all pids, regardless of control status 
    if True in new_ct_person_table[['serology_person_id', 'control_status']].isna().drop_duplicates().any().unique():
        print("\033[1;31m"+'   Fail! Some individuals do not have serology_person_id and/or control_status in '+new_ct_dataset+\
              '.serology_person. All participants in the serology dataset should have this'+"\033[0;0m")

    # 1 VUMC non-controls and VUMC controls (neg controls)
    non_and_neg_controls = new_ct_person_table[new_ct_person_table.control_status.isin(['Non-Control','Negative'])]
    non_and_neg_controls_demog = non_and_neg_controls.drop(['serology_person_id','collection_date'
                                            , 'control_status', 'person_id'],1).drop_duplicates()
   
     ## check that they have person_id in the person table
    if non_and_neg_controls['person_id'].isna().any() == True:
        print("\033[1;31m"+'   Fail! Some non controls (aou participants) are missing person_id in '+new_ct_dataset+\
                      '.serology_person. They need it to be able to link to CDR person table demographics.'+"\033[0;0m")

     ## check that they DO NOT have demograohic data in the person table
    if False in non_and_neg_controls_demog.isna().any().unique():
        print("\033[1;31m"+'   Fail! Some non controls (aou participants) have demographics data in '+new_ct_dataset+\
              '.serology_person.'+"\033[0;0m")
    

    # 2 positive Controls:
    pos_controls = new_ct_person_table[new_ct_person_table.control_status == 'Positive']
                                                                     
    ## check that positive controls do not have person_id in the person table
    if pos_controls['person_id'].isna().any() == False:
        print("\033[1;31m"+'   Fail! Some positive controls (non aou participants) have person_id in '+new_ct_dataset+\
                      '.serology_person.'+"\033[0;0m")
                                                                         
    # 3 check that Boston positive controls DO NOT have demograohic data in the person table
    non_demog_cols =['serology_person_id','collection_date', 'control_status', 'person_id', 'control_status', 'Provider']

    boston_pos_controls = pos_controls.merge(pos_controls_provider[pos_controls_provider.Provider == 'Boston'])
    boston_pos_controls_demog = boston_pos_controls.drop(non_demog_cols,1).drop_duplicates()
                                                                         
    if False in boston_pos_controls_demog.isna().any().unique():
        print("\033[1;31m"+'   Fail! Some Boston positive controls have demographics data in '+new_ct_dataset+'.serology_person.'+"\033[0;0m")
                                                                     
    # 4 check that Vanderbilt U positive controls have demograohic data in the person table
    vandi_pos_controls = pos_controls.merge(pos_controls_provider[pos_controls_provider.Provider == 'Vanderbilt'])
    vandi_pos_controls_demog = vandi_pos_controls.drop(non_demog_cols,1).drop_duplicates()
                                                                         
    if False not in vandi_pos_controls_demog.isna().all().unique():
        #display(vandi_pos_controls)
        print("\033[1;31m"+'   Fail! Vanderbilt positive controls do not have demographics data in '+new_ct_dataset+\
              '.serology_person.'+"\033[0;0m")

serology_dataset_qc(new_ct_dataset = new_ct_dataset, rt_dataset = rt_dataset
                    , ct_ser_dataset = ct_ser_dataset, dataset_project = dataset_project, project = project)


