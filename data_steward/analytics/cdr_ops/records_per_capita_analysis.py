# -*- coding: utf-8 -*-
# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.4.2
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# ## Notebook is used to complete with [EDQ-383](https://precisionmedicineinitiative.atlassian.net/browse/EDQ-383)
#
#
# #### Background
#
#
# In an attempt to start ‘benchmarking’ sites, we want to understand how many ‘records per participant' and ‘records per participant per year’ there are for each site. This kind of information could enable us to better understand which sites may benefit from ETL changes.

from google.cloud import bigquery
# %reload_ext google.cloud.bigquery
client = bigquery.Client()
# %load_ext google.cloud.bigquery

# %matplotlib inline
from notebooks import parameters
from utils import bq
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# +
DATASET = parameters.LATEST_DATASET

print("""
DATASET TO USE: {}
""".format(DATASET))


# -


def get_records_per_capita(table_name, number_records, capita_string, dataset):
    """
    Function is used to generate a query that shows the number of records, number of
    persons, and number of records per person for each of the health provider
    organizations.
    
    This query is then put into BigQuery and translated into a dataframe that can
    be used for analysis purposes. This dataframe has the following:
        - for each HPO ID
            - number of records for the site
            - number of persons for the site
            - number of 'records per capita'
                 
                 
    
    Parameters
    ----------
    table_name (str): represents the name of the OMOP table to be investigated
                      (example: drug_exposure)
                      
    number_records (str): used for the title of the column in the dataframe
                          to represent the number of records
                          
    capita_string (str): represents the title of the column in the dataframe
                         used to represent the records per capita
                         
    dataset (str): dataset to be queried (defined at the top of the workbook)
    
    
    Returns
    -------
    dataframe (df): contains the information specified in the top of the docstring
    """

    query = """
    SELECT
    DISTINCT
    mx.src_hpo_id, 
    COUNT(x.{table_name}_id) as {number_records}, 
    COUNT(DISTINCT p.person_id) as num_persons_for_site,
    ROUND(COUNT(x.{table_name}_id) / COUNT(DISTINCT p.person_id)) as {capita_string}
    FROM
    `{dataset}.unioned_ehr_{table_name}` x
    JOIN
    `{dataset}.unioned_ehr_person` p
    ON
    x.person_id = p.person_id 
    JOIN
    `{dataset}._mapping_{table_name}` mx
    ON
    x.{table_name}_id = mx.{table_name}_id
    GROUP BY 1
    ORDER BY {capita_string} DESC
    """.format(table_name=table_name,
               number_records=number_records,
               capita_string=capita_string,
               dataset=dataset)

    dataframe = pd.io.gbq.read_gbq(query, dialect='standard')

    return dataframe


def add_total_records_per_capita_row(dataframe, number_records, capita_string):
    """
    Function is used to add a 'total' row at the bottom of a dataframe that shows the
    relative 'records per capita' for the particular dataframe
    
    This row will show:
        a. the number of rows in the table
        b. the number of persons contributing data to the table
        c. the total number of rows divided by the persons in the table
    
    Parameters:
    ----------
    dataframe (df): dataframe for a particular table. shows a-c (above) for each of the
        HPOs that uploaded data
                              
    number_records (str): used for the title of the column in the dataframe
                          to represent the number of records
                          
    capita_string (str): represents the title of the column in the dataframe
                         used to represent the records per capita
        
    Returns:
    --------
    dataframe (df): the inputted dataframe with an additional 'total' row at the end
    """

    dataframe = dataframe.append(
        dataframe.sum(numeric_only=True).rename('Total'))

    hpo_names = dataframe['src_hpo_id'].tolist()

    hpo_names[-1:] = ["Total"]

    dataframe['src_hpo_id'] = hpo_names

    records_total = dataframe.loc['Total'][number_records]

    persons_total = dataframe.loc['Total']['num_persons_for_site']

    total_records_per_capita = round((records_total) / persons_total, 2)

    dataframe.at['Total', capita_string] = total_records_per_capita

    return dataframe


# ### Graphing Functions


def create_dicts_w_info(df, x_label, column_label):
    """
    This function is used to create a dictionary that can be easily converted to a
    graphical representation based on the values for a particular dataframe
    
    Parameters
    ----------
    df (dataframe): dataframe that contains the information to be converted
    
    x_label (string): the column of the dataframe whose rows will then be converted
        to they keys of a dictionary
    
    column_label (string): the column that contains the data quality metric being
        investigated
    
    Returns
    -------
    data_qual_info (dictionary): has the following structure
        
        keys: the column for a particular dataframe that represents the elements that
            whose data quality is being compared (e.g. HPOs, different measurement/unit
            combinations)
        
        values: the data quality metric being compared
    """
    rows = df[x_label].unique().tolist()

    data_qual_info = {}

    for row in rows:
        sample_df = df.loc[df[x_label] == row]

        data = sample_df.iloc[0][column_label]

        data_qual_info[row] = data

    return data_qual_info


def create_graphs(info_dict, xlabel, ylabel, title, img_name, color,
                  total_diff_color, turnoff_x):
    """
    Function is used to create a bar graph for a particular dictionary with information about
    data quality
    
    Parameters
    ----------
    info_dict (dictionary): contains information about data quality. The keys for the dictionary
        will serve as the x-axis labels whereas the values should serve as the 'y-value' for the
        particular bar
        
    xlabel (str): label to display across the x-axis
    
    ylabel (str): label to display across the y-axis
    
    title (str): title for the graph
    
    img_name (str): image used to save the image to the local repository
    
    color (str): character used to specify the colours of the bars
    
    total_diff_color (bool): indicates whether or not the last bar should be coloured red (
        as opposed to the rest of the bars on the graph). This is typically used when the ultimate
        value of the dictionary is of particular important (e.g. representing an 'aggregate' metric
        across all of the sites)
        
    turnoff_x (bool): used to disable the x-axis labels (for each of the bars). This is typically used
        when there are so many x-axis labels that they overlap and obscure legibility
    """
    bar_list = plt.bar(range(len(info_dict)),
                       list(info_dict.values()),
                       align='center',
                       color=color)

    # used to change the color of the 'aggregate' column; usually implemented for an average
    if total_diff_color:
        bar_list[len(info_dict) - 1].set_color('r')

    if not turnoff_x:
        plt.xticks(range(len(info_dict)),
                   list(info_dict.keys()),
                   rotation='vertical')
    else:
        plt.xticks([])

    plt.ylabel(ylabel)
    plt.xlabel(xlabel)
    plt.title(title)
    plt.show()
    plt.savefig(img_name, bbox_inches="tight")


# #### Looking at the drug exposure table

number_records = 'number_of_drugs'
capita_string = 'drugs_per_capita'

drug_exposure_results = get_records_per_capita(table_name='drug_exposure',
                                               number_records=number_records,
                                               capita_string=capita_string,
                                               dataset=DATASET)

drug_exposure_results = add_total_records_per_capita_row(
    dataframe=drug_exposure_results,
    number_records=number_records,
    capita_string=capita_string)

drug_exposure_dict = create_dicts_w_info(df=drug_exposure_results,
                                         x_label='src_hpo_id',
                                         column_label='drugs_per_capita')

create_graphs(drug_exposure_dict,
              xlabel='HPO',
              ylabel='Drug Records Per Capita',
              title='Drugs Per Capita',
              img_name='{}.jpg'.format(capita_string),
              color='b',
              total_diff_color=True,
              turnoff_x=False)

# #### Looking at the visit occurrence table

# +
number_records = 'number_of_visits'
capita_string = 'visits_per_capita'

drug_exposure_results = get_records_per_capita(table_name='visit_occurrence',
                                               number_records=number_records,
                                               capita_string=capita_string,
                                               dataset=DATASET)

drug_exposure_results = add_total_records_per_capita_row(
    dataframe=drug_exposure_results,
    number_records=number_records,
    capita_string=capita_string)

drug_exposure_dict = create_dicts_w_info(df=drug_exposure_results,
                                         x_label='src_hpo_id',
                                         column_label='visits_per_capita')

create_graphs(drug_exposure_dict,
              xlabel='HPO',
              ylabel='Visit Records Per Capita',
              title='Visits Per Capita',
              img_name='{}.jpg'.format(capita_string),
              color='b',
              total_diff_color=True,
              turnoff_x=False)
# -

# #### Looking at the measurement table

# +
number_records = 'number_of_measurements'
capita_string = 'measurements_per_capita'

drug_exposure_results = get_records_per_capita(table_name='measurement',
                                               number_records=number_records,
                                               capita_string=capita_string,
                                               dataset=DATASET)

drug_exposure_results = add_total_records_per_capita_row(
    dataframe=drug_exposure_results,
    number_records=number_records,
    capita_string=capita_string)

drug_exposure_dict = create_dicts_w_info(df=drug_exposure_results,
                                         x_label='src_hpo_id',
                                         column_label=capita_string)

create_graphs(drug_exposure_dict,
              xlabel='HPO',
              ylabel='Measurement Records Per Capita',
              title='Measurements Per Capita',
              img_name='{}.jpg'.format(capita_string),
              color='b',
              total_diff_color=True,
              turnoff_x=False)
# -

# #### Looking at the procedure occurrence table

# +
number_records = 'number_of_procedures'
capita_string = 'procedures_per_capita'

drug_exposure_results = get_records_per_capita(
    table_name='procedure_occurrence',
    number_records=number_records,
    capita_string=capita_string,
    dataset=DATASET)

drug_exposure_results = add_total_records_per_capita_row(
    dataframe=drug_exposure_results,
    number_records=number_records,
    capita_string=capita_string)

drug_exposure_dict = create_dicts_w_info(df=drug_exposure_results,
                                         x_label='src_hpo_id',
                                         column_label=capita_string)

create_graphs(drug_exposure_dict,
              xlabel='HPO',
              ylabel='Procedure Records Per Capita',
              title='Procedures Per Capita',
              img_name='{}.jpg'.format(capita_string),
              color='b',
              total_diff_color=True,
              turnoff_x=False)
# -

# #### Looking at the condition occurrence table

# +
number_records = 'number_of_conditions'
capita_string = 'conditions_per_capita'

drug_exposure_results = get_records_per_capita(
    table_name='condition_occurrence',
    number_records=number_records,
    capita_string=capita_string,
    dataset=DATASET)

drug_exposure_results = add_total_records_per_capita_row(
    dataframe=drug_exposure_results,
    number_records=number_records,
    capita_string=capita_string)

drug_exposure_dict = create_dicts_w_info(df=drug_exposure_results,
                                         x_label='src_hpo_id',
                                         column_label=capita_string)

create_graphs(drug_exposure_dict,
              xlabel='HPO',
              ylabel='Condition Records Per Capita',
              title='Conditions Per Capita',
              img_name='{}.jpg'.format(capita_string),
              color='b',
              total_diff_color=True,
              turnoff_x=False)
