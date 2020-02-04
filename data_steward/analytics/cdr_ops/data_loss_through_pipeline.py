# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.3.0
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# ### This notebook is used to model how the volume of data changes at different points in the pipeline
#
# The different datasets are as follows:
# - unioned_ehr (submitted directly from sites - no cleaning rules applied)
# - combined datasets
# - deidentified datasets
#
# The dimensions of 'data volume' are as follows (for each table):
# - number of participants
# - number of records

from notebooks import bq, render, parameters
import pandas as pd
import numpy as np
import six
import matplotlib.pyplot as plt

# +
unioned = parameters.UNIONED_EHR_DATASET_COMBINED
combined = parameters.COMBINED_DATASET_ID
deid = parameters.DEID_DATASET_ID

print("""
Unioned Dataset: {unioned}
Combined Dataset: {combined}
De-ID Dataset: {deid}
""".format(unioned = unioned, combined = combined, deid = deid))


# -

def generate_query(dataset, person_var, record_var, table_name, field_name):
    """
    This function is used to:
        a. generate a string that can be fed into BigQuery
        b. create a dataframe that contains information about the number of people and
           records for a particular dataset
           
    Parameters
    ----------
    dataset (string): name of the dataset that will be queried (originally from the
                      parameters file)
    
    person_var (string): variable that dictates how the 'number of people' will be
                         displayed in the resultant dataframe
                         
    record_var (string): variable that dictates how the 'number of records' will be
                         displayed in the resultant dataframe
                         
    table_name (string): represents the table that is being queried
                         
    field_name (string): represents the field that should count the number of records
                         for a particular dataset/table combination. this is usually
                         'table name'_id
    

    Returns
    -------
    dataframe (df): dataframe with the information specified at the beginning of the
                    docstring
    """
    query = """
        SELECT
        DISTINCT
        COUNT(DISTINCT p.person_id) as {person_var}, COUNT(DISTINCT x.{field_name}) as {record_var}
        FROM
        `{dataset}.{table_name}` x
        JOIN
        `{dataset}.person` p
        ON
        x.person_id = p.person_id
        ORDER BY {person_var} DESC
    """.format(person_var = person_var, field_name = field_name,
               table_name = table_name, record_var = record_var, dataset = dataset)
    
    dataframe = bq.query(query)
    
    return(dataframe)


def extract_first_int_from_series(series):
    """
    Function is used to extract the first integer from a Pandas series object.
    
    Parameters
    ----------
    series (series): Pandas series object
    
    Returns
    -------
    integer (int): the first integer from a Pandas series object
    """
    
    series_as_list = series.tolist()
    
    first_int = series_as_list[0]
    
    return first_int


def render_mpl_table(data, col_width=15, row_height=0.625, font_size=12,
                     header_color='#40466e', row_colors=['#f1f1f2', 'w'], edge_color='w',
                     bbox=[0, 0, 1, 1], header_columns=0,
                     ax=None, **kwargs):
    """
    Function is used to improve the formatting / image quality of the output. The
    parameters can be changed as needed/desired.
    """
    
    # the np.array added to size is the main determinant for column dimensions
    if ax is None:
        size = (np.array(data.shape[::-1]) + np.array([2, 1])) * np.array([col_width, row_height])
        fig, ax = plt.subplots(figsize=size)
        ax.axis('off')

    mpl_table = ax.table(cellText=data.values, bbox=bbox, colLabels=data.columns, **kwargs)

    mpl_table.auto_set_font_size(False)
    mpl_table.set_fontsize(font_size)

    for k, cell in  six.iteritems(mpl_table._cells):
        cell.set_edgecolor(edge_color)
        if k[0] == 0 or k[1] < header_columns:
            cell.set_text_props(weight='bold', color='w')
            cell.set_facecolor(header_color)
        else:
            cell.set_facecolor(row_colors[k[0]%len(row_colors) ])
    return ax


# ## Looking at how volume varies in the condition_occurrence table

condition_table_name = 'condition_occurrence'
condition_field_name = 'condition_occurrence_id'

condition_unioned = generate_query(
    dataset = unioned, person_var = 'unioned_condition_num_persons', record_var = 'unioned_condition_num_records', 
    table_name = condition_table_name, field_name = condition_field_name)

condition_unioned

condition_combined = generate_query(
    dataset = combined, person_var = 'combined_condition_num_persons', record_var = 'combined_condition_num_records', 
    table_name = condition_table_name, field_name = condition_field_name)

condition_combined

condition_deid = generate_query(
    dataset = deid, person_var = 'deid_condition_num_persons', record_var = 'deid_condition_num_records', 
    table_name = condition_table_name, field_name = condition_field_name)

condition_deid

# +
unioned_num_persons = extract_first_int_from_series(condition_unioned['unioned_condition_num_persons'])
combined_num_persons = extract_first_int_from_series(condition_combined['combined_condition_num_persons'])
deid_num_persons = extract_first_int_from_series(condition_deid['deid_condition_num_persons'])

unioned_num_records = extract_first_int_from_series(condition_unioned['unioned_condition_num_records'])
combined_num_records = extract_first_int_from_series(condition_combined['combined_condition_num_records'])
deid_num_records = extract_first_int_from_series(condition_deid['deid_condition_num_records'])

unioned_combined_person_drop = unioned_num_persons - combined_num_persons
combined_deid_person_drop = combined_num_persons - deid_num_persons
total_person_drop = unioned_combined_person_drop + combined_deid_person_drop

unioned_combined_record_drop = unioned_num_records - combined_num_records
combined_deid_record_drop = combined_num_records - deid_num_records
total_record_drop = unioned_combined_record_drop + combined_deid_record_drop

data = [[unioned_combined_person_drop, combined_deid_person_drop, total_person_drop],
        [unioned_combined_record_drop, combined_deid_record_drop, total_record_drop]]

condition_df = pd.DataFrame(data, columns = ['Unioned/Combined Drop', 'Combined/De-ID Drop', 'Total Drop'],
                           index = ['Condition - Person', 'Condition - Record'])

condition_df

# +
condition_df.reset_index(level=0, inplace=True)

ax = render_mpl_table(condition_df, header_columns=0, col_width=2.0)

plt.tight_layout()

plt.savefig('condition_dropoff.jpg', bbox_inches ="tight")
# -

# ## Looking at how volume varies in the visit_occurrence_table table

visit_table_name = 'visit_occurrence'
visit_field_name = 'visit_occurrence_id'

visit_unioned = generate_query(
    dataset = unioned, person_var = 'unioned_visit_num_persons', record_var = 'unioned_visit_num_records', 
    table_name = visit_table_name, field_name = visit_field_name)

visit_unioned

visit_combined = generate_query(
    dataset = combined, person_var = 'combined_visit_num_persons', record_var = 'combined_visit_num_records', 
    table_name = visit_table_name, field_name = visit_field_name)

visit_combined

visit_deid = generate_query(
    dataset = deid, person_var = 'deid_visit_num_persons', record_var = 'deid_visit_num_records', 
    table_name = visit_table_name, field_name = visit_field_name)

visit_deid

# +
unioned_num_persons = extract_first_int_from_series(visit_unioned['unioned_visit_num_persons'])
combined_num_persons = extract_first_int_from_series(visit_combined['combined_visit_num_persons'])
deid_num_persons = extract_first_int_from_series(visit_deid['deid_visit_num_persons'])

unioned_num_records = extract_first_int_from_series(visit_unioned['unioned_visit_num_records'])
combined_num_records = extract_first_int_from_series(visit_combined['combined_visit_num_records'])
deid_num_records = extract_first_int_from_series(visit_deid['deid_visit_num_records'])

unioned_combined_person_drop = unioned_num_persons - combined_num_persons
combined_deid_person_drop = combined_num_persons - deid_num_persons
total_person_drop = unioned_combined_person_drop + combined_deid_person_drop

unioned_combined_record_drop = unioned_num_records - combined_num_records
combined_deid_record_drop = combined_num_records - deid_num_records
total_record_drop = unioned_combined_record_drop + combined_deid_record_drop

data = [[unioned_combined_person_drop, combined_deid_person_drop, total_person_drop],
        [unioned_combined_record_drop, combined_deid_record_drop, total_record_drop]]

visit_df = pd.DataFrame(data, columns = ['Unioned/Combined Drop', 'Combined/De-ID Drop', 'Total Drop'],
                           index = ['Visit - Person', 'Visit - Record'])

visit_df
# -
# ## Looking at how volume varies in the procedure_occurrence table


# +
visit_df.reset_index(level=0, inplace=True)

ax = render_mpl_table(visit_df, header_columns=0, col_width=2.0)

plt.tight_layout()

plt.savefig('visit_dropoff.jpg', bbox_inches ="tight")
# -

procedure_table_name = 'procedure_occurrence'
procedure_field_name = 'procedure_occurrence_id'

procedure_unioned = generate_query(
    dataset = unioned, person_var = 'unioned_procedure_num_persons', record_var = 'unioned_procedure_num_records', 
    table_name = procedure_table_name, field_name = procedure_field_name)

procedure_unioned

procedure_combined = generate_query(
    dataset = combined, person_var = 'combined_procedure_num_persons', record_var = 'combined_procedure_num_records', 
    table_name = procedure_table_name, field_name = procedure_field_name)

procedure_combined

procedure_deid = generate_query(
    dataset = deid, person_var = 'deid_procedure_num_persons', record_var = 'deid_procedure_num_records', 
    table_name = procedure_table_name, field_name = procedure_field_name)

procedure_deid

# +
unioned_num_persons = extract_first_int_from_series(procedure_unioned['unioned_procedure_num_persons'])
combined_num_persons = extract_first_int_from_series(procedure_combined['combined_procedure_num_persons'])
deid_num_persons = extract_first_int_from_series(procedure_deid['deid_procedure_num_persons'])

unioned_num_records = extract_first_int_from_series(procedure_unioned['unioned_procedure_num_records'])
combined_num_records = extract_first_int_from_series(procedure_combined['combined_procedure_num_records'])
deid_num_records = extract_first_int_from_series(procedure_deid['deid_procedure_num_records'])

unioned_combined_person_drop = unioned_num_persons - combined_num_persons
combined_deid_person_drop = combined_num_persons - deid_num_persons
total_person_drop = unioned_combined_person_drop + combined_deid_person_drop

unioned_combined_record_drop = unioned_num_records - combined_num_records
combined_deid_record_drop = combined_num_records - deid_num_records
total_record_drop = unioned_combined_record_drop + combined_deid_record_drop

data = [[unioned_combined_person_drop, combined_deid_person_drop, total_person_drop],
        [unioned_combined_record_drop, combined_deid_record_drop, total_record_drop]]

procedure_df = pd.DataFrame(data, columns = ['Unioned/Combined Drop', 'Combined/De-ID Drop', 'Total Drop'],
                           index = ['Procedure - Person', 'Procedure - Record'])

procedure_df

# +
procedure_df.reset_index(level=0, inplace=True)

ax = render_mpl_table(procedure_df, header_columns=0, col_width=2.0)

plt.tight_layout()

plt.savefig('procedure_dropoff.jpg', bbox_inches ="tight")
# -

# ## Looking at how volume varies in the observation table

observation_table_name = 'observation'
observation_field_name = 'observation_id'

observation_unioned = generate_query(
    dataset = unioned, person_var = 'unioned_observation_num_persons', record_var = 'unioned_observation_num_records', 
    table_name = observation_table_name, field_name = observation_field_name)

observation_unioned

observation_combined = generate_query(
    dataset = combined, person_var = 'combined_observation_num_persons', record_var = 'combined_observation_num_records', 
    table_name = observation_table_name, field_name = observation_field_name)

observation_combined

observation_deid = generate_query(
    dataset = deid, person_var = 'deid_observation_num_persons', record_var = 'deid_observation_num_records', 
    table_name = observation_table_name, field_name = observation_field_name)

observation_deid

# +
unioned_num_persons = extract_first_int_from_series(observation_unioned['unioned_observation_num_persons'])
combined_num_persons = extract_first_int_from_series(observation_combined['combined_observation_num_persons'])
deid_num_persons = extract_first_int_from_series(observation_deid['deid_observation_num_persons'])

unioned_num_records = extract_first_int_from_series(observation_unioned['unioned_observation_num_records'])
combined_num_records = extract_first_int_from_series(observation_combined['combined_observation_num_records'])
deid_num_records = extract_first_int_from_series(observation_deid['deid_observation_num_records'])

unioned_combined_observation_person_drop = unioned_num_persons - combined_num_persons
combined_deid_observation_person_drop = combined_num_persons - deid_num_persons
total_observation_person_drop = unioned_combined_observation_person_drop + combined_deid_observation_person_drop

unioned_combined_observation_record_drop = unioned_num_records - combined_num_records
combined_deid_observation_record_drop = combined_num_records - deid_num_records
total_record_observation_drop = unioned_combined_observation_record_drop + combined_deid_observation_record_drop

data = [[unioned_combined_observation_person_drop, combined_deid_observation_person_drop, total_observation_person_drop],
        [unioned_combined_observation_record_drop, combined_deid_observation_record_drop, total_record_observation_drop]]

observation_df = pd.DataFrame(data, columns = ['Unioned/Combined Drop', 'Combined/De-ID Drop', 'Total Drop'],
                           index = ['Observation - Person', 'Observation - Record'])

observation_df

# +
observation_df.reset_index(level=0, inplace=True)

ax = render_mpl_table(observation_df, header_columns=0, col_width=2.0)

plt.tight_layout()

plt.savefig('observation_dropoff.jpg', bbox_inches ="tight")
# -

# ## Looking at how volume varies in the drug_exposure table

drug_table_name = 'drug_exposure'
drug_field_name = 'drug_exposure_id'

drug_unioned = generate_query(
    dataset = unioned, person_var = 'unioned_drug_num_persons', record_var = 'unioned_drug_num_records', 
    table_name = drug_table_name, field_name = drug_field_name)

drug_unioned

drug_combined = generate_query(
    dataset = combined, person_var = 'combined_drug_num_persons', record_var = 'combined_drug_num_records', 
    table_name = drug_table_name, field_name = drug_field_name)

drug_combined

drug_deid = generate_query(
    dataset = deid, person_var = 'deid_drug_num_persons', record_var = 'deid_drug_num_records', 
    table_name = drug_table_name, field_name = drug_field_name)

drug_deid

# +
unioned_num_persons = extract_first_int_from_series(drug_unioned['unioned_drug_num_persons'])
combined_num_persons = extract_first_int_from_series(drug_combined['combined_drug_num_persons'])
deid_num_persons = extract_first_int_from_series(drug_deid['deid_drug_num_persons'])

unioned_num_records = extract_first_int_from_series(drug_unioned['unioned_drug_num_records'])
combined_num_records = extract_first_int_from_series(drug_combined['combined_drug_num_records'])
deid_num_records = extract_first_int_from_series(drug_deid['deid_drug_num_records'])

unioned_combined_drug_person_drop = unioned_num_persons - combined_num_persons
combined_deid_drug_person_drop = combined_num_persons - deid_num_persons
total_drug_person_drop = unioned_combined_drug_person_drop + combined_deid_drug_person_drop

unioned_combined_drug_record_drop = unioned_num_records - combined_num_records
combined_deid_drug_record_drop = combined_num_records - deid_num_records
total_record_drug_drop = unioned_combined_drug_record_drop + combined_deid_drug_record_drop

data = [[unioned_combined_drug_person_drop, combined_deid_drug_person_drop, total_drug_person_drop],
        [unioned_combined_drug_record_drop, combined_deid_drug_record_drop, total_record_drug_drop]]

drug_df = pd.DataFrame(data, columns = ['Unioned/Combined Drop', 'Combined/De-ID Drop', 'Total Drop'],
                           index = ['Drug - Person', 'Drug - Record'])

drug_df

# +
drug_df.reset_index(level=0, inplace=True)

ax = render_mpl_table(drug_df, header_columns=0, col_width=2.0)

plt.tight_layout()

plt.savefig('drug_dropoff.jpg', bbox_inches ="tight")
# -


