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

# # This notebook is used to model how the volume of data changes at different points in the pipeline
#
# ### The different datasets are as follows:
# - unioned_ehr (submitted directly from sites - no cleaning rules applied)
# - combined datasets
# - deidentified datasets
# #
# ### The dimensions of 'data volume' are as follows (for each table):
# - number of participants
# - number of records

from google.cloud import bigquery
# %reload_ext google.cloud.bigquery
client = bigquery.Client()
# %load_ext google.cloud.bigquery

# %matplotlib inline
import utils.bq
from notebooks import parameters
import pandas as pd
import numpy as np
import six
import matplotlib.pyplot as plt
import operator

# +
unioned = parameters.UNIONED_Q4_2019
combined = parameters.COMBINED_Q4_2019
deid = parameters.DEID_Q4_2019

print(f"""
Unioned Dataset: {unioned}
Combined Dataset: {combined}
De-ID Dataset: {deid}
""")

# -

# ## Below are the functions that can be used to create graphs for visualization purposes


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
    #plt.show()
    plt.savefig(img_name, bbox_inches="tight")


def render_mpl_table(data,
                     col_width=15,
                     row_height=0.625,
                     font_size=12,
                     header_color='#40466e',
                     row_colors=['#f1f1f2', 'w'],
                     edge_color='w',
                     bbox=[0, 0, 1, 1],
                     header_columns=0,
                     ax=None,
                     **kwargs):
    """
    Function is used to improve the formatting / image quality of the output. The
    parameters can be changed as needed/desired.
    """

    # the np.array added to size is the main determinant for column dimensions
    if ax is None:
        size = (np.array(data.shape[::-1]) + np.array([2, 1])) * np.array(
            [col_width, row_height])
        fig, ax = plt.subplots(figsize=size)
        ax.axis('off')

    mpl_table = ax.table(cellText=data.values,
                         bbox=bbox,
                         colLabels=data.columns,
                         **kwargs)

    mpl_table.auto_set_font_size(False)
    mpl_table.set_fontsize(font_size)

    for k, cell in six.iteritems(mpl_table._cells):
        cell.set_edgecolor(edge_color)
        if k[0] == 0 or k[1] < header_columns:
            cell.set_text_props(weight='bold', color='w')
            cell.set_facecolor(header_color)
        else:
            cell.set_facecolor(row_colors[k[0] % len(row_colors)])
    return ax


def create_pie_chart(dataframe, title, img_name):
    """
    Function is used to create a pie chart that can show how much each site contributes
    to the overall 'drop' between the unioned and combined datasets

    Function also saves the outputted pie chart to the current directory

    Parameters
    ----------
    dataframe (df): dataframe for a particular table. shows the following for
                    HPOs that uploaded data:

        a. the number of rows in the unioned dataset
        b. the number of rows in the combined dataset
        c. the total 'drop' of rows across unioned to combined, expressed as a percentage
        d. the relative 'contribution' of each site to the overall drop from unioned to
           combined


    title (str): title of the graph

    img_name (str): title of the image to be saved
    """
    hpo_list = dataframe['source_hpo'].tolist()[1:]  # do not take 'total'
    percent_of_drop = dataframe['percent_of_drop'].tolist()[1:]

    labels = []

    # creating the labels for the graph
    for hpo, perc in zip(hpo_list, percent_of_drop):
        string = '{}, {}%'.format(hpo, perc)
        labels.append(string)

    wedges = [0.1] * len(labels)

    plt.pie(percent_of_drop,
            labels=None,
            shadow=True,
            startangle=140,
            explode=wedges)

    plt.axis('equal')
    plt.title(title)
    plt.legend(bbox_to_anchor=(0.5, 0.75, 1.0, 0.85), labels=labels)

    plt.savefig(img_name, bbox_inches="tight")

    plt.show()


# ## Below are functions that can be used to create 'aggregate' dataframes for different tables


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
    """.format(person_var=person_var,
               field_name=field_name,
               table_name=table_name,
               record_var=record_var,
               dataset=dataset)

    dataframe = pd.io.gbq.read_gbq(query, dialect='standard')

    return (dataframe)


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


def create_aggregate_table_df(unioned, combined, deid, unioned_persons_string,
                              combined_persons_string, deid_persons_string,
                              unioned_records_string, combined_records_string,
                              deid_records_string, person_string,
                              record_string):
    """
    Function is used to create a dataframe that can display the 'drop off' of records across multiple
    stages of the pipeline.


    Parameters:
    -----------

    unioned (dataframe): contains information regarding the number of persons and record in the unioned
        dataset

    combined (dataframe): contains information regarding the number of persons and record in the combined
        dataset

    deid (dataframe): contains information regarding the number of persons and record in the deid
        dataset

    unioned_person_string (str): column name to determine the number of persons in the unioned dataset

    combined_person_string (str): column name to determine the number of persons in the combined dataset

    deid_person_string (str): column name to determine the number of persons in the deid dataset

    unioned_records_string (str): column name to determine the number of records in the unioned dataset

    combined_records_string (str): column name to determine the number of records in the combined dataset

    deid_records_string (str): column name to determine the number of records in the deid dataset

    person_string (str): row title to indicate the person drop for each stage of the pipeline

    record_string (str): row title to indicate the record drop for each stage of the pipeline


    Returns:
    --------
    df (dataframe): contains information about the record and person count drop across each stage of
                    the pipeline
    """

    unioned_num_persons = extract_first_int_from_series(
        unioned[unioned_persons_string])
    combined_num_persons = extract_first_int_from_series(
        combined[combined_persons_string])
    deid_num_persons = extract_first_int_from_series(deid[deid_persons_string])

    unioned_num_records = extract_first_int_from_series(
        unioned[unioned_records_string])
    combined_num_records = extract_first_int_from_series(
        combined[combined_records_string])
    deid_num_records = extract_first_int_from_series(deid[deid_records_string])

    unioned_combined_person_drop = unioned_num_persons - combined_num_persons
    combined_deid_person_drop = combined_num_persons - deid_num_persons
    total_person_drop = unioned_combined_person_drop + combined_deid_person_drop
    total_person_drop_percent = round(
        total_person_drop / unioned_num_persons * 100, 2)

    unioned_combined_record_drop = unioned_num_records - combined_num_records
    combined_deid_record_drop = combined_num_records - deid_num_records
    total_record_drop = unioned_combined_record_drop + combined_deid_record_drop
    total_record_drop_percent = round(
        total_record_drop / unioned_num_records * 100, 2)

    data = [[
        unioned_combined_person_drop, combined_deid_person_drop,
        total_person_drop, total_person_drop_percent
    ],
            [
                unioned_combined_record_drop, combined_deid_record_drop,
                total_record_drop, total_record_drop_percent
            ]]

    df = pd.DataFrame(data,
                      columns=[
                          'Unioned/Combined Drop', 'Combined/De-ID Drop',
                          'Total Drop', 'Total Drop (%)'
                      ],
                      index=[person_string, record_string])

    return df


# ## Let's keep a dictionary so we can log the percent drop across all of the tables

# +
table_order = ['Condition', 'Visit', 'Procedure', 'Observation', 'Drug']

percent_drops = {'person_drop': [], 'record_drop': []}
# -

# ## Looking at how volume varies in the condition_occurrence table

condition_table_name = 'condition_occurrence'
condition_field_name = 'condition_occurrence_id'

condition_unioned = generate_query(dataset=unioned,
                                   person_var='unioned_condition_num_persons',
                                   record_var='unioned_condition_num_records',
                                   table_name=condition_table_name,
                                   field_name=condition_field_name)

condition_unioned

condition_combined = generate_query(dataset=combined,
                                    person_var='combined_condition_num_persons',
                                    record_var='combined_condition_num_records',
                                    table_name=condition_table_name,
                                    field_name=condition_field_name)

condition_combined

condition_deid = generate_query(dataset=deid,
                                person_var='deid_condition_num_persons',
                                record_var='deid_condition_num_records',
                                table_name=condition_table_name,
                                field_name=condition_field_name)

condition_deid

condition_df = create_aggregate_table_df(
    unioned=condition_unioned,
    combined=condition_combined,
    deid=condition_deid,
    unioned_persons_string='unioned_condition_num_persons',
    combined_persons_string='combined_condition_num_persons',
    deid_persons_string='deid_condition_num_persons',
    unioned_records_string='unioned_condition_num_records',
    combined_records_string='combined_condition_num_records',
    deid_records_string='deid_condition_num_records',
    person_string='Condition - Person',
    record_string='Condition - Record')

condition_df

# +
condition_df.reset_index(level=0, inplace=True)

ax = render_mpl_table(condition_df, header_columns=0, col_width=2.0)

plt.tight_layout()

plt.savefig('condition_dropoff.png', bbox_inches="tight")

# +
person_idx, record_idx = 0, 1

cp_drop = condition_df.at[person_idx, 'Total Drop (%)']
cr_drop = condition_df.at[record_idx, 'Total Drop (%)']

percent_drops['person_drop'].append(cp_drop)
percent_drops['record_drop'].append(cr_drop)
# -

# ## Looking at how volume varies in the visit_occurrence_table table

visit_table_name = 'visit_occurrence'
visit_field_name = 'visit_occurrence_id'

visit_unioned = generate_query(dataset=unioned,
                               person_var='unioned_visit_num_persons',
                               record_var='unioned_visit_num_records',
                               table_name=visit_table_name,
                               field_name=visit_field_name)

visit_unioned

visit_combined = generate_query(dataset=combined,
                                person_var='combined_visit_num_persons',
                                record_var='combined_visit_num_records',
                                table_name=visit_table_name,
                                field_name=visit_field_name)

visit_combined

visit_deid = generate_query(dataset=deid,
                            person_var='deid_visit_num_persons',
                            record_var='deid_visit_num_records',
                            table_name=visit_table_name,
                            field_name=visit_field_name)

visit_deid

visit_df = create_aggregate_table_df(
    unioned=visit_unioned,
    combined=visit_combined,
    deid=visit_deid,
    unioned_persons_string='unioned_visit_num_persons',
    combined_persons_string='combined_visit_num_persons',
    deid_persons_string='deid_visit_num_persons',
    unioned_records_string='unioned_visit_num_records',
    combined_records_string='combined_visit_num_records',
    deid_records_string='deid_visit_num_records',
    person_string='Visit - Person',
    record_string='Visit - Record')

visit_df
# +
visit_df.reset_index(level=0, inplace=True)

ax = render_mpl_table(visit_df, header_columns=0, col_width=2.0)

plt.tight_layout()

plt.savefig('visit_dropoff.jpg', bbox_inches="tight")

# +
person_idx, record_idx = 0, 1

vp_drop = visit_df.at[person_idx, 'Total Drop (%)']
vr_drop = visit_df.at[record_idx, 'Total Drop (%)']

percent_drops['person_drop'].append(vp_drop)
percent_drops['record_drop'].append(vr_drop)
# -

# ## Looking at how volume varies in the procedure_occurrence table

procedure_table_name = 'procedure_occurrence'
procedure_field_name = 'procedure_occurrence_id'

procedure_unioned = generate_query(dataset=unioned,
                                   person_var='unioned_procedure_num_persons',
                                   record_var='unioned_procedure_num_records',
                                   table_name=procedure_table_name,
                                   field_name=procedure_field_name)

procedure_unioned

procedure_combined = generate_query(dataset=combined,
                                    person_var='combined_procedure_num_persons',
                                    record_var='combined_procedure_num_records',
                                    table_name=procedure_table_name,
                                    field_name=procedure_field_name)

procedure_combined

procedure_deid = generate_query(dataset=deid,
                                person_var='deid_procedure_num_persons',
                                record_var='deid_procedure_num_records',
                                table_name=procedure_table_name,
                                field_name=procedure_field_name)

procedure_deid

procedure_df = create_aggregate_table_df(
    unioned=procedure_unioned,
    combined=procedure_combined,
    deid=procedure_deid,
    unioned_persons_string='unioned_procedure_num_persons',
    combined_persons_string='combined_procedure_num_persons',
    deid_persons_string='deid_procedure_num_persons',
    unioned_records_string='unioned_procedure_num_records',
    combined_records_string='combined_procedure_num_records',
    deid_records_string='deid_procedure_num_records',
    person_string='Procedure - Person',
    record_string='Procedure - Record')

procedure_df

# +
procedure_df.reset_index(level=0, inplace=True)

ax = render_mpl_table(procedure_df, header_columns=0, col_width=2.0)

plt.tight_layout()

plt.savefig('procedure_dropoff.jpg', bbox_inches="tight")

# +
person_idx, record_idx = 0, 1

pp_drop = procedure_df.at[person_idx, 'Total Drop (%)']
pr_drop = procedure_df.at[record_idx, 'Total Drop (%)']

percent_drops['person_drop'].append(pp_drop)
percent_drops['record_drop'].append(pr_drop)
# -

# ## Looking at how volume varies in the observation table
#
# ## NOTE: It may make sense that the combined dataset has more records than the unioned EHR dataset for the observation table. This increase is because we expect an influx of patient-provided informations from the participants' surveys.

observation_table_name = 'observation'
observation_field_name = 'observation_id'

observation_unioned = generate_query(
    dataset=unioned,
    person_var='unioned_observation_num_persons',
    record_var='unioned_observation_num_records',
    table_name=observation_table_name,
    field_name=observation_field_name)

observation_unioned

observation_combined = generate_query(
    dataset=combined,
    person_var='combined_observation_num_persons',
    record_var='combined_observation_num_records',
    table_name=observation_table_name,
    field_name=observation_field_name)

observation_combined

observation_deid = generate_query(dataset=deid,
                                  person_var='deid_observation_num_persons',
                                  record_var='deid_observation_num_records',
                                  table_name=observation_table_name,
                                  field_name=observation_field_name)

observation_deid

observation_df = create_aggregate_table_df(
    unioned=observation_unioned,
    combined=observation_combined,
    deid=observation_deid,
    unioned_persons_string='unioned_observation_num_persons',
    combined_persons_string='combined_observation_num_persons',
    deid_persons_string='deid_observation_num_persons',
    unioned_records_string='unioned_observation_num_records',
    combined_records_string='combined_observation_num_records',
    deid_records_string='deid_observation_num_records',
    person_string='Observation - Person',
    record_string='Observation - Record')

observation_df

# +
observation_df.reset_index(level=0, inplace=True)

ax = render_mpl_table(observation_df, header_columns=0, col_width=2.0)

plt.tight_layout()

plt.savefig('observation_dropoff.jpg', bbox_inches="tight")

# +
person_idx, record_idx = 0, 1

op_drop = observation_df.at[person_idx, 'Total Drop (%)']
or_drop = observation_df.at[record_idx, 'Total Drop (%)']

percent_drops['person_drop'].append(op_drop)
percent_drops['record_drop'].append(or_drop)
# -

# ## Looking at how volume varies in the drug_exposure table

drug_table_name = 'drug_exposure'
drug_field_name = 'drug_exposure_id'

drug_unioned = generate_query(dataset=unioned,
                              person_var='unioned_drug_num_persons',
                              record_var='unioned_drug_num_records',
                              table_name=drug_table_name,
                              field_name=drug_field_name)

drug_unioned

drug_combined = generate_query(dataset=combined,
                               person_var='combined_drug_num_persons',
                               record_var='combined_drug_num_records',
                               table_name=drug_table_name,
                               field_name=drug_field_name)

drug_combined

drug_deid = generate_query(dataset=deid,
                           person_var='deid_drug_num_persons',
                           record_var='deid_drug_num_records',
                           table_name=drug_table_name,
                           field_name=drug_field_name)

drug_deid

drug_df = create_aggregate_table_df(
    unioned=drug_unioned,
    combined=drug_combined,
    deid=drug_deid,
    unioned_persons_string='unioned_drug_num_persons',
    combined_persons_string='combined_drug_num_persons',
    deid_persons_string='deid_drug_num_persons',
    unioned_records_string='unioned_drug_num_records',
    combined_records_string='combined_drug_num_records',
    deid_records_string='deid_drug_num_records',
    person_string='Drug - Person',
    record_string='Drug - Record')

drug_df

# +
drug_df.reset_index(level=0, inplace=True)

ax = render_mpl_table(drug_df, header_columns=0, col_width=2.0)

plt.tight_layout()

plt.savefig('drug_dropoff.jpg', bbox_inches="tight")
# +
person_idx, record_idx = 0, 1

dp_drop = drug_df.at[person_idx, 'Total Drop (%)']
dr_drop = drug_df.at[record_idx, 'Total Drop (%)']

percent_drops['person_drop'].append(dp_drop)
percent_drops['record_drop'].append(dr_drop)
# -

# ## Not let's put all of the percent drops into a single dataframe

# +
person_drops = percent_drops['person_drop']
record_drops = percent_drops['record_drop']
final_dict = {}

for table_name, person_drop, record_drop in zip(table_order, person_drops,
                                                record_drops):
    final_dict[table_name] = [person_drop, record_drop]
# -

overall_drop_df = pd.DataFrame(data=final_dict,
                               index=['Person Drop (%)', 'Record Drop (%)'])

# +
overall_drop_df.reset_index(level=0, inplace=True)

ax = render_mpl_table(overall_drop_df, header_columns=0, col_width=2.0)

plt.tight_layout()

plt.savefig('overall_percent_dropoff.jpg', bbox_inches="tight")

# -

# ## Now we are going to look at the dropoff across the different sites


def generate_site_level_query(id_name, unioned, table_name, combined):
    """
    Function is used to generate the dataframe that shows the following items:
        a. each source HPO ID
        b. the number of rows for the HPO for a particular table in the unioned dataset
        c. the number of rows for the HPO for a particular table in the combined dataset
        d. the total 'drop' of rows across unioned to combined, expressed as a percentage

    Parameters
    ----------
    id_name (string): represents the 'primary key' of the table (the unique identifier
                      for each row)

    unioned (string): the name of the unioned dataset to be queried

    table_name (string): name of the table that is being investigated

    combined (string): the name of the combined dataset to be queried


    Returns
    -------
    dataframe (df): contains all of the information outlined in the top of the docstring
    """

    site_level_query = """
    SELECT
      DISTINCT a.src_hpo_id AS source_hpo,
      a.num_rows_unioned,
      b.num_rows_combined,
      ROUND( (a.num_rows_unioned - b.num_rows_combined) / a.num_rows_unioned * 100, 2) AS percent_unioned_rows_dropped
    FROM (
      SELECT
        DISTINCT mx.src_hpo_id,
        COUNT(x1.{id_name}) AS num_rows_unioned,
      FROM
        `{unioned}.{table_name}` x1
      LEFT JOIN
        `{unioned}._mapping_{table_name}` mx
      ON
        x1.{id_name} = mx.{id_name}
      GROUP BY
        1) a
    JOIN (
      SELECT
        DISTINCT mx.src_hpo_id,
        COUNT(x2.{id_name}) AS num_rows_combined,
      FROM
        `{combined}.{table_name}` x2
      LEFT JOIN
        `{combined}._mapping_{table_name}` mx
      ON
        x2.{id_name} = mx.{id_name}
      GROUP BY
        1 ) b
    ON
      a.src_hpo_id = b.src_hpo_id
    ORDER BY
      percent_unioned_rows_dropped DESC
    """.format(id_name=id_name,
               unioned=unioned,
               table_name=table_name,
               combined=combined)

    dataframe = utils.bq.query(site_level_query)

    return dataframe


def add_total_drop_row(dataframe):
    """
    Function is used to add a 'total' row at the bottom of a dataframe that shows the
    relative 'drop' across the pipeline (unioned to combined) for the different sites.

    This row will show:
        a. the number of rows in the unioned dataset
        b. the number of rows in the combined dataset
        c. the total 'drop' of rows across unioned to combined, expressed as a percentage

    Parameters:
    ----------
    dataframe (df): dataframe for a particular table. shows a-c (above) for each of the
        HPOs that uploaded data

    Returns:
    --------
    dataframe (df): the inputted dataframe with an additional 'total' row at the end
    """

    dataframe = dataframe.append(
        dataframe.sum(numeric_only=True).rename('Total'))

    hpo_names = dataframe['source_hpo'].tolist()

    hpo_names[-1:] = ["Total"]

    dataframe['source_hpo'] = hpo_names

    unioned_total = dataframe.loc['Total']['num_rows_unioned']

    combined_total = dataframe.loc['Total']['num_rows_combined']

    total_drop_percent = round(
        (unioned_total - combined_total) / unioned_total * 100, 2)

    dataframe.at['Total', 'percent_unioned_rows_dropped'] = total_drop_percent

    return dataframe


def add_percent_of_drop_column(dataframe):
    """
    Function is used to add a 'percent_of_drop' column that shows how much
    each site's 'drop' contributed to the 'overall' drop from the unioned
    to the combined steps of the pipeline.

    Parameters
    ----------
    dataframe (df): dataframe for a particular table. shows the following for
                    HPOs that uploaded data:

        a. the number of rows in the unioned dataset
        b. the number of rows in the combined dataset
        c. the total 'drop' of rows across unioned to combined, expressed as a percentage

    Returns
    -------
    dataframe (df): the above dataframe with a new column that shows each site's
                    'contribution' to the overall drop between unioned and
                    combined
    """
    unioned_total = dataframe.loc['Total']['num_rows_unioned']
    combined_total = dataframe.loc['Total']['num_rows_combined']
    total_drop = unioned_total - combined_total

    hpo_list = dataframe['source_hpo'].tolist()

    rows_unioned = dataframe['num_rows_unioned']
    rows_combined = dataframe['num_rows_combined']

    subtracted = map(operator.sub, rows_unioned, rows_combined)
    subtracted_list = list(subtracted)

    percent_of_drop = [round(x / total_drop * 100, 2) for x in subtracted_list]

    dataframe['percent_of_drop'] = percent_of_drop

    return dataframe


# ### Lets see the dropoff in the measurement table

measurement_info = generate_site_level_query(id_name='measurement_id',
                                             unioned=unioned,
                                             table_name='measurement',
                                             combined=combined)

measurement_info = add_total_drop_row(measurement_info)

measurement_info = add_percent_of_drop_column(measurement_info)

# +
measurement_info_dict = create_dicts_w_info(
    df=measurement_info,
    x_label='source_hpo',
    column_label='percent_unioned_rows_dropped')

create_graphs(info_dict=measurement_info_dict,
              xlabel='HPO',
              ylabel='% Rows Dropped from Unioned to Combined',
              title='Measurement Drop (Unioned to Combined) By Site',
              img_name='unioned_combined_measurement_drop.jpg',
              color='b',
              total_diff_color=True,
              turnoff_x=False)
# -

measurement_info = measurement_info.sort_values(by='percent_of_drop',
                                                ascending=False)

create_pie_chart(
    measurement_info,
    title='Unioned to Combined Drop \n (Measurement) Contributions',
    img_name='measurement_unioned_combined_drop_site_contribution.jpg')

# ### Drug Exposure Table

drug_info = generate_site_level_query(id_name='drug_exposure_id',
                                      unioned=unioned,
                                      table_name='drug_exposure',
                                      combined=combined)

drug_info = add_total_drop_row(drug_info)

drug_info = add_percent_of_drop_column(drug_info)

# +
drug_info_dict = create_dicts_w_info(
    df=drug_info,
    x_label='source_hpo',
    column_label='percent_unioned_rows_dropped')

create_graphs(info_dict=drug_info_dict,
              xlabel='HPO',
              ylabel='% Rows Dropped from Unioned to Combined',
              title='Drug Drop (Unioned to Combined) By Site',
              img_name='unioned_combined_drug_drop.jpg',
              color='b',
              total_diff_color=True,
              turnoff_x=False)
# -

drug_info = drug_info.sort_values(by='percent_of_drop', ascending=False)

create_pie_chart(drug_info,
                 title='Unioned to Combined Drop \n (Drug) Contributions',
                 img_name='drug_unioned_combined_drop_site_contribution.jpg')

# ### Procedure Occurrence Table

procedure_info = generate_site_level_query(id_name='procedure_occurrence_id',
                                           unioned=unioned,
                                           table_name='procedure_occurrence',
                                           combined=combined)

procedure_info = add_total_drop_row(procedure_info)

procedure_info = add_percent_of_drop_column(procedure_info)

# +
procedure_info_dict = create_dicts_w_info(
    df=procedure_info,
    x_label='source_hpo',
    column_label='percent_unioned_rows_dropped')

create_graphs(info_dict=procedure_info_dict,
              xlabel='HPO',
              ylabel='% Rows Dropped from Unioned to Combined',
              title='Procedure Drop (Unioned to Combined) By Site',
              img_name='unioned_combined_procedure_drop.jpg',
              color='b',
              total_diff_color=True,
              turnoff_x=False)
# -

procedure_info = procedure_info.sort_values(by='percent_of_drop',
                                            ascending=False)

create_pie_chart(
    procedure_info,
    title='Unioned to Combined Drop \n (Procedure) Contributions',
    img_name='procedure_unioned_combined_drop_site_contribution.jpg')

# ### Visit Occurrence Table

visit_info = generate_site_level_query(id_name='visit_occurrence_id',
                                       unioned=unioned,
                                       table_name='visit_occurrence',
                                       combined=combined)

visit_info = add_total_drop_row(visit_info)

visit_info = add_percent_of_drop_column(visit_info)

# +
visit_info_dict = create_dicts_w_info(
    df=visit_info,
    x_label='source_hpo',
    column_label='percent_unioned_rows_dropped')

create_graphs(info_dict=visit_info_dict,
              xlabel='HPO',
              ylabel='% Rows Dropped from Unioned to Combined',
              title='Visit Drop (Unioned to Combined) By Site',
              img_name='unioned_combined_visit_drop.jpg',
              color='b',
              total_diff_color=True,
              turnoff_x=False)

# +
visit_info = visit_info.sort_values(by='percent_of_drop', ascending=False)

create_pie_chart(visit_info,
                 title='Unioned to Combined Drop \n (Visit) Contributions',
                 img_name='visit_unioned_combined_drop_site_contribution.jpg')
# -

# ### Observation Table

observation_info = generate_site_level_query(id_name='observation_id',
                                             unioned=unioned,
                                             table_name='observation',
                                             combined=combined)

# +
observation_info = add_total_drop_row(observation_info)

observation_info = add_percent_of_drop_column(observation_info)

# +
observation_info_dict = create_dicts_w_info(
    df=observation_info,
    x_label='source_hpo',
    column_label='percent_unioned_rows_dropped')

create_graphs(info_dict=observation_info_dict,
              xlabel='HPO',
              ylabel='% Rows Dropped from Unioned to Combined',
              title='Observation Drop (Unioned to Combined) By Site',
              img_name='unioned_combined_observation_drop.jpg',
              color='b',
              total_diff_color=True,
              turnoff_x=False)

# +
observation_info = observation_info.sort_values(by='percent_of_drop',
                                                ascending=False)

create_pie_chart(
    observation_info,
    title='Unioned to Combined Drop \n (Observation) Contributions',
    img_name='observation_unioned_combined_drop_site_contribution.jpg')
