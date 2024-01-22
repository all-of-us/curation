# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
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

# ## This notebook is used to investigate the racial and gender demographics for the EHR data for the AoU program. This notebook will investigate these data completeness metrics based on both the aggregate and site levels.
#
# ### Potential use cases:
# - Understand if sites are uploading incomplete/incorrect datasets
#     - example (as of 02/26/2020): a site is reporting >97% 'Unknown' racial concepts. this site would be expected to be fairly homogenous
#     - this information could be provided to sites so they could better assess whether or not their EHR matches the demographics that they might encounter in HealthPro
#     - see if the data being provided by AoU recapitulates existing findings with respect to healthcare disparities (e.g. which groups are more likely to seek care)

from google.cloud import bigquery
# %reload_ext google.cloud.bigquery
client = bigquery.Client()
# %load_ext google.cloud.bigquery

import utils.bq
from notebooks import parameters
# %matplotlib inline
import matplotlib.pyplot as plt
import numpy as np
import six
import scipy.stats
import pandas as pd
import math
from operator import add

# +
DATASET = parameters.LATEST_DATASET

print("""
DATASET TO USE: {}
""".format(DATASET))
# -

# ### Investigating the Person Table
# - NOTE: These queries DO NOT require joins (unlike queries that you may construct for tables from other domains)

# #### Determining the most common racial concepts

race_popularity_query = """
SELECT
DISTINCT
c.concept_name as race_concept_name, p.race_concept_id, COUNT(*) as cnt
FROM
`{DATASET}.unioned_ehr_person` p
LEFT JOIN
`{DATASET}.concept` c
ON
p.race_concept_id = c.concept_id
GROUP BY 1, 2
ORDER BY cnt DESC
""".format(DATASET=DATASET)

race_popularity = pd.io.gbq.read_gbq(race_popularity_query, dialect='standard')

most_popular_race_cnames = race_popularity['race_concept_name'].tolist()
most_popular_race_cids = race_popularity['race_concept_id'].tolist()

# #### Want all of the race concept IDs and their names in a dictionary (for storage/access later on)

race_id_and_name_query = """
SELECT
DISTINCT
c.concept_name, p.race_concept_id
FROM
`{DATASET}.unioned_ehr_person` p
LEFT JOIN
`{DATASET}.concept` c
ON
p.race_concept_id = c.concept_id
GROUP BY 1, 2
""".format(DATASET=DATASET)

race_df = pd.io.gbq.read_gbq(race_id_and_name_query, dialect='standard')
race_dict = race_df.set_index('race_concept_id').to_dict()

race_dict = race_dict['concept_name']  # get rid of unnecessary nesting

# #### Let's look at the race of the 'Person' table for each of the sites

racial_distribution_by_site_query = """
SELECT
DISTINCT
a.*, b.number_from_site, ROUND(a.number_of_demographic / b.number_from_site * 100, 2) as percent_of_site_persons
FROM
  (SELECT
  DISTINCT
  mp.src_hpo_id, p.race_concept_id, c.concept_name,
  COUNT(p.race_concept_id) as number_of_demographic,
  FROM
  `{DATASET}.unioned_ehr_person` p
  LEFT JOIN
  `{DATASET}._mapping_person` mp
  ON
  p.person_id = mp.src_person_id
  LEFT JOIN
  `{DATASET}.concept` c
  ON
  p.race_concept_id = c.concept_id
  GROUP BY 1, 2, 3
  ORDER BY number_of_demographic DESC) a
JOIN
  (
  SELECT
  DISTINCT
  mp.src_hpo_id, COUNT(mp.src_hpo_id) as number_from_site
  FROM
  `{DATASET}.unioned_ehr_person` p
  LEFT JOIN
  `{DATASET}._mapping_person` mp
  ON
  p.person_id = mp.src_person_id
  GROUP BY 1
  ) b
ON
a.src_hpo_id = b.src_hpo_id
ORDER BY b.number_from_site DESC, number_of_demographic DESC
""".format(DATASET=DATASET)

racial_distribution_by_site = pd.io.gbq.read_gbq(racial_distribution_by_site_query, dialect='standard')

# ### Now we want to put this information into a format that can be easily converted into a bar graph


def return_hpos_to_display(hpo_names, max_num_sites_to_display):
    """
    Function is intended to return a means for divide the number of HPOs into an
    appropriate number of lists based on the maximum number of sites a user
    wants to display.

    This is useful for creating graphs that will only display a fraction of the
    total HPOs.

    Parameters
    ----------
    hpo_names (list): list of all the health provider organizations (in string form)

    num_sites_to_display (int): user-specified number of sites to display in each graph


    Returns
    -------
    all_hpos (list): contains several lists, each of which contains a number of sites
        (or one fewer) defined by the user. the combination of all the lists should
        have all of the original HPOs given by the hpo_names parameter
    """
    length = len(hpo_names)

    num_lists = math.ceil(length / max_num_sites_to_display)

    base = math.floor(length / num_lists)

    remainder = length - (base * num_lists)

    all_hpos = []

    starting_idx = 0
    ending_idx = starting_idx + base  # add one because it is not inclusive

    for list_num in range(num_lists):

        # this is useful for when the number of sites to display
        # does not go evenly into the number of HPOs - essentially
        # add it to the 'earlier' lists
        if list_num < remainder:
            ending_idx = ending_idx + 1

        sites = hpo_names[starting_idx:ending_idx]

        # reset for subsequent lists
        starting_idx = ending_idx
        ending_idx = starting_idx + base

        all_hpos.append(sites)

    return all_hpos


def create_information_dictionary_for_sites(hpo_dfs, selected_hpo_names,
                                            most_popular_race_cids):
    """
    Function is used to create a dictionary that contains the racial makeup of a selected
    number of sites (expressed as a percentage, from a source dataframe)

    Parameters
    ----------
    hpo_dfs (dictonary): has the following structure
        key: string representing an HPO ID
        value: dataframe that contains information about the different race concepts (IDs
               and names) and their relative spread within the site

    selected_hpo_names (list): contains strings that represent the different HPOs that will
        ultimately be translated to a dictionary


    most_popular_race_cids (list): list of the most popular concept IDs (across all sites)


    Returns
    -------
    racial_percentages (dictionary): has the following structure
        key: race concept ID
        value: list, each index represents one of the sites in the 'selected_hpo_names'
               parameter. the value represents the proportion of persons from the HPO
               who have the reported race concept ID
    """

    racial_percentages = {}

    # want to get the percentages for each of the race concept IDs
    for race_concept_id in most_popular_race_cids:
        race_percentage_list = []

        # want to look at the sites in parallel - access their dataframe
        for hpo in selected_hpo_names:
            df = hpo_dfs[hpo]
            temp = df.loc[df['race_concept_id'] == race_concept_id]

            if temp.empty:
                race_percentage_list.append(0)
            else:
                val = float(temp['percent_of_site_persons'])  # convert to float
                race_percentage_list.append(val)

        racial_percentages[race_concept_id] = race_percentage_list

    return racial_percentages


# ### Below is a means to create a 'stacked' bar graph where:
# - all of the y-values have a bar of 100%
# - the bars themselves are divdided into separate colours
# - each colour's proportion of the bar shows that colour's percent of the whole site
# - each colour is meant to represent a different racial group
#
# #### This kind of graphical representation is not particularly useful from a data quality perspective (since it is inherently visual) but it has potential to be helpful in the future so it is not worth deleting


def create_graphs(hpo_names_to_display, num_races_for_legend,
                  racial_percentages, img_name):
    """
    Function is used to create and save graphs that show the racial distribution for
    a selected number of sites

    Parameters
    ----------
    hpo_names_to_display (list): list with a user-specified number of HPOs that are to
        be displayed in the graph

    num_races_for_legend (int): the number of races that are to be displayed next
        to the graph

    racial_percentages (dictionary): has the following structure
        key: race concept ID
        value: list, each index represents one of the sites in the 'selected_hpo_names'
               parameter. the value represents the proportion of persons from the HPO
               who have the reported race concept ID

    img_name (string): name for the image to be displayed
    """
    num_sites_to_display = len(hpo_names_to_display)
    bar_width = 2 / num_sites_to_display

    idxs = []
    for x in range(num_sites_to_display):
        idxs.append(x)

    prev_bottom = [0] * num_sites_to_display

    race_cids = list(racial_percentages.keys())

    for racial_id in race_cids:

        list_by_hpo = racial_percentages[racial_id]
        plt.bar(idxs, list_by_hpo, bottom=prev_bottom, width=bar_width)
        prev_bottom = list(map(add, prev_bottom, list_by_hpo))

    plt.xticks(idxs, hpo_names_to_display, rotation=90)

    # allow user to show how many to display; otherwise overwhelming
    plt.legend(labels=most_popular_race_cnames[:num_races_for_legend],
               bbox_to_anchor=(1, 1))
    plt.ylabel('Percentage of Racial Breakdown for the Site')
    plt.xlabel('Health Provider Organization (HPO)')
    plt.title('Racial Distribution By Site - Person Table from EHR')

    plt.savefig(img_name, bbox_inches="tight")

    plt.show()


# #### Inserting a function that will enable formatting for dataframes (if this is to be used in future iterations)


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


hpo_names = list(set(racial_distribution_by_site['src_hpo_id'].tolist()))

# +
hpo_dfs = {}

for hpo in hpo_names:
    temp_df = racial_distribution_by_site.loc[
        racial_distribution_by_site['src_hpo_id'] == hpo]

    hpo_dfs[hpo] = temp_df
# -

# ### Below is an example dataframe that can be used to see the demographics for any site
#
# Note that the render_mpl_table function can be used to format and save this table

hpo_dfs['seec_miami']

# +
max_num_sites_to_display = 10

larger_hpo_list = return_hpos_to_display(
    hpo_names=hpo_names, max_num_sites_to_display=max_num_sites_to_display)
# -

for img_number, hpo_list in enumerate(larger_hpo_list):
    information_dictionary = create_information_dictionary_for_sites(
        hpo_dfs=hpo_dfs,
        selected_hpo_names=hpo_list,
        most_popular_race_cids=most_popular_race_cids)

    img_name = 'racial_distribution_{img_num}'.format(img_num=img_number + 1)

    create_graphs(hpo_names_to_display=hpo_list,
                  num_races_for_legend=9,
                  racial_percentages=information_dictionary,
                  img_name=img_name)

# ## Now we are going to get a more quantitative approach to show the disparity in record count between the person table and the other 5 canonical tables. The following tables are considered 'canonical':
# - Condition occurrence
# - Observation
# - Procedure occurrence
# - Drug Exposure
# - Visit occurrence
#
# NOTE: This may wind up simply unveiling established health disparities (e.g. reifying that certain racial groups are far more likely to seek care) but it could hold the potential to unveil egregious differences in the ETL. Again, this would be far-fetched but an interesting finding nonetheless.

# ### Let's get the general information for the 'Person' table

racial_distribution_by_site.head(
    5)  # just to show what a row would look like here


def create_query_for_particular_table(dataset, percent_of_table, table_name):
    """
    Function is used to create a query to show, for a particular table, the following:
        - for each HPO ID
            - race concept ID and the corresponding name
            - number of IDs for that particular group in the specified table
            - total number of IDs for the HPO
            - percentage of the records for the site that belong to that demographic class

    This query is then run through bigquery and returns a dataframe


    Parameters
    ----------
    dataset (str): dataset to be queried (defined at the top of the workbook)

    percent_of_table (str): the string to represent the percentage of the records for the
                            site that belong to the particular demographic class

    table_name (str): name of the table to be investigated


    Returns
    -------
    dataframe (df): contains the information specified in the top of the docstring

    """

    query = """
    SELECT
    DISTINCT
    a.src_hpo_id, a.race_concept_id, a.concept_name,
    ROUND(a.number_of_demographic / b.number_from_site * 100, 2) as {percent_of_table}
    FROM
      (SELECT
      DISTINCT
      mp.src_hpo_id, p.race_concept_id, c.concept_name,
      COUNT(p.race_concept_id) as number_of_demographic,
      FROM
      `{dataset}.unioned_ehr_{table_name}` x
      LEFT JOIN
      `{dataset}.unioned_ehr_person` p
      ON
      x.person_id = p.person_id
      LEFT JOIN
      `{dataset}._mapping_person` mp
      ON
      p.person_id = mp.src_person_id
      LEFT JOIN
      `{dataset}.concept` c
      ON
      p.race_concept_id = c.concept_id
      GROUP BY 1, 2, 3
      ORDER BY number_of_demographic DESC) a
    JOIN
      (
      SELECT
      DISTINCT
      mp.src_hpo_id, COUNT(mp.src_hpo_id) as number_from_site
      FROM
      `{dataset}.unioned_ehr_{table_name}` x
      LEFT JOIN
      `{dataset}.unioned_ehr_person` p
      ON
        x.person_id = p.person_id
      LEFT JOIN
      `{dataset}._mapping_person` mp
      ON
      p.person_id = mp.src_person_id
      GROUP BY 1
      ) b
    ON
    a.src_hpo_id = b.src_hpo_id
    ORDER BY a.src_hpo_id ASC, {percent_of_table} DESC
    """.format(dataset=dataset,
               percent_of_table=percent_of_table,
               table_name=table_name)

    dataframe = pd.io.gbq.read_gbq(query, dialect='standard')

    return dataframe


# ### Showing the demographic breakdown for each of the sites (for the 5 different canonical tables)

drug_exposure_results = create_query_for_particular_table(
    dataset=DATASET,
    percent_of_table='drug_percent_of_site_persons',
    table_name='drug_exposure')

condition_occurrence_results = create_query_for_particular_table(
    dataset=DATASET,
    percent_of_table='condition_percent_of_site_persons',
    table_name='condition_occurrence')

observation_results = create_query_for_particular_table(
    dataset=DATASET,
    percent_of_table='observation_percent_of_site_persons',
    table_name='observation')

procedure_occurrence_results = create_query_for_particular_table(
    dataset=DATASET,
    percent_of_table='procedure_percent_of_site_persons',
    table_name='procedure_occurrence')

visit_occurrence_results = create_query_for_particular_table(
    dataset=DATASET,
    percent_of_table='visit_percent_of_site_persons',
    table_name='visit_occurrence')

# ### Combining the tables so all of the demographic breakdowns for the tables are adjacent to one another

combined_df = pd.merge(
    drug_exposure_results,
    condition_occurrence_results,
    how='left',
    left_on=['src_hpo_id', 'race_concept_id', 'concept_name'],
    right_on=['src_hpo_id', 'race_concept_id', 'concept_name'])

combined_df = pd.merge(
    combined_df,
    observation_results,
    how='left',
    left_on=['src_hpo_id', 'race_concept_id', 'concept_name'],
    right_on=['src_hpo_id', 'race_concept_id', 'concept_name'])

combined_df = pd.merge(
    combined_df,
    procedure_occurrence_results,
    how='left',
    left_on=['src_hpo_id', 'race_concept_id', 'concept_name'],
    right_on=['src_hpo_id', 'race_concept_id', 'concept_name'])

combined_df = pd.merge(
    combined_df,
    visit_occurrence_results,
    how='left',
    left_on=['src_hpo_id', 'race_concept_id', 'concept_name'],
    right_on=['src_hpo_id', 'race_concept_id', 'concept_name'])

# ### Determining the 'disparity matrix' for each of the different races


def find_all_distributions_for_site_race_combo(df, hpo, race,
                                               person_distribution):
    """
    This function is used to calculate the relative 'underrepresentation' of a given
    race for a particular table when compared to the race's overall representation in
    the person table.

    For instance, a site may have 65% participants who identify as 'White'. The persons
    who identify with this race, however, only make up 60% of the drug_exposure_ids in
    the drug exposure table. This would result in a 'underrepresentation' of 5% for
    persons at this particular site for this particular table.


    Parameters
    ----------
    df (df): dataframe that contains the following information in its fields:
        a. src_hpo_id
        b. race_concept_id
        c. concept_name (for the aforementioned race_concept_id)
        d. drug_percent_of_site persons: percent of the primary keys in the particular
                                         table that are attributed to patients with the
                                         aforementioned race_concept_id
        e. the same metric as d but also for the condition, observation, procedure,
           and visit tables

    hpo (string): HPO whose 'representation' metric is going to be assessed

    race (string): race concept name that will be evaluated for 'representation'

    person_distribution: the proportion of person_ids for the particular site that
                         belong to the aforementioned race


    Returns
    -------
    difference_df: contains the 'difference' between the proportion of records
        in each of the site tables who belong to a race with respect to the proportion
        of persons who belong to a race for that site
    """

    applicable_row = df.loc[(df['concept_name'] == race) &
                            (df['src_hpo_id'] == hpo)]

    try:
        drug_distrib = applicable_row['drug_percent_of_site_persons'].tolist(
        )[0]
    except IndexError:  # site does not have it
        drug_distrib = np.nan

    try:
        observation_distrib = applicable_row[
            'observation_percent_of_site_persons'].tolist()[0]
    except IndexError:  # site does not have it
        observation_distrib = np.nan

    try:
        visit_distrib = applicable_row['visit_percent_of_site_persons'].tolist(
        )[0]
    except IndexError:  # site does not have it
        visit_distrib = np.nan

    try:
        procedure_distrib = applicable_row[
            'procedure_percent_of_site_persons'].tolist()[0]
    except IndexError:  # site does not have it
        procedure_distrib = np.nan

    try:
        condition_distrib = applicable_row[
            'condition_percent_of_site_persons'].tolist()[0]
    except IndexError:  # site does not have it
        condition_distrib = np.nan

    final_list = [
        drug_distrib, observation_distrib, visit_distrib, procedure_distrib,
        condition_distrib
    ]

    for idx, table_value in enumerate(final_list):
        person_underrepresentation = table_value - person_distribution
        final_list[idx] = round(person_underrepresentation, 2)

    labels = [
        'drug_person_diff', 'observation_person_diff', 'visit_person_diff',
        'procedure_person_diff', 'condition_person_diff'
    ]

    difference_df = pd.DataFrame(data=[final_list], columns=labels, index=[hpo])

    return difference_df


# ### Here is where we actually create the dataframes that show the relative 'disparity' for all of the dataframes

# #### In an attempt to make the amount of information outputted to dataframes more manageable, we have the variable below to determine whether or not a site 'deviates' enough to warrant getting added

threshold_for_significant_disparity = 8  # a percentage

# +
race_dfs = {}

for race in most_popular_race_cnames:

    base_df = pd.DataFrame(data=[],
                           columns=[
                               'drug_person_diff', 'observation_person_diff',
                               'visit_person_diff', 'procedure_person_diff',
                               'condition_person_diff'
                           ])

    for hpo in hpo_names:

        # should be a person
        person_distribution = racial_distribution_by_site.loc[
            (racial_distribution_by_site['concept_name'] == race) &
            (racial_distribution_by_site['src_hpo_id'] == hpo)]

        try:
            person_distribution = person_distribution[
                'percent_of_site_persons'].tolist()[0]
        except IndexError:  # site does not have it
            person_distribution = np.nan

        difference_df = find_all_distributions_for_site_race_combo(
            df=combined_df,
            hpo=hpo,
            race=race,
            person_distribution=person_distribution)

        max_disparity = abs(max(difference_df.iloc[0].tolist()))

        if max_disparity > threshold_for_significant_disparity:
            base_df = base_df.append(difference_df)
        else:
            pass

    race_dfs[race] = base_df

# -

for race_key, df in race_dfs.items():
    if not df.empty:
        df.insert(loc=0, column='hpo_id',
                  value=df.index)  # needed for the formatting of the output

        ax = render_mpl_table(df, header_columns=0, col_width=2.0)

        plt.tight_layout()

        try:
            race_key = race_key.replace(" ", "_").lower()
        except AttributeError:  # race key does not exist
            pass

        try:
            save_string = race_key + '_relative_representation_in_sites'
        except TypeError:  # race key does not exist
            save_string = 'key_not_available'

        plt.savefig(save_string, bbox_inches="tight")


