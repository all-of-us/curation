# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
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

# ## Script is used to determine any potential sites that may be using uploading erroneous measurements. Sites may have 'outlier' values beacuse:
# - They may be using a unit_concept_id that does not have a correspondining 'conversion' in '[unit_mapping.csv](https://github.com/all-of-us/curation/blob/develop/data_steward/resources/unit_mapping.csv)'.

# +
from notebooks import bq, render, parameters

import matplotlib.pyplot as plt
import numpy as np
from pandas.plotting import table 
# %matplotlib inline
import six
from scipy import stats
import math
import seaborn as sns

# +
measurement_ancestors = [
# lipids
# 40772590, 40782589, 40795800, 40772572,

# #cbc
# 40789356, 40789120, 40789179, 40782521,
# 40772748, 40782735, 40789182, 40786033,
# 40779159,

# #cbc w diff
# 40785788, 40785796, 40779195, 40795733,
# 40795725, 40772531, 40779190, 
# 40785793,
# 40779191, 40782561, 40789266, 

#cmp
# 3049187, 3053283, 40775801, 40779224,
# 40779250, 40782562, 40782579, 40785850,
# 40785861, 40785869, 40789180, 40789190,
# 40789527, 40791227, 40792413, 40792440,
# 40795730, 40795740 40795754,
    
#physical measurement
45875982, 45876161, 45876166, 45876171,
45876174, 45876226]
# -

DATASET = parameters.LATEST_DATASET
print(
"""
DATASET TO USE: {}
""".format(DATASET))


def find_descendants(DATASET, ancestor_concept):
    """
    Function is used to find the descendants of a particular ancestor concept ID using
    Bigquery.

    This function then creates a long string of said 'descendant' concepts so it can
    be used in future queries.
    
    Parameters
    ----------
    DATASET (string): string representing the dataset to be queried. Taken from the
        parameters file
        
    ancestor_concept(integer): integer that is the 'ancestor_concept_id' for a particular
        set of labs
        
    Returns
    -------
    string_desc_concepts(string): string of all the descendant concept IDs that
        represent the concept_ids for the particular measurement set
    """

    descendant_concepts = """
    (SELECT
    DISTINCT
    m.measurement_concept_id
    FROM
    `{}.unioned_ehr_measurement` m
    LEFT JOIN
    `{}.concept_ancestor` ca
    ON
    m.measurement_concept_id = ca.descendant_concept_id
    WHERE
    ca.ancestor_concept_id IN ({})
    GROUP BY 1)""".format(DATASET, DATASET, ancestor_concept)

    desc_concepts_df = bq.query(descendant_concepts)
    
    descendant_concept_ids = desc_concepts_df['measurement_concept_id'].tolist()
    
    string_desc_concepts = "("
    num_descs = len(descendant_concept_ids)

    for idx, concept_id in enumerate(descendant_concept_ids):
        string_desc_concepts += str(concept_id)
        if idx < num_descs - 1:
            string_desc_concepts += ", "
        else:
            string_desc_concepts += ")"
    
    return string_desc_concepts


def find_total_number_of_units_for_lab_type(DATASET, string_desc_concepts):
    """
    Function is used to find the total number of records that have a unit_concept_id
    for the 'cluster' of measurement concept IDs that represent a particular lab
    type. The unit_concept_id must be:
        a. non-null
        b. not 0

    Parameters
    ----------
    DATASET (string): string representing the dataset to be queried. Taken from the
        parameters file

    string_desc_concepts(string): string of all the descendant concept IDs that
        represent the concept_ids for the particular measurement set
        
    Returns
    -------
    tot_units (int): represents the total number of recoreds for the particular
        measurement set that have a unit_concept ID
    """
    
    total_unit_concept_names = """
    SELECT SUM(a.count) as tot_concepts
    FROM
        (SELECT
        DISTINCT
        c.concept_name as unit_name, c.standard_concept, COUNT(*) as count
        FROM
        `{}.unioned_ehr_measurement` m
        LEFT JOIN
        `{}.concept` c
        ON
        m.unit_concept_id = c.concept_id
        WHERE
        m.measurement_concept_id IN {}
        AND
        m.unit_concept_id IS NOT NULL
        AND
        m.unit_concept_id <> 0
        GROUP BY 1, 2
        ORDER BY count DESC) a
    """.format(DATASET, DATASET, string_desc_concepts)

    tot_units_df = bq.query(total_unit_concept_names)
    tot_units = tot_units_df['tot_concepts'].iloc[0]

    return tot_units


def find_most_popular_unit_type(tot_units, DATASET, string_desc_concepts):
    """
    Function is used to find the most popular unit type for the 'cluster' 
    of measurement concept IDs that represent a particular measurement set.
    
    Parameters
    ----------
    tot_units (int): represents the total number of recoreds for the particular
        measurement set that have a unit_concept ID
        
    DATASET (string): string representing the dataset to be queried. Taken from the
        parameters file
    
    string_desc_concepts(string): string of all the descendant concept IDs that
        represent the concept_ids for the particular measurement set.
    
    Returns
    -------
    most_pop_unit (string): string that represents the most popular unit concept
        name for the particular measurement set.

    """
    units_for_lab = """
    SELECT
    DISTINCT
    c.concept_name as unit_name, c.standard_concept, COUNT(*) as count, ROUND(COUNT(*) / {} * 100, 2) as percentage_units
    FROM
    `{}.unioned_ehr_measurement` m
    LEFT JOIN
    `{}.concept` c
    ON
    m.unit_concept_id = c.concept_id
    WHERE
    m.measurement_concept_id IN {}
    AND
    m.unit_concept_id IS NOT NULL
    AND
    m.unit_concept_id <> 0
    GROUP BY 1, 2
    ORDER BY count DESC
    """.format(tot_units, DATASET, DATASET, string_desc_concepts)

    units_for_lab_df = bq.query(units_for_lab)
    
    desc_concept_ids = units_for_lab_df['unit_name'].tolist()

    most_pop_unit = desc_concept_ids[0]
    
    return most_pop_unit



def metrics_for_whole_dataset(DATASET, most_pop_unit, string_desc_concepts, ancestor_concept):
    """
    Function is used to determine select metrics for the whole dataset for all
    of the measurement concept IDs that represent a particular measurement set.
    
    Parameters
    ----------
    DATASET (string): string representing the dataset to be queried. Taken from the
        parameters file
    
    most_pop_unit (string): string that represents the most popular unit concept
        name for the particular measurement set.
        
    string_desc_concepts(string): string of all the descendant concept IDs that
        represent the concept_ids for the particular measurement set.
        
    ancestor_concept (int): used as the 'starting' point for all of the measurements.
        can hopefully capture multiple descendants that reflect the same type of
        measurement.
    
    Returns
    -------
    median (float): number that represents the 'median' of all the measurements
        for the measurement set that have the most popular unit concept
    
    stdev (float): number that represents the 'standard deviation' of all the
        measurements for the measurement set that have the most popular unit concept
        
    num_records (float): number of records (across all sites) that are being measured
        for the particular ancestor_concept_id, unit_concept_id, etc.
        
    mean (float): number that represents the 'mean' of all the measurements for the
        meawsurement set that have the most popular unit
        
    values (list): list of values for the entire dataset; used to create the
        box-and-whisker plot
    """
    
    find_range_overall = """
    SELECT
    m.value_as_number
    FROM
    `{}.unioned_ehr_measurement` m
    JOIN
    `{}.concept` c
    ON
    m.unit_concept_id = c.concept_id
    WHERE
    c.concept_name like '%{}%'
    AND
    m.measurement_concept_id IN {}
    AND
    m.value_as_number IS NOT NULL
    AND
    m.value_as_number <> 9999999  -- issue with one site that heavily skews data
    AND
    m.value_as_number <> 0.0  -- not something we expect; appears for a site
    ORDER BY
    m.value_as_number ASC
    """.format(DATASET, DATASET, most_pop_unit, string_desc_concepts)

    measurements_for_lab_and_unit = bq.query(find_range_overall)

    values = measurements_for_lab_and_unit['value_as_number'].tolist()
    
    find_ancestor_lab = """
    SELECT
    DISTINCT
    c.concept_name
    FROM
    `{}.concept` c
    WHERE
    c.concept_id = {}
    """.format(DATASET, ancestor_id)
    
    concept_name = bq.query(find_ancestor_lab)
    concept_name = concept_name['concept_name'].tolist()
    concept_name = str(concept_name[0].lower())  # actual name
    
    num_records = len(values)
    mean = np.mean(values)

    decile1 = np.percentile(values, 10)

    quartile1 = np.percentile(values, 25)
    median = np.percentile(values, 50)
    quartile3 = np.percentile(values, 75)

    decile9 = np.percentile(values, 90)

    stdev = np.std(np.asarray(values))

    print("There are {} records for concept: {}\n".format(num_records, concept_name))
    print("The 10th percentile is: {}".format(decile1))
    print("The 25th percentile is: {}".format(quartile1))
    print("The 50th percentile is: {}".format(median))
    print("The 75th percentile is: {}".format(quartile3))
    print("The 90th percentile is: {}\n".format(decile9))

    print("The mean is: {}".format(mean))
    print("The standard deviation is: {}".format(round(stdev, 2)))
    
    return median, stdev, num_records, mean, values


#
#     ({} - a.mean) as numerator,
#     ROUND((POW({}, 2) / {}), 2) as prt1, 
#     (POW(a.stdev, 2)) / a.total_rows as prt2,
#     ROUND(((POW({}, 2)) / {}) + ((POW(a.stdev, 2)) / a.total_rows), 2) as sample,
#     ROUND(SQRT( ((POW({}, 2)) / {}) + ((POW(a.stdev, 2)) / a.total_rows)), 2) as denom,
#     
#     
#     
#     ROUND(({} - a.mean) / SQRT( ((POW({}, 2)) / {}) + ((POW(a.stdev, 2)) / a.total_rows)), 2) as t_value, 
#     
#     ROUND(POW(((POW({}, 2)) / {}) + ((POW(a.stdev, 2)) / a.total_rows), 2) /
#     (POW(((POW({}, 2)) / {}), 2) / ({} - 1) + POW(((POW(a.stdev, 2)) / a.total_rows), 2) / (a.total_rows - 1)), 2)
#     as degrees_freedom, 
#

def create_site_distribution_df(
    median, tot_stdev, tot_records, total_mean, DATASET, string_desc_concepts, most_pop_unit):
    """
    Function is used to create and return a dataframe that shows the mean and standard deviation
    for a site's values (of a particular measurement set, for the most popular unit_concept)
    compared to the values for the entire dataset.
    
    Parameters
    ----------
    median (float): number that represents the 'median' of all the measurements
        for the measurement set that have the most popular unit concept
    
    tot_stdev (float): number that represents the 'standard deviation' of all the
        measurements for the measurement set that have the most popular unit concept
        
    tot_records (float): number of records (across all sites) that are being measured
        for the particular ancestor_concept_id, unit_concept_id, etc.
        
    DATASET (string): string representing the dataset to be queried. Taken from the
        parameters file
        
    string_desc_concepts(string): string of all the descendant concept IDs that
        represent the concept_ids for the particular measurement set.
    
    most_pop_unit (string): string that represents the most popular unit concept
        name for the particular measurement set.

    Returns
    -------
    site_value_distribution_df (dataframe): dataframe that allows one to compare
        each site's values (of a particular measurement set, for the most popular unit_concept)
        compared to the values for the entire dataset.
    
    """
    
    find_site_distribution = """
    SELECT
    DISTINCT
    a.src_hpo_id, a.mean, a.min, a.tenth_perc, a.first_quartile, a.median, 
    a.third_quartile, a.ninetieth_perc, a.max, a.stdev, 
    a.total_mean, a.total_median, a.total_stdev,
    a.total_rows,
    COUNT(*) as hpo_rows
    
    
    FROM
      (SELECT
      mm.src_hpo_id,
      ROUND(AVG(m.value_as_number) OVER (PARTITION BY mm.src_hpo_id), 2) as mean,
      ROUND(MIN(m.value_as_number) OVER (PARTITION BY mm.src_hpo_id), 2) as min,
      ROUND(PERCENTILE_CONT(m.value_as_number, 0.1) OVER (PARTITION BY mm.src_hpo_id), 2) as tenth_perc,
      ROUND(PERCENTILE_CONT(m.value_as_number, 0.25) OVER (PARTITION BY mm.src_hpo_id), 2) as first_quartile,
      ROUND(PERCENTILE_CONT(m.value_as_number, 0.5) OVER (PARTITION BY mm.src_hpo_id), 2) as median,
      ROUND(PERCENTILE_CONT(m.value_as_number, 0.75) OVER (PARTITION BY mm.src_hpo_id), 2) as third_quartile,
      ROUND(PERCENTILE_CONT(m.value_as_number, 0.9) OVER (PARTITION BY mm.src_hpo_id), 2) as ninetieth_perc,
      ROUND(MAX(m.value_as_number) OVER (PARTITION BY mm.src_hpo_id), 2) as max,
      ROUND(STDDEV_POP(m.value_as_number) OVER (PARTITION BY mm.src_hpo_id), 2) as stdev,
      ROUND({}, 2) as total_mean,
      ROUND({}, 2) as total_median, 
      ROUND({}, 2) as total_stdev,
      ROUND({}, 2) as total_rows
      FROM
      `{}.unioned_ehr_measurement` m
      JOIN
      `{}._mapping_measurement` mm
      ON
      m.measurement_id = mm.measurement_id 
      JOIN
      `{}.concept` c
      ON
      c.concept_id = m.unit_concept_id 
      where
      m.measurement_concept_id IN {}
      AND
      m.value_as_number IS NOT NULL
      AND
      m.value_as_number <> 9999999  -- issue with one site that heavily skews data
      AND
      m.value_as_number <> 0.0  -- not something we expect; appears for a site
      AND
      m.unit_concept_id <> 0
      AND
      c.concept_name LIKE '%{}%'
      AND
      m.value_as_number IS NOT NULL
      ORDER BY mm.src_hpo_id, median DESC) a
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14
    ORDER BY median DESC
    """.format(total_mean, median, tot_stdev, tot_records,
               DATASET, DATASET, DATASET, string_desc_concepts, most_pop_unit)

    site_value_distribution_df = bq.query(find_site_distribution)
    
    return site_value_distribution_df


def run_statistics(ancestor_concept):
    """
    Function runs the statistics and created the dataframe for all of the measurements
    for a particular ancestor_concept_id (and its descendants).
    
    Parameters
    ----------
    ancestor_concept (int): used as the 'starting' point for all of the measurements.
        can hopefully capture multiple descendants that reflect the same type of
        measurement.
        
    Returns
    -------
    site_value_distribution_df (dataframe): dataframe that allows one to compare
        each site's values (of a particular measurement set, for the most popular unit_concept)
        compared to the values for the entire dataset.
    
    entire_values (list): list of values for the entire dataset; used to create the
        box-and-whisker plot
    """
    
    descendant_concepts = find_descendants(DATASET, ancestor_concept)
    
    tot_units = find_total_number_of_units_for_lab_type(DATASET, descendant_concepts)
    
    most_popular_unit = find_most_popular_unit_type(tot_units, DATASET, descendant_concepts)
    
    median, stdev, num_records, mean, entire_values = metrics_for_whole_dataset(
        DATASET, most_popular_unit, descendant_concepts, ancestor_concept)
    
    site_value_distribution_df = create_site_distribution_df(
        median, stdev, num_records, mean, DATASET, descendant_concepts, most_popular_unit)
    
    
    return site_value_distribution_df, most_popular_unit, entire_values


# #### Below cell modified from this [StackOverflow](https://stackoverflow.com/questions/26678467/export-a-pandas-dataframe-as-a-table-image)

def render_mpl_table(data, col_width=3.0, row_height=0.625, font_size=12,
                     header_color='#40466e', row_colors=['#f1f1f2', 'w'], edge_color='w',
                     bbox=[0, 0, 1, 1], header_columns=0,
                     ax=None, **kwargs):
    """
    Function is used to improve the formatting / image quality of the output
    """
    
    if ax is None:
        size = (np.array(data.shape[::-1]) + np.array([0, 1])) * np.array([col_width, row_height])
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


def replace_name(name):
    """
    Function is used to replace the characters of the given string (either the
    concept name or the unit name) to ensure that the file can be saved properly.
    
    Parameters
    ----------
    name (str): string representing either the concept or unit name
    
    
    Return
    ------
    name(str): string representing either the concept or unit name (now without
        characters that could mess up the saving process)
    """
    
    name = name.replace(" ", "_")
    name = name.replace("/", "_")
    name = name.replace(".", "_")
    name = name.replace("-", "_")
    name = name.replace("[", "_")
    name = name.replace("]", "_")
    name = name.replace(",", "_")
    name = name.replace("(", "_")
    name = name.replace(")", "_")
    
    if name[-1:] == "_":
        name = name[:-1]  # chop off the underscore
        
    num_underscores = 0
    for char in name:
        if char == "_":
            num_underscores += 1
    
    # now we want to get rid of excessively long strings
    new_name = ''
    if num_underscores > 2:
        x = 0
        for char in name:
            if char == '_':
                x += 1
            if x < 3:
                new_name += char
        
        name = new_name
        
    print(name)
    
    return name


def process_image(DATASET, df, most_popular_unit):
    """
    Function is used to:
        a. Create the name of the resulting image
        b. Save the image
    
    Parameters
    ----------
    DATASET (string): string representing the dataset to be queried. Taken from the
        parameters file
    
    df (dataframe): dataframe that allows one to compare each site's values (of a 
        particular measurement set, for the most popular unit_concept) compared to the values 
        for the entire dataset.
    """
    find_ancestor_lab = """
    SELECT
    DISTINCT
    c.concept_name
    FROM
    `{}.concept` c
    WHERE
    c.concept_id = {}
    """.format(DATASET, ancestor_id)


    concept_name = bq.query(find_ancestor_lab)
    concept_name = concept_name['concept_name'].tolist()
    concept_name = str(concept_name[0].lower())  # actual name
    
    concept_name = replace_name(concept_name)
    unit_name = replace_name(most_popular_unit)

    img_name = concept_name + "_" + unit_name + "_site_value_distributions.png"
    
    ax = render_mpl_table(df, header_columns=0, col_width=2.0)
    plt.savefig(img_name)
    
    return img_name


def add_significance_cols(df):
    """
    Function is used to add columns to the dataframe to give information about how the HPO's
    data for the selected measurement/unit combination compares to the overall measurement/unit
    combination (across all sites).
    
    These signficance metrics are based on Welch's t-test (unpaired t-test, using different subjects,
    not assuming equal variance.)
    
    parameters:
    -----------
    site_value_distribution_df (dataframe): dataframe that allows one to compare
        each site's values (of a particular measurement set, for the most popular unit_concept)
        compared to the values for the entire dataset
        
    returns:
    --------
    site_value_distribution_df (dataframe): dataframe that allows one to compare
        each site's values (of a particular measurement set, for the most popular unit_concept)
        compared to the values for the entire dataset.
    """
    t_vals, degrees_freedom, p_vals, sig_diff = [], [], [], []

    for idx, hpo in df.iterrows():
        hpo_mean = hpo['mean']
        hpo_stdev = hpo['stdev']
        hpo_rows = hpo['hpo_rows']

        tot_mean = hpo['total_mean']
        tot_stdev = hpo['total_stdev']
        tot_rows = hpo['total_rows']

        numerator = tot_mean - hpo_mean

        denom = ((tot_stdev ** 2) / tot_rows) + ((hpo_stdev ** 2) / hpo_rows)
        denominator = denom ** (1/2)

        try:
            t_val = numerator / denominator
        except ZeroDivisionError:
            t_val = 0

        if t_val > 0:  # standardize for the sake of the p-value calculation
            t_val = t_val * -1


        print(hpo['src_hpo_id'])

        t_vals.append(round(t_val, 2))

        num = ( (tot_stdev ** 2) / tot_rows + (hpo_stdev ** 2) / hpo_rows ) ** 2
        denom_pt1 = (tot_stdev ** 2 / tot_rows) ** 2 / (tot_rows - 1)

        try:
            denom_pt2 = (hpo_stdev ** 2 / hpo_rows) ** 2  / (hpo_rows - 1)
            denom = denom_pt1 + denom_pt2
        except ZeroDivisionError:
            denom = denom_pt1

        deg_freedom = num / denom

        degrees_freedom.append(round(deg_freedom, 2))

        p = stats.t.cdf(t_val, df=deg_freedom) * 2  # for two-tailed info

        if p < 0.05:
            sig_diff.append("True")
        else:
            sig_diff.append("False")

        print(t_val)
        print(deg_freedom)
        print(p)
        print("\n")

        p_vals.append(round(p, 4))

    df['t_val'] = t_vals
    df['degrees_freedom'] = degrees_freedom
    df['p_val'] = p_vals
    df['sig_diff'] = sig_diff
    
    return df

for ancestor_id in measurement_ancestors:
    site_value_distribution_df, most_popular_unit, entire_values = run_statistics(ancestor_id)
    
    df = add_significance_cols(site_value_distribution_df)
    
    img_name = process_image(DATASET, site_value_distribution_df, most_popular_unit)
site_value_distribution_df

entire_values

import pandas as pd

# +
d = {'Entire Dataset': entire_values}
df = pd.DataFrame(d)

df

df.T.boxplot(vert=False)
plt.show()
# -


