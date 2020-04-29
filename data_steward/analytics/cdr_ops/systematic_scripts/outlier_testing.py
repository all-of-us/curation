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

# ## Script is used to determine any potential sites that may be using uploading erroneous measurements. Sites may have 'outlier' values beacuse (running list):
# - They may be using a unit_concept_id that does not have a correspondining 'conversion' in '[unit_mapping.csv](https://github.com/all-of-us/curation/blob/develop/data_steward/resources/unit_mapping.csv)'.

from google.cloud import bigquery
# %reload_ext google.cloud.bigquery
client = bigquery.Client()
# %load_ext google.cloud.bigquery

# +
import bq_utils
import utils.bq
from notebooks import parameters

# %matplotlib inline
import matplotlib.pyplot as plt
import numpy as np
import six
import scipy.stats
import pandas as pd
# -

measurement_ancestors = [
    # lipids
    40782589, 40795800, 40772572

    # #cbc
    # 40789356, 40789120, 40789179, 40772748,
    # 40782735, 40789182, 40786033, 40779159

    # #cbc w diff
    # 40785788, 40785796, 40779195, 40795733,
    # 40795725, 40772531, 40779190, 40785793,
    # 40779191, 40782561, 40789266

    #cmp
    # 3049187, 3053283, 40775801, 40779224,
    # 40782562, 40782579, 40785850, 40785861,
    # 40785869, 40789180, 40789190, 40789527,
    # 40791227, 40792413, 40792440, 40795730,
    # 40795740, 40795754

    #physical measurement
#     40654163,
#     40655804,
#     40654162,
#     40655805,
#     40654167,
#     40654164
]

DATASET = parameters.LATEST_DATASET
print("""
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
        
    ancestor_concept (integer): integer that is the 'ancestor_concept_id' for a particular
        set of labs
        
    Returns
    -------
    string_desc_concepts(string): string of all the descendant concept IDs that
        represent the concept_ids for the particular measurement set
    """

    descendant_concepts = """
    SELECT
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
    GROUP BY 1""".format(DATASET, DATASET, ancestor_concept)
    
    print(descendant_concepts)

    desc_concepts_df = pd.io.gbq.read_gbq(descendant_concepts, dialect='standard')
    
    print('success!')

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

    tot_units_df = pd.io.gbq.read_gbq(total_unit_concept_names, dialect='standard')
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

    units_for_lab_df = pd.io.gbq.read_gbq(units_for_lab, dialect='standard')

    desc_concept_ids = units_for_lab_df['unit_name'].tolist()

    most_pop_unit = desc_concept_ids[0]

    return most_pop_unit


def metrics_for_whole_dataset(DATASET, most_pop_unit, string_desc_concepts,
                              ancestor_concept):
    """
    Function is used to determine select metrics for the whole dataset for all
    of the measurement concept IDs that represent a particular measurement set.
    
    Parameters
    ----------
    DATASET (string): string representing the dataset to be queried. Taken from the
        parameters file
    
    most_pop_unit (string): string that represents the most popular unit concept
        name for the particular measurement set.
        
    string_desc_concepts (string): string of all the descendant concept IDs that
        represent the concept_ids for the particular measurement set.
        
    ancestor_concept (int): used as the 'starting' point for all of the measurements.
        can hopefully capture multiple descendants that reflect the same type of
        measurement.
    
    Returns
    -------
    median (float): number that represents the 'median' of all the measurements
        for the measurement set that have the most popular unit concept
        
    tot_stdev (float): number that represents the 'standard deviation' of all the
        measurements for the measurement set that have the most popular unit concept
        
    tot_records (float): number of records (across all sites) that are being measured
        for the particular ancestor_concept_id, unit_concept_id, etc.
        
    mean (float): number that represents the 'mean' of all the measurements for the
        meawsurement set that have the most popular unit
        
    decile1 (float): number that represents the 10th percentile of all the measurements
        for the measurement set that have the most popular unit concept
        
    quartile1 (float): number that represents the 25th percentile of all the measurements
        for the measurement set that have the most popular unit concept
    
    quartile3 (float): number that represents the 75th percentile of all the measurements
        for the measurement set that have the most popular unit concept
    
    decile9 (float): number that represents the 90th percentile of all the measurements
        for the measurement set that have the most popular unit concept
        
    concept_name (string): string representing the concept name (cluster of measurements)
        that is being investigated
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

    measurements_for_lab_and_unit = pd.io.gbq.read_gbq(find_range_overall, dialect='standard')

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

    concept_name = pd.io.gbq.read_gbq(find_ancestor_lab, dialect='standard')
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

    print("There are {} records for concept: {}\n".format(
        num_records, concept_name))
    print("The 10th percentile is: {}".format(decile1))
    print("The 25th percentile is: {}".format(quartile1))
    print("The 50th percentile is: {}".format(median))
    print("The 75th percentile is: {}".format(quartile3))
    print("The 90th percentile is: {}\n".format(decile9))

    print("The mean is: {}".format(round(mean, 2)))
    print("The standard deviation is: {}".format(round(stdev, 2)))

    return median, stdev, num_records, mean, decile1, quartile1, median, quartile3, decile9, concept_name


def create_site_distribution_df(DATASET, string_desc_concepts, most_pop_unit):
    """
    Function is used to create and return a dataframe that shows the mean and standard deviation
    for a site's values (of a particular measurement set, for the most popular unit_concept)
    compared to the values for the entire dataset.
    
    Parameters
    ----------        
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
      ROUND(STDDEV_POP(m.value_as_number) OVER (PARTITION BY mm.src_hpo_id), 2) as stdev
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
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    ORDER BY median DESC
    """.format(DATASET, DATASET, DATASET, string_desc_concepts, most_pop_unit)

    site_value_distribution_df = pd.io.gbq.read_gbq(find_site_distribution, dialect='standard')

    return site_value_distribution_df


def generate_aggregate_df(median, decile1, quartile1, quartile3, decile9, stdev,
                          num_records, mean):
    """
    Function is used to generate a dataframe that contains information about the
    measurement values (of a particular measurement set, for the most popular unit)
    across all of the applicable sites.
    
    Parameters
    ----------
    median (float): number that represents the 'median' of all the measurements
        for the measurement set that have the most popular unit concept
        
    decile1 (float): number that represents the 10th percentile of all the measurements
        for the measurement set that have the most popular unit concept
        
    quartile1 (float): number that represents the 25th percentile of all the measurements
        for the measurement set that have the most popular unit concept
    
    quartile3 (float): number that represents the 75th percentile of all the measurements
        for the measurement set that have the most popular unit concept
    
    decile9 (float): number that represents the 90th percentile of all the measurements
        for the measurement set that have the most popular unit concept
    
    tot_stdev (float): number that represents the 'standard deviation' of all the
        measurements for the measurement set that have the most popular unit concept
        
    tot_records (float): number of records (across all sites) that are being measured
        for the particular ancestor_concept_id, unit_concept_id, etc.
        
    Returns
    -------
    aggregate_df (dataframe): dataframe that allows one to observe statistical information about
        the values (of a particular measurement set, for the most popular unit_concept)
        across all of the sites
    """

    data = [{
        'total_tenth_perc': round(decile1, 2),
        'total_first_quartile': round(quartile1, 2),
        'total_median': round(median, 2),
        'total_third_quartile': round(quartile3, 2),
        'total_ninetieth_perc': round(decile9, 2),
        'total_mean': round(mean, 2),
        'total_stdev': round(stdev, 2),
        'total_records': round(num_records, 2)
    }]

    aggregate_df = pd.DataFrame(data)

    return aggregate_df


# ## Below is the 'main' function that dictates most of the 'flow' of the analysis


def run_statistics(ancestor_concept, DATASET):
    """
    Function runs the statistics and created the dataframe for all of the measurements
    for a particular ancestor_concept_id (and its descendants).
    
    Parameters
    ----------
    ancestor_concept (int): used as the 'starting' point for all of the measurements.
        can hopefully capture multiple descendants that reflect the same type of
        measurement
        
    DATASET (string): string representing the dataset to be queried. Taken from the
        parameters file
        
    Returns
    -------
    site_value_distribution_df (dataframe): dataframe that allows one to compare
        each site's values (of a particular measurement set, for the most popular unit_concept)
        compared to the values for the entire dataset
        
    most_popular_unit (string): string that represents the most popular unit concept
        name for the particular measurement set
    
    concept_name (string): string representing the concept name (cluster of measurements)
        that is being investigated
    
    aggregate_df (dataframe): dataframe that allows one to observe statistical information about
        the values (of a particular measurement set, for the most popular unit_concept)
        across all of the sites
    """

    descendant_concepts = find_descendants(DATASET, ancestor_concept)

    tot_units = find_total_number_of_units_for_lab_type(DATASET,
                                                        descendant_concepts)

    most_popular_unit = find_most_popular_unit_type(tot_units, DATASET,
                                                    descendant_concepts)

    median, stdev, num_records, mean, decile1, quartile1, median, quartile3, decile9, concept_name = \
        metrics_for_whole_dataset(DATASET, most_popular_unit, descendant_concepts, ancestor_concept)

    site_value_distribution_df = create_site_distribution_df(
        DATASET, descendant_concepts, most_popular_unit)

    aggregate_df = generate_aggregate_df(median, decile1, quartile1, quartile3,
                                         decile9, stdev, num_records, mean)

    return site_value_distribution_df, most_popular_unit, concept_name, aggregate_df


# #### Below cell modified from this [StackOverflow](https://stackoverflow.com/questions/26678467/export-a-pandas-dataframe-as-a-table-image)


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


def replace_name(name):
    """
    Function is used to replace the characters of the given string (either the
    concept name or the unit name) to ensure that the file can be saved properly.
    
    Parameters
    ----------
    name (str): string representing either the concept or unit name
    
    Return
    ------
    name (str): string representing either the concept or unit name (now without
        characters that could mess up the saving process)
    """

    bad_chars = [" ", "/", ".", "-", "[", "]", ",", "(", ")", "|", ":"]

    for ch in bad_chars:
        name = name.replace(ch, "_")

    if name[-1:] == "_":
        name = name[:-1]  # chop off the final underscore

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


def add_significance_cols(df, aggregate_df):
    """
    Function is used to add columns to the dataframe to give information about how the HPOs'
    data for the selected measurement/unit combination compares to the overall measurement/unit
    combination (across all sites).
    
    These signficance metrics are based on Welch's t-test (unpaired t-test, using different subjects,
    not assuming equal variance.)
    
    parameters:
    -----------
    df (dataframe): dataframe that contains information (of a particular measurement set, 
        for the most popular unit_concept) about each of the applicable sites
        
    aggregate_df (dataframe): dataframe that contains information (of a particular measurement set,
        for the most popular unit_concept) across all of the applicable sites
        
    returns:
    --------
    df (dataframe): the same df as above but now with new columns to compare each site's data
        to the data of the entire dataset (using Welch's t-test)
    """
    t_vals, degrees_freedom, p_vals, sig_diff = [], [], [], []

    tot_mean = aggregate_df['total_mean'].iloc[0]
    tot_stdev = aggregate_df['total_stdev'].iloc[0]
    tot_rows = aggregate_df['total_records'].iloc[0]

    for idx, hpo in df.iterrows():
        hpo_mean = hpo['mean']
        hpo_stdev = hpo['stdev']
        hpo_rows = hpo['hpo_rows']

        numerator = tot_mean - hpo_mean

        denom = ((tot_stdev**2) / tot_rows) + ((hpo_stdev**2) / hpo_rows)
        denominator = denom**(1 / 2)

        try:
            t_val = numerator / denominator
        except ZeroDivisionError:
            t_val = 0

        if t_val > 0:  # standardize for the sake of the p-value calculation
            t_val = t_val * -1

        t_vals.append(round(t_val, 2))

        num = ((tot_stdev**2) / tot_rows + (hpo_stdev**2) / hpo_rows)**2
        denom_pt1 = (tot_stdev**2 / tot_rows)**2 / (tot_rows - 1)

        try:
            denom_pt2 = (hpo_stdev**2 / hpo_rows)**2 / (hpo_rows - 1)
            denom = denom_pt1 + denom_pt2
        except ZeroDivisionError:
            denom = denom_pt1

        deg_freedom = num / denom

        degrees_freedom.append(round(deg_freedom, 2))

        p = scipy.stats.t.cdf(t_val, df=deg_freedom) * 2  # for two-tailed info

        if p < 0.05:
            sig_diff.append("True")
        else:
            sig_diff.append("False")

        p_vals.append(round(p, 4))

    df['t_val'] = t_vals
    df['degrees_freedom'] = degrees_freedom
    df['p_val'] = p_vals
    df['sig_diff'] = sig_diff

    return df


def process_image(concept_name, df, most_popular_unit, aggregate_df):
    """
    Function is used to:
        a. Create the name of the resulting image
        b. Save the image
    
    Parameters
    ----------
    concept_name (string): string representing the concept name (cluster of measurements)
        that is being investigated
    
    df (dataframe): dataframe that allows one to compare each site's values (of a 
        particular measurement set, for the most popular unit_concept) compared to the values 
        for the entire dataset
        
    most_popular_unit (string): string that represents the most popular unit concept
        name for the particular measurement set
        
    aggregate_df (dataframe): dataframe that contains information (of a particular measurement set,
        for the most popular unit_concept) across all of the applicable sites
    
    Returns
    -------
    img_name (string): name of the image of the formatted dataframe that will be saved
    
    concept_name (string): string that represents the concept name (cluster of measurements)
        after formatting
    """
    concept_name = replace_name(concept_name)
    unit_name = replace_name(most_popular_unit)

    if not aggregate_df:
        img_name = concept_name + "_" + unit_name + "_site_value_distributions.png"
    else:
        img_name = concept_name + "_" + unit_name + "_aggregate_df.png"

    ax = render_mpl_table(df, header_columns=0, col_width=2.0)
    plt.savefig(img_name, bbox_inches="tight")

    return img_name, concept_name


def create_statistics_dictionary(df, aggregate_df):
    """
    Function is used to create lists that can be used in creating a box-and-whisker
    plot using the bxp function.
    
    Parameters
    ----------
    df (dataframe): dataframe that shows each site's statistical values (of a 
        particular measurement set, for the most popular unit_concept)
    
    aggregate_df (dataframe): dataframe that contains information (of a particular measurement
        set, for the most popular unit_concept) across all of the applicable sites
        
    Returns
    -------
    lst (list): contains a series of dictionaries. each dictionary is used to represent one
        site (or the information across all of the sites). the key:value pair of this dictionary
        has a particular statistic:value (e.g. ninetieth percentile:150)
    
    names (list): list of the HPO names that make up the rows of the dataframe
    """
    stats = {}

    tot_min, tot_max = 9999999999, -999999999

    for idx, row in df.iterrows():
        hpo = row['src_hpo_id']

        stats[hpo] = {}
        stats[hpo]['mean'] = row['mean']
        stats[hpo]['whislo'] = row['tenth_perc']
        stats[hpo]['q1'] = row['first_quartile']
        stats[hpo]['med'] = row['median']
        stats[hpo]['q3'] = row['third_quartile']
        stats[hpo]['whishi'] = row['ninetieth_perc']

        minimum, maximum = row['min'], row['max']

        if minimum < tot_min:
            tot_min = minimum
        if maximum > tot_max:
            tot_max = maximum

        stats[hpo]['fliers'] = np.array([row['min'], row['max']])

    stats['aggregate_info'] = {}
    stats['aggregate_info']['mean'] = aggregate_df['total_mean'].iloc[0]
    stats['aggregate_info']['whislo'] = aggregate_df['total_tenth_perc'].iloc[0]
    stats['aggregate_info']['q1'] = aggregate_df['total_first_quartile'].iloc[0]
    stats['aggregate_info']['med'] = aggregate_df['total_median'].iloc[0]
    stats['aggregate_info']['q3'] = aggregate_df['total_third_quartile'].iloc[0]
    stats['aggregate_info']['whishi'] = aggregate_df[
        'total_ninetieth_perc'].iloc[0]
    stats['aggregate_info']['fliers'] = np.array([minimum, maximum])

    lst = []
    names = []

    for key, value in stats.items():
        names.append(key)
        lst.append(value)

    return lst, names


def format_title(title):
    """
    Function is used to create the title of a graph based on the measurement
    concept and unit concept being used. This ensures that the new title
    has capitalized letters and lacks underscores.
    
    Parameters
    ----------
    title (string): name of the image of the formatted dataframe that will be saved. now
        lacks everything except for the measurement set name and the unit concept name.
        
    Returns
    -------
    title (string): title that can now be used for the graph (properly formatted)
    
    """
    s = list(title)

    for idx, char in enumerate(s):
        if idx == 0:
            s[idx] = s[idx].capitalize()
        elif s[idx] == '_':
            s[idx] = ' '
            s[idx + 1] = s[idx + 1].capitalize()

    title = "".join(s)

    return title


def display_boxplot(lst, img_name, names):
    """
    Function is used to create a box-and-whisker plot based on the information
    about the measurement values for each of the sites and across all of the 
    sites. This function ensures that the axes are labeled properly and that
    the saved image is formatted appropriately.
    
    Parameters
    ----------
    lst (list): contains a series of dictionaries. each dictionary is used to represent one
        site (or the information across all of the sites). the key:value pair of this dictionary
        has a particular statistic:value (e.g. ninetieth percentile:150)
    
    img_name (string): name of the image of the formatted dataframe that will be saved
    
    names (list): list of the HPO names that make up the rows of the dataframe
    """

    fig, ax = plt.subplots()
    ticks = []
    
    for x in range(len(names)):
        ticks.append(x + 1)

    ax.bxp(lst, showfliers=False, vert=False, showmeans=True)
    ax.legend()
    plt.yticks(ticks=ticks, labels=names)

    x_label = format_title(
        img_name[:-29])  # get rid of site_value_distributions.png

    plt.title(x_label + " By Site")
    plt.xlabel(x_label)
    plt.ylabel('Site')
    plt.legend()

    end = '_box_and_whisker.png'

    if img_name[:-30] == '_':
        img_name = img_name[:-30] + end
    else:
        img_name = img_name[:-29] + end

    plt.savefig(img_name, bbox_inches="tight")

    plt.show()


# ## Below does the bulk of the 'heavy lifting' of this script

for ancestor_id in measurement_ancestors:
    site_value_distribution_df, most_popular_unit, concept_name, aggregate_df = \
        run_statistics(ancestor_id, DATASET)

    df = add_significance_cols(site_value_distribution_df, aggregate_df)

    img_name, concept_name = process_image(concept_name,
                                           df,
                                           most_popular_unit,
                                           aggregate_df=False)

    # returns are not used much
    x, a = process_image(concept_name,
                         aggregate_df,
                         most_popular_unit,
                         aggregate_df=True)

    info, hpo_names = create_statistics_dictionary(df, aggregate_df)
    
    print(hpo_names)

    display_boxplot(info, img_name, hpo_names)






