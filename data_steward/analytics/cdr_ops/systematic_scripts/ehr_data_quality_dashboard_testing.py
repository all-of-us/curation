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

# ## Notebook is intended to compare the EHR data in the latest AoU dataset to the thresholds established by the [OHDSI DataQualityDashboard](https://github.com/OHDSI/DataQualityDashboard)

from google.cloud import bigquery
# %reload_ext google.cloud.bigquery
client = bigquery.Client()
# %load_ext google.cloud.bigquery

# +
import utils.bq
from notebooks import parameters

# %matplotlib inline
import matplotlib.pyplot as plt
import numpy as np
import six
import scipy.stats
import pandas as pd

# +
DATASET = parameters.LATEST_DATASET

print("""
DATASET TO USE: {}
""".format(DATASET))
# -

# ### The below query is used to generate a 'measurement dataframe'. This dataframe shows each of the measurement/unit combinations that appear in BOTH the data quality dashboard and the unioned_ehr_measurement table. These are 'matched' only if the input in the unioned_ehr dataframe has both the correct measurement and unit.
#
# ### Beyond displaying the name-matched measurement/unit combinations, the resulting table shows:
# - The plausible "low" and "high" values defined by the data quality dashboard team
# - The total number of values for the measurement/unit combination that are outside these "plausible" values
# - The total number of values for the measurement/unit combination (both 'plausible' and 'not plausible')
# - The percentage of all values for the measurement/unit combination that are implausible

# +
measurement_df_query = """
SELECT
DISTINCT
num_vals_total.measurement_name, num_vals_total.unit_name, num_vals_total.plausibleValueLow, num_vals_total.plausibleValueHigh,
IFNULL(SUM(imp_vals.number_implausible_vals), 0) as num_implausible_vals,
IFNULL(SUM(num_vals_total.num_vals_total), 0) as num_values_tot,
IFNULL(ROUND(SUM(imp_vals.number_implausible_vals) / SUM(num_vals_total.num_vals_total) * 100, 2), 0) as percent_implausible_vals
FROM
(SELECT
DISTINCT
mm.src_hpo_id, c.concept_name as measurement_name, c2.concept_name as unit_name, a.plausibleValueLow, a.plausibleValueHigh,
COUNT(mm.src_hpo_id) as num_vals_total
FROM
`{}.unioned_ehr_measurement` m
JOIN
`{}._mapping_measurement` mm
ON
mm.measurement_id = m.measurement_id
JOIN
`{}.concept` c
ON
c.concept_id = m.measurement_concept_id
JOIN
`{}.concept` c2
ON
c2.concept_id = m.unit_concept_id
JOIN
  (
  SELECT
  DISTINCT
  c.concept_name, dqd.plausibleValueLow, dqd.plausibleValueHigh, c2.concept_name as unit_name
  FROM
  `{}.dqd_concept_level` dqd
  JOIN
  `{}.concept` c
  ON
  dqd.conceptId = c.concept_id
  JOIN
  `{}.concept` c2
  ON
  dqd.unitConceptId = c2.concept_id
  ORDER BY dqd.plausibleValueHigh DESC) a
ON
LOWER(a.concept_name) = LOWER(c.concept_name)
AND
LOWER(a.unit_name) = LOWER(c2.concept_name)
GROUP BY 1, 2, 3, 4, 5
ORDER BY measurement_name DESC, mm.src_hpo_id
) num_vals_total
LEFT JOIN
(SELECT
DISTINCT
mm.src_hpo_id, c.concept_name as measurement_name, c2.concept_name as unit_name, a.plausibleValueLow, a.plausibleValueHigh,
COUNT(mm.src_hpo_id) as number_implausible_vals
FROM
`{}.unioned_ehr_measurement` m
JOIN
`{}._mapping_measurement` mm
ON
mm.measurement_id = m.measurement_id
JOIN
`{}.concept` c
ON
c.concept_id = m.measurement_concept_id
JOIN
`{}.concept` c2
ON
c2.concept_id = m.unit_concept_id
JOIN
  (
  SELECT
  DISTINCT
  c.concept_name, dqd.plausibleValueLow, dqd.plausibleValueHigh, c2.concept_name as unit_name
  FROM
  `{}.dqd_concept_level` dqd
  JOIN
  `{}.concept` c
  ON
  dqd.conceptId = c.concept_id
  JOIN
  `{}.concept` c2
  ON
  dqd.unitConceptId = c2.concept_id
  ORDER BY dqd.plausibleValueHigh DESC) a
ON
LOWER(a.concept_name) = LOWER(c.concept_name)
AND
LOWER(a.unit_name) = LOWER(c2.concept_name)
WHERE
m.value_as_number < a.plausibleValueLow
OR
m.value_as_number > a.plausibleValueHigh
GROUP BY 1, 2, 3, 4, 5
ORDER BY measurement_name DESC, mm.src_hpo_id
) imp_vals
ON
LOWER(imp_vals.src_hpo_id) = LOWER(num_vals_total.src_hpo_id)
AND
LOWER(imp_vals.measurement_name) = LOWER(num_vals_total.measurement_name)
AND
imp_vals.plausibleValueLow = num_vals_total.plausibleValueLow
AND
imp_vals.plausibleValueHigh = num_vals_total.plausibleValueHigh
AND
LOWER(imp_vals.unit_name) = LOWER(num_vals_total.unit_name) -- to ensure looking at the same unit
GROUP BY 1, 2, 3, 4
ORDER BY percent_implausible_vals DESC
""".format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, \
           DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET)

measurement_df = pd.io.gbq.read_gbq(measurement_df_query, dialect='standard')
# -

# ##### Creating copies of the measurement dataframe. Enables further exploration/manipulation without needing to re-run the above query.

c1 = measurement_df
c2 = measurement_df

# ### The below query is used to generate a 'HPO dataframe'. This dataframe shows:
# - Each of the HPOs
# - The total number of values submitted by the HPO that match a particular measurement/unit combination specified in the data quality dashboard
# - Of the above values, the proportion of said value_as_number values that explicitly lie outside the 'plausible' range specified by the DQD team
#
# NOTE: Much of the logic behind this query is very similar to the query above (the main difference lies in the outermost SELECT DISTINCT)

# +
hpo_query = """
SELECT
DISTINCT
z.src_hpo_id, SUM(z.num_implausible_vals) as tot_implaus, SUM(z.num_values_tot) as tot_values, ROUND( SUM(z.num_implausible_vals) * 100 / SUM(z.num_values_tot), 2) as percent_implaus
FROM
(   SELECT
    DISTINCT
    num_vals_total.src_hpo_id, num_vals_total.measurement_name, num_vals_total.unit_name, num_vals_total.plausibleValueLow, num_vals_total.plausibleValueHigh,
    IFNULL(SUM(imp_vals.number_implausible_vals), 0) as num_implausible_vals,
    IFNULL(SUM(num_vals_total.num_vals_total), 0) as num_values_tot,
    IFNULL(ROUND(SUM(imp_vals.number_implausible_vals) / SUM(num_vals_total.num_vals_total) * 100, 2), 0) as percent_implausible_vals
    FROM
    (SELECT
    DISTINCT
    mm.src_hpo_id, c.concept_name as measurement_name, c2.concept_name as unit_name, a.plausibleValueLow, a.plausibleValueHigh,
    COUNT(mm.src_hpo_id) as num_vals_total
    FROM
    `{}.unioned_ehr_measurement` m
    JOIN
    `{}._mapping_measurement` mm
    ON
    mm.measurement_id = m.measurement_id
    JOIN
    `{}.concept` c
    ON
    c.concept_id = m.measurement_concept_id
    JOIN
    `{}.concept` c2
    ON
    c2.concept_id = m.unit_concept_id
    JOIN
      (
      SELECT
      DISTINCT
      c.concept_name, dqd.plausibleValueLow, dqd.plausibleValueHigh, c2.concept_name as unit_name
      FROM
      `{}.dqd_concept_level` dqd
      JOIN
      `{}.concept` c
      ON
      dqd.conceptId = c.concept_id
      JOIN
      `{}.concept` c2
      ON
      dqd.unitConceptId = c2.concept_id
      ORDER BY dqd.plausibleValueHigh DESC) a
    ON
    LOWER(a.concept_name) = LOWER(c.concept_name)
    AND
    LOWER(a.unit_name) = LOWER(c2.concept_name)
    GROUP BY 1, 2, 3, 4, 5
    ORDER BY measurement_name DESC, mm.src_hpo_id
    ) num_vals_total
    LEFT JOIN
    (SELECT
    DISTINCT
    mm.src_hpo_id, c.concept_name as measurement_name, c2.concept_name as unit_name, a.plausibleValueLow, a.plausibleValueHigh,
    COUNT(mm.src_hpo_id) as number_implausible_vals
    FROM
    `{}.unioned_ehr_measurement` m
    JOIN
    `{}._mapping_measurement` mm
    ON
    mm.measurement_id = m.measurement_id
    JOIN
    `{}.concept` c
    ON
    c.concept_id = m.measurement_concept_id
    JOIN
    `{}.concept` c2
    ON
    c2.concept_id = m.unit_concept_id
    JOIN
      (
      SELECT
      DISTINCT
      c.concept_name, dqd.plausibleValueLow, dqd.plausibleValueHigh, c2.concept_name as unit_name
      FROM
      `{}.dqd_concept_level` dqd
      JOIN
      `{}.concept` c
      ON
      dqd.conceptId = c.concept_id
      JOIN
      `{}.concept` c2
      ON
      dqd.unitConceptId = c2.concept_id
      ORDER BY dqd.plausibleValueHigh DESC) a
    ON
    LOWER(a.concept_name) = LOWER(c.concept_name)
    AND
    LOWER(a.unit_name) = LOWER(c2.concept_name)
    WHERE
    m.value_as_number < a.plausibleValueLow
    OR
    m.value_as_number > a.plausibleValueHigh
    GROUP BY 1, 2, 3, 4, 5
    ORDER BY measurement_name DESC, mm.src_hpo_id
    ) imp_vals
    ON
    LOWER(imp_vals.src_hpo_id) = LOWER(num_vals_total.src_hpo_id)
    AND
    LOWER(imp_vals.measurement_name) = LOWER(num_vals_total.measurement_name)
    AND
    imp_vals.plausibleValueLow = num_vals_total.plausibleValueLow
    AND
    imp_vals.plausibleValueHigh = num_vals_total.plausibleValueHigh
    AND
    LOWER(imp_vals.unit_name) = LOWER(num_vals_total.unit_name) -- to ensure looking at the same unit
    GROUP BY 1, 2, 3, 4, 5
    ORDER BY num_vals_total.measurement_name DESC, percent_implausible_vals DESC, num_vals_total.src_hpo_id) z
GROUP BY 1
ORDER BY percent_implaus DESC
""".format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, \
           DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET)

hpo_df = pd.io.gbq.read_gbq(hpo_query, dialect='standard')
# -

# ##### Creating copies of the HPO dataframe. Enables further exploration/manipulation without needing to re-run the above query.

c3 = hpo_df
c4 = hpo_df


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


# #### Below is used to add a column to show the 'accountability' of a particular HPO. This means how much their 'implausible' values contribute to the 'implausible values' across all of the sites (expressed as a percentage)
#
# NOTE: exclude the end of the 'tot_implaus' column in the denominator to avoid the existing 'total' value

# +
hpo_df['percent_of_implaus'] = round(
    hpo_df['tot_implaus'] / sum(hpo_df['tot_implaus'].tolist()[:-1]) * 100, 2)

hpo_df = hpo_df.sort_values(by=['percent_of_implaus'], ascending=False)
# -

# #### Below adds a 'total' row to the bottom of the HPO dataframe

# +
hpo_df = hpo_df.append(hpo_df.sum(numeric_only=True).rename('Total'))

hpo_names = hpo_df['src_hpo_id'].tolist()

hpo_names[-1:] = ["Total"]

hpo_df['src_hpo_id'] = hpo_names
# -

# #### redo the 'percent_implaus' column for the total row

hpo_df['percent_implaus'] = round(
    hpo_df['tot_implaus'] / hpo_df['tot_values'] * 100, 2)

# #### redo the 'percent_of_implaus' column for the total row

hpo_df['percent_of_implaus'] = round(
    hpo_df['tot_implaus'] / sum(hpo_df['tot_implaus'].tolist()[:-1]) * 100, 2)

# #### NOTE: the cell below is adapted largely from [this GitHub link](https://stackoverflow.com/questions/19726663/how-to-save-the-pandas-dataframe-series-data-as-a-figure)


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


# +
ax = render_mpl_table(hpo_df, header_columns=0, col_width=2.0)

plt.tight_layout()

plt.savefig('site_implausibility_totals.jpg', bbox_inches="tight")

# +
percent_imp_dictionary = create_dicts_w_info(hpo_df, 'src_hpo_id',
                                             'percent_implaus')

create_graphs(info_dict=percent_imp_dictionary,
              xlabel='Site',
              ylabel='% of Values that are Implausible',
              title='Percentage of Values that are Implausible by Site',
              img_name='percentage_of_implausible_values_distribution.png',
              color='b',
              total_diff_color=True,
              turnoff_x=False)

# +
hpo_list = hpo_df['src_hpo_id'].tolist()[:-1]  # take off the total

implaus_perc_by_hpo = hpo_df['percent_of_implaus'].tolist()[:-1]

labels = []

# creating the labels for the graph
for hpo, perc in zip(hpo_list, implaus_perc_by_hpo):
    string = '{}, {}%'.format(hpo, perc)
    labels.append(string)

wedges = [0.1] * len(labels)

plt.pie(implaus_perc_by_hpo,
        labels=None,
        shadow=True,
        startangle=140,
        explode=wedges)

plt.axis('equal')
plt.title("Percentage of Total 'Implausible' Records by Site")
plt.legend(bbox_to_anchor=(0.5, 0.75, 1.0, 0.85), labels=labels)

img_name = 'percent_implausible_by_site.jpg'
plt.savefig(img_name, bbox_inches="tight")

plt.show()

# +
hpo_dict = create_dicts_w_info(hpo_df, 'src_hpo_id', 'percent_of_implaus')

del hpo_dict["Total"]  # want to get rid of the total (which will be ~100%)

create_graphs(info_dict=hpo_dict,
              xlabel='Site',
              ylabel='Percentage of Implausible Values',
              title="Spread of Implausible Values by Site",
              img_name='spread_implausible_values.jpg',
              color='b',
              total_diff_color=False,
              turnoff_x=False)
# -

measurement_df

c1 = measurement_df
c2 = measurement_df

# #### Below is used to add an 'aggregate' row that shows data quality across all of the different measurement/unit combinations specified by the data quality dashboard

# +
measurement_df = measurement_df.append(
    measurement_df.sum(numeric_only=True).rename('Total'))

measurement_names = measurement_df['measurement_name'].tolist()

measurement_names[-1:] = ["Total Measurements"]

measurement_df['measurement_name'] = measurement_names
# -

# #### Recreating the 'percent_implausbile_vals' column (mostly for the final aggregate row)

measurement_df['percent_implausible_vals'] = round(
    measurement_df['num_implausible_vals'] / measurement_df['num_values_tot'] *
    100, 2)

measurement_df

# +
meas_dict = create_dicts_w_info(measurement_df, 'measurement_name',
                                'percent_implausible_vals')

create_graphs(info_dict=meas_dict,
              xlabel='Measurement / Unit Combination',
              ylabel='Proportion of Values that are Implausible',
              title="Proportion of Implausible Values by Measurement/Unit",
              img_name='proportion_implausible_by_measurement.png',
              color='b',
              total_diff_color=True,
              turnoff_x=True)
