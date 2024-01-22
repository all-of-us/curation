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

# # Script is used to assess the number of notes for each site.

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
from wordcloud import WordCloud

# +
DATASET = parameters.LATEST_DATASET

print("""
DATASET TO USE: {}
""".format(DATASET))
# -

# #### Simple query

# +
general_notes_query = """
SELECT
DISTINCT
mn.src_hpo_id, COUNT(*) as num_notes
FROM
`{}.unioned_ehr_note` n
JOIN
`{}._mapping_note` mn
ON
mn.note_id = n.note_id
GROUP BY 1
ORDER BY mn.src_hpo_id
""".format(DATASET, DATASET)

note_df = pd.io.gbq.read_gbq(general_notes_query, dialect='standard')
# -

note_df


def create_dicts_w_info(df, column_label):
    """
    This function is used to create a dictionary that can be easily converted to a
    graphical representation based on the values for a particular dataframe

    Parameters
    ----------
    df (dataframe): dataframe that contains the information to be converted

    column_label (string): the column of the dataframe whose rows will then be
        converted to the keys of the dictionary
    """
    hpos = df['src_hpo_id'].unique().tolist()

    site_dictionaries = {}

    for hpo in hpos:
        sample_df = df.loc[df['src_hpo_id'] == hpo]

        data = sample_df.iloc[0][column_label]

        site_dictionaries[hpo] = data

    return site_dictionaries


def create_graphs(info_dict, xlabel, ylabel, title, img_name, colour,
                  total_diff_colour):
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

    colour (str): character used to specify the colours of the bars

    total_diff_colour (bool): indicates whether or not the last bar should be coloured red (
        as opposed to the rest of the bars on the graph). This is typically used when the ultimate
        value of the dictionary is of particular important (e.g. representing an 'aggregate' metric
        across all of the sites)
    """
    bar_list = plt.bar(range(len(info_dict)),
                       list(info_dict.values()),
                       align='center',
                       color=colour)

    # used to change the color of the 'aggregate' column; usually implemented for an average
    if total_diff_colour:
        bar_list[len(info_dict) - 1].set_color('r')

    plt.xticks(range(len(info_dict)),
               list(info_dict.keys()),
               rotation='vertical')
    plt.ylabel(ylabel)
    plt.xlabel(xlabel)
    plt.title(title)
    #plt.show()
    plt.savefig(img_name, bbox_inches="tight")


# +
gen_note_dictionary = create_dicts_w_info(note_df, 'num_notes')

create_graphs(info_dict=gen_note_dictionary,
              xlabel='Site',
              ylabel='Number of Notes',
              title='Number of General Notes By Site',
              img_name='gen_notes_by_site.jpg',
              colour='b',
              total_diff_colour=False)
# -

# ### May want to differentiate notes that have titles (that are not '0')

# +
notes_w_title_query = """
SELECT
DISTINCT
mn.src_hpo_id, COUNT(*) as num_notes
FROM
`{}.unioned_ehr_note` n
JOIN
`{}._mapping_note` mn
ON
mn.note_id = n.note_id
WHERE
n.note_title IS NOT NULL
AND
n.note_title NOT LIKE '0'
GROUP BY 1
ORDER BY mn.src_hpo_id, num_notes DESC
""".format(DATASET, DATASET)

note_title_df = pd.io.gbq.read_gbq(general_notes_query, dialect='standard')

# +
gen_note_title_dictionary = create_dicts_w_info(note_title_df, 'num_notes')

create_graphs(info_dict=gen_note_title_dictionary,
              xlabel='Site',
              ylabel='Number of Notes',
              title='Number of Notes with Titles By Site',
              img_name='notes_w_titles_by_site.jpg',
              colour='b',
              total_diff_colour=False)

# +
zero_titles_query = """
SELECT
DISTINCT
mn.src_hpo_id, COUNT(*) as num_notes
FROM
`{}.unioned_ehr_note` n
JOIN
`{}._mapping_note` mn
ON
mn.note_id = n.note_id
WHERE
n.note_title LIKE '0'
GROUP BY 1
ORDER BY mn.src_hpo_id, num_notes DESC
""".format(DATASET, DATASET)

zero_df = pd.io.gbq.read_gbq(zero_titles_query, dialect='standard')

# +
zero_note_title_dictionary = create_dicts_w_info(zero_df, 'num_notes')

create_graphs(info_dict=zero_note_title_dictionary,
              xlabel='Site',
              ylabel='Number of Notes',
              title='Number of Notes with "0" Titles By Site',
              img_name='notes_w_zero_titles_by_site.jpg',
              colour='b',
              total_diff_colour=False)
# -

# ### Only want to take notes with > 10000
#
# NOTE: You may modify the query to exclude note_title LIKE '%0%'

# +
note_titles_query = """
SELECT
DISTINCT
a.*
FROM
    (SELECT
    DISTINCT
    n.note_title, COUNT(*) as num_notes
    FROM
    `{}.unioned_ehr_note` n
    JOIN
    `{}._mapping_note` mn
    ON
    mn.note_id = n.note_id
    WHERE
    n.note_title IS NOT NULL
    GROUP BY 1
    ORDER BY num_notes DESC) a
WHERE a.num_notes > 10000
ORDER BY a.num_notes DESC
LIMIT 30
""".format(DATASET, DATASET)

titles_df = pd.io.gbq.read_gbq(note_titles_query, dialect='standard')
# -

titles_df

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
ax = render_mpl_table(titles_df, header_columns=0, col_width=2.0)

plt.tight_layout()

plt.savefig('note_frequencies_desc', bbox_inches="tight")

# +
titles_dict = {}

for index, row in titles_df.iterrows():
    title = row['note_title']
    cnt = row['num_notes']

    title_words = title.split()

    ## creating a list with all of the words
    title_lst = [
        word for word in title_words if word.lower() not in ['note', 'notes']
    ]
    title = ' '.join(title_lst)

    if title in titles_dict:
        titles_dict[title.lower()] += cnt
    else:
        titles_dict[title.lower()] = cnt
# -

# ### Converting the list with note titles/frequencies to a string. This string can then be used to generate a wordcloud.

# +
novel = ""

for title, num in titles_dict.items():
    new_string = title + " "

    # NOTE divide by 10,000 to ensure that the wordcloud is not overloaded
    num = int(round(num / 10000, 0))
    tot_string = new_string * num

    novel += tot_string
# -

# ### Please see the [WordCloud documentation](https://amueller.github.io/word_cloud/generated/wordcloud.WordCloud.html) for more information on the functions/parameters associated with the WordCloud object

# +
wordcloud = WordCloud(relative_scaling=0.5).fit_words(titles_dict)

plt.imshow(wordcloud, interpolation='bilinear')
plt.axis("off")
plt.savefig('word_cloud_note_titles.png', bbox_inches="tight")
plt.show()
# -


