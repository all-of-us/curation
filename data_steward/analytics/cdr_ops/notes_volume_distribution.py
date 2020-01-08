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

import sys
print(sys.executable)

# +
from notebooks import bq, render, parameters

# %matplotlib inline
import matplotlib.pyplot as plt
import numpy as np
from pandas.plotting import table 
# %matplotlib inline
import six
import scipy.stats
import math
import seaborn as sns
import pandas as pd
import matplotlib
from os import path
from PIL import Image
from wordcloud import WordCloud, STOPWORDS, ImageColorGenerator

# +
DATASET = parameters.LATEST_DATASET

print("""
DATASET TO USE: {}
""".format(DATASET))

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

note_df = bq.query(general_notes_query)
# -

note_df


def create_dicts_w_info(df, column_label):
    
    hpos = df['src_hpo_id'].unique().tolist()
    
    site_dictionaries = {}

    for hpo in hpos:   
        sample_df = df.loc[df['src_hpo_id'] == hpo]
        
        data = sample_df.iloc[0][column_label]

        site_dictionaries[hpo] = data
    
    return site_dictionaries


def create_graphs(info_dict, xlabel, ylabel, title, img_name, color, total_diff_color):
    """
    Function is used to create graphs for each of the
    
    """
    bar_list = plt.bar(range(len(info_dict)), list(info_dict.values()), align='center', color = color)
    
    # used to change the color of the 'aggregate' column; usually implemented for an average
    if total_diff_color:
        bar_list[len(info_dict) - 1].set_color('r')
    
    
    plt.xticks(range(len(info_dict)), list(info_dict.keys()), rotation='vertical')
    plt.ylabel(ylabel)
    plt.xlabel(xlabel)
    plt.title(title)
    #plt.show()
    plt.savefig(img_name, bbox_inches="tight")

# +
gen_note_dictionary = create_dicts_w_info(note_df, 'num_notes')

create_graphs(info_dict=gen_note_dictionary, xlabel='Site', ylabel='Number of Notes', 
              title='Number of General Notes By Site', img_name='gen_notes_by_site.jpg',
              color='b', total_diff_color=False)

# +
notes_w_title_query = """
SELECT
DISTINCT
mn.src_hpo_id, COUNT(*) as num_notes
FROM
`aou-res-curation-prod.ehr_ops.unioned_ehr_note` n
JOIN
`aou-res-curation-prod.ehr_ops._mapping_note` mn
ON
mn.note_id = n.note_id
WHERE
n.note_title IS NOT NULL
GROUP BY 1
ORDER BY mn.src_hpo_id, num_notes DESC
""".format(DATASET, DATASET)

note_title_df = bq.query(general_notes_query)

# +
gen_note_title_dictionary = create_dicts_w_info(note_title_df, 'num_notes')

create_graphs(info_dict=gen_note_title_dictionary, xlabel='Site', ylabel='Number of Notes', 
              title='Number of Notes with Titles By Site', img_name='notes_w_titles_by_site.jpg',
              color='b', total_diff_color=False)

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
    `aou-res-curation-prod.ehr_ops.unioned_ehr_note` n
    JOIN
    `aou-res-curation-prod.ehr_ops._mapping_note` mn
    ON
    mn.note_id = n.note_id
    WHERE
    n.note_title IS NOT NULL
    GROUP BY 1
    ORDER BY num_notes DESC) a
WHERE a.num_notes > 10000
ORDER BY a.num_notes DESC
"""

titles_df = bq.query(note_titles_query)
# -

titles_df

# +
titles_dict = {}

for index, row in titles_df.iterrows():
    title = row['note_title']
    cnt = row['num_notes']
    
    if title in titles_dict:
        titles_dict[title.lower()] += cnt
    else:
        titles_dict[title.lower()] = cnt

print(titles_dict)
# -

# ?WordCloud

# +
novel = ""

for title, num in titles_dict.items():
    new_string = title + " "
    tot_string = new_string * num
    
    novel += tot_string
# -

print(len(novel))

# +
wordcloud = WordCloud().generate(text = novel)

plt.imshow(wordcloud, interpolation='bilinear')
plt.axis("off")
plt.show()

# +
wordcloud = WordCloud().generate(text = 'jelly belly smelly jelly jelly jelly jelly jellyjelly jelly jelly jelly jelly jelly')

plt.imshow(wordcloud, interpolation='bilinear')
plt.axis("off")
plt.show()
# -


