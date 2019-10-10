# -*- coding: utf-8 -*-
from defaults import DEFAULT_DATASETS

# +
import bq
import pandas as pd

pd.set_option('display.max_colwidth', -1)
RDR = DEFAULT_DATASETS.latest.rdr
# -

# For the race question (`observation_concept_id=1586140`) a generalization rule is applied such that responses with multiple selected races are replaced by a response containing `2000000008` "More than one population".
#
# In the case where `1586147` “Hispanic” is one of the **two** selected races: 
# * if the other selected race is `1586146` "White", we leave the response as-is
# * otherwise we replace the other selection with `2000000001` "Another single population"
#
# In the case where there are **more than two** races selected, including `1586147` "Hispanic" we replace all other selected races with `2000000008` "More than one population".

from IPython.display import display, HTML
def display_dataframe(df):
    if len(df) == 0:
        html = HTML('<div class="alert alert-info">There are no records in the dataframe.</div>')
    else:
        html = HTML(df.to_html())
    display(html)


MULTIRACIAL_DIST_QUERY = """
WITH race_combo AS
(SELECT person_id, 
  questionnaire_response_id, 
  STRING_AGG(REPLACE(value_source_value, 'WhatRaceEthnicity_', ''), ' ' ORDER BY value_source_value) selected_races
 FROM {RDR}.observation
 WHERE observation_source_concept_id = 1586140
 GROUP BY person_id, questionnaire_response_id)
 
SELECT 
  selected_races, 
  COUNT(DISTINCT person_id) row_count
FROM race_combo 
GROUP BY selected_races
ORDER BY selected_races
"""
multi_race_count_df = bq.query(MULTIRACIAL_DIST_QUERY.format(RDR=RDR))

display_dataframe(multi_race_count_df)

DUPLICATE_GEN_RACE_QUERY = """
SELECT o.questionnaire_response_id, 
  o.person_id, 
  o.value_source_concept_id, 
  COUNT(1) row_count
 FROM {DEID}.observation o
WHERE observation_source_concept_id = 1586140
GROUP BY o.questionnaire_response_id, o.person_id, o.value_source_concept_id
HAVING row_count > 1
"""
