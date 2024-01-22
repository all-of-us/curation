# -*- coding: utf-8 -*-
# +
import utils.bq
from notebooks import render, parameters
import pandas as pd

pd.set_option('display.max_colwidth', -1)
RDR = parameters.RDR_DATASET_ID
DEID = parameters.DEID_DATASET_ID
VOCAB = parameters.VOCAB_DATASET_ID
# -

print("""RDR = {RDR}
DEID = {DEID}
VOCAB = {VOCAB}""".format(RDR=RDR, DEID=DEID, VOCAB=VOCAB))

# For the race question (`observation_concept_id=1586140`) a generalization rule is applied such that responses with multiple selected races are replaced by a response containing `2000000008` "More than one population".
#
# In the case where `1586147` “Hispanic” is one of the **two** selected races:
# * if the other selected race is `1586142` "Asian" OR `1586143` "Black" OR `1586146` "White", we leave the response as-is
# * otherwise we replace the other selection with `2000000001` "Another single population"
#
# In the case where there are **more than two** races selected, including `1586147` "Hispanic" we replace all other selected races with `2000000008` "More than one population".

# # Counts for all race combo responses

MULTIRACIAL_DIST_QUERY = """
WITH race_combo AS
(SELECT o.person_id,
  o.questionnaire_response_id,
  STRING_AGG(REPLACE(c.concept_code, 'WhatRaceEthnicity_', ''), ' ' ORDER BY value_source_value) selected_races
 FROM {DATASET}.observation o
 JOIN {VOCAB}.concept c ON o.value_source_concept_id = c.concept_id
 WHERE observation_source_concept_id = 1586140
 GROUP BY person_id, questionnaire_response_id)

SELECT
  selected_races,
  (LENGTH(selected_races) - LENGTH(REPLACE(selected_races, ' ', '')) + 1) AS selected_count,
  COUNT(DISTINCT person_id) row_count
FROM race_combo
GROUP BY selected_races
ORDER BY selected_count, selected_races
"""

render.md('## In dataset `{RDR}`'.format(RDR=RDR))

q = MULTIRACIAL_DIST_QUERY.format(DATASET=RDR, VOCAB=VOCAB)
multi_race_count_df = utils.bq.query(q)
render.dataframe(multi_race_count_df)

render.md('## In dataset `{DEID}`'.format(DEID=DEID))

# Generalization during the privacy methodology should limit the populations represented in the deidentified dataset to those who selected 1 or 2 races only. Where 2 races are selected, Hispanic must be one of them.

q = MULTIRACIAL_DIST_QUERY.format(DATASET=DEID, VOCAB=VOCAB)
multi_race_count_df = utils.bq.query(q)
render.dataframe(multi_race_count_df)
