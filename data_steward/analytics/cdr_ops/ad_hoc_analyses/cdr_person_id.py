# # Person
# ## Person ID validation

import utils.bq
from notebooks.parameters import RDR_DATASET_ID, EHR_DATASET_ID

# Report sites where the number of invalid / total participant IDs
# exceeds this threshold and provide diagnostics
INVALID_THRESHOLD = 0.5

# Get list of all hpo_ids
hpo_ids = utils.bq.query("""
SELECT REPLACE(table_id, '_person', '') AS hpo_id
FROM `{EHR_DATASET_ID}.__TABLES__`
WHERE table_id LIKE '%person'
AND table_id NOT LIKE '%unioned_ehr_%' AND table_id NOT LIKE '\\\_%'
""".format(EHR_DATASET_ID=EHR_DATASET_ID)).hpo_id.tolist()

# For each site submission, how many person_ids cannot be found in the latest RDR dump (*not_in_rdr*) or are not valid 9-digit participant identifiers (_invalid_).

subqueries = []
subquery = """
SELECT
 '{h}' AS hpo_id,
 not_in_rdr.n AS not_in_rdr,
 invalid.n AS invalid,
 CAST(T.row_count AS INT64) AS total
FROM {EHR_DATASET_ID}.__TABLES__ T
LEFT JOIN
(SELECT COUNT(1) AS n
 FROM {EHR_DATASET_ID}.{h}_person e
 WHERE NOT EXISTS(
  SELECT 1
  FROM {RDR_DATASET_ID}.person r
  WHERE r.person_id = e.person_id)) not_in_rdr
 ON TRUE
LEFT JOIN
(SELECT COUNT(1) AS n
 FROM {EHR_DATASET_ID}.{h}_person e
 WHERE NOT person_id BETWEEN 100000000 AND 999999999) invalid
 ON TRUE
WHERE T.table_id = '{h}_person'"""
for hpo_id in hpo_ids:
    subqueries.append(
        subquery.format(h=hpo_id,
                        EHR_DATASET_ID=EHR_DATASET_ID,
                        RDR_DATASET_ID=RDR_DATASET_ID))
q = '\n\nUNION ALL\n'.join(subqueries)
df = utils.bq.query(q)
df

# ## HPO sites where proportion of invalid person_ids exceeds threshold

df['invalid_ratio'] = df.not_in_rdr / df.total
above_threshold = df.invalid_ratio > INVALID_THRESHOLD
hpos_above_threshold = df[above_threshold]['hpo_id'].values.tolist()
hpos_above_threshold

# ## Compare names and person_ids in RDR vs pii_name vs person

# +
RDR_EHR_NAME_MATCH_QUERY = '''
WITH
  rdr_first_name AS
  (SELECT DISTINCT person_id,
   FIRST_VALUE(value_as_string)
     OVER (PARTITION BY person_id, observation_source_value ORDER BY value_as_string) val
  FROM {RDR_DATASET_ID}.observation
  WHERE observation_source_value = 'PIIName_First'),

  rdr_last_name AS
  (SELECT DISTINCT person_id,
   FIRST_VALUE(value_as_string)
     OVER (PARTITION BY person_id, observation_source_value ORDER BY value_as_string) val
  FROM {RDR_DATASET_ID}.observation
  WHERE observation_source_value = 'PIIName_Last'),

  rdr_name AS
  (SELECT
     f.person_id person_id,
     f.val       first_name,
     l.val       last_name
   FROM rdr_first_name f JOIN rdr_last_name l USING (person_id))

 SELECT
   '{HPO_ID}'                 hpo_id,
   rdr.person_id              rdr_person_id,
   rdr.first_name             rdr_first_name,
   rdr.last_name              rdr_last_name,
   pii.person_id              pii_person_id,
   pii.first_name             pii_first_name,
   pii.middle_name            pii_middle_name,
   pii.last_name              pii_last_name,
   p.person_id                person_person_id,
   p.person_source_value      person_person_source_value
 FROM rdr_name rdr
 JOIN `{EHR_DATASET_ID}.{HPO_ID}_pii_name` pii
   ON  pii.first_name = rdr.first_name
   AND pii.last_name  = rdr.last_name
 LEFT JOIN `{EHR_DATASET_ID}.{HPO_ID}_person` p
   ON pii.person_id = p.person_id
'''


def get_rdr_ehr_name_match_hpo_query(hpo_id):
    return RDR_EHR_NAME_MATCH_QUERY.format(RDR_DATASET_ID=RDR_DATASET_ID,
                                           EHR_DATASET_ID=EHR_DATASET_ID,
                                           HPO_ID=hpo_id)


def get_rdr_ehr_name_match_query(hpo_ids):
    subqueries = []
    for hpo_id in hpo_ids:
        subquery = get_rdr_ehr_name_match_hpo_query(hpo_id)
        subqueries.append(subquery)
    return '\n\nUNION ALL\n\n'.join(
        map(lambda subquery: '(' + subquery + ')', subqueries))


def rdr_ehr_name_match(hpo_ids):
    q = get_rdr_ehr_name_match_query(hpo_ids)
    return utils.bq.query(q)


# -

rdr_ehr_name_match(hpos_above_threshold)
