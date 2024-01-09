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

# ## Notebook allows us to see that the concept table in the CDR query only contains standard concepts:
# - Relevant JIRA ticket: '[DC-389](https://precisionmedicineinitiative.atlassian.net/browse/DC-389)'.
#
# ##### DC-389:
# Checks the concept table in the CDR to ensure that each of the concept IDs:
# - matches a concept_ID in the concept table
# - all of the standard_concept fields are of type 'S'
# - domain_id of the concept matches the domain_id
#
# Concept IDs should be replaced if it does not meet all of the three criteria are not met.

# ## NOTE: This notebook currently looks to see if there are ANY instances in which the ultimate 'concept_id' for a table in the combined data set is either non-standard or in the incorrect domain. This includes instances where:
#
# - The original concept was non-standard
# - The original concept was standard and subsequently converted to a nonstandard concept
#
# #### This notebook checks where non-standard concepts failed to map to standard concepts using c1.standard_concept NOT IN ('S', 'C')'.
#
#
# #### This notebook also does not exclude instances where the concept_id = 0.
import bq_utils
import utils.bq
from notebooks import parameters

# +
ref = parameters.UNIONED_EHR_DATASET_COMBINED
combined = parameters.COMBINED_DATASET_ID

print("""
Reference Dataset: {ref}
Combined Dataset: {combined}
""".format(ref=ref, combined=combined))
# -

# ### The below query is used to determine condition_concept_id in the combined dataset to see if it is both standard and of the domain 'condition'

# +
co_query = """
SELECT
DISTINCT
co.condition_concept_id as pre_cr_concept_id, c1.standard_concept as pre_cr_standard_concept, c1.concept_name as pre_cr_cn,
co_combined.condition_concept_id as post_cr_concept_id, c2.standard_concept as post_cr_standard_concept, c2.concept_name as post_cr_cn,
(LOWER(c2.domain_id) LIKE '%condition%') as post_cr_domain_correct,
COUNT(*) as count, COUNT(DISTINCT mco.src_hpo_id) as num_sites_w_change

-- linking the 'pre-cr' to the 'post-cr counterpart
FROM
`{ref}.condition_occurrence` co
JOIN
`{combined}.condition_occurrence` co_combined
ON
co.condition_occurrence_id = co_combined.condition_occurrence_id

-- to determine how many sites are affected by the change
JOIN
`{ref}._mapping_condition_occurrence` mco
ON
co.condition_occurrence_id = mco.condition_occurrence_id

-- trying to figure out the status of the 'pre-cr' concept ID
JOIN
`{ref}.concept` c1
ON
co.condition_concept_id = c1.concept_id

-- trying to figure out the status of the 'post-CR' concept ID
JOIN
`{combined}.concept` c2
ON
co_combined.condition_concept_id = c2.concept_id

GROUP BY 1, 2, 3, 4, 5, 6, 7
ORDER BY count DESC
""".format(combined=combined, ref=ref)

co_results = utils.bq.query(co_query)
# -

co_results

# #### Check instances where the final concept is not standard

co_cleaning_rule_failure = co_results.loc[(
    co_results['post_cr_standard_concept'] not in ('S', 'C'))]

co_cleaning_rule_failure

# #### Check instances where the final concept is not of the correct domain idea

co_no_domain_enforcement = co_results.loc[~co_results['post_cr_domain_correct']]

co_no_domain_enforcement

# +
total_co_failures = co_cleaning_rule_failure['count'].sum()

print(
    "There is/are {num} instances where a condition occurrence concept ID was not \n"
    "mapped to a standard concept ID.".format(num=total_co_failures))

# +
total_co_domain_failures = co_no_domain_enforcement['count'].sum()

print(
    "There is/are {num} instances where a condition concept ID from the combined dataset was not of \n"
    "the 'Condition' domain.".format(num=total_co_domain_failures))
# -

# ### The below query is used to determine  drug_concept_id in the combined dataset to see if it is both standard and of the domain 'drug'

# +
de_query = """
SELECT
DISTINCT
de.drug_concept_id as pre_cr_concept_id, c1.standard_concept as pre_cr_standard_concept, c1.concept_name as pre_cr_cn,
de_combined.drug_concept_id as post_cr_concept_id, c2.standard_concept as post_cr_standard_concept, c2.concept_name as post_cr_cn,
(LOWER(c2.domain_id) LIKE '%drug%') as post_cr_domain_correct,
COUNT(*) as count, COUNT(DISTINCT mde.src_hpo_id) as num_sites_w_change

-- linking the 'pre-cr' to the 'post-cr counterpart
FROM
`{ref}.drug_exposure` de
JOIN
`{combined}.drug_exposure` de_combined
ON
de.drug_exposure_id = de_combined.drug_exposure_id

-- to determine how many sites are affected by the change
JOIN
`{ref}._mapping_drug_exposure` mde
ON
de.drug_exposure_id = mde.drug_exposure_id

-- trying to figure out the status of the 'pre-cr' concept ID
JOIN
`{ref}.concept` c1
ON
de.drug_concept_id = c1.concept_id

-- trying to figure out the status of the 'post-CR' concept ID
JOIN
`{combined}.concept` c2
ON
de_combined.drug_concept_id = c2.concept_id

GROUP BY 1, 2, 3, 4, 5, 6, 7
ORDER BY count DESC
""".format(combined=combined, ref=ref)

de_results = utils.bq.query(de_query)
# -

print(de_query)

de_results

# #### Check instances where the final concept is not standard

de_cleaning_rule_failure = de_results.loc[(
    de_results['post_cr_standard_concept'] not in ('S', 'C'))]

de_cleaning_rule_failure

# #### Check instances where the final concept is not of the correct domain idea

co_no_domain_enforcement = co_results.loc[~co_results['post_cr_domain_correct']]

co_no_domain_enforcement

# +
total_de_failures = de_cleaning_rule_failure['count'].sum()

print("There is/are {num} instances where a drug exposure concept ID was not \n"
      "mapped to a standard concept ID.".format(num=total_de_failures))

# +
total_de_domain_failures = co_no_domain_enforcement['count'].sum()

print(
    "There is/are {num} instances where a drug concept ID from the combined dataset was not of \n"
    "the 'Drug' domain.".format(num=total_de_domain_failures))
# -

# ### The below query is used to determine  measurement_concept_id in the combined dataset to see if it is both standard and of the domain 'measurement'

# +
m_query = """
SELECT
DISTINCT
m.measurement_concept_id as pre_cr_concept_id, c1.standard_concept as pre_cr_standard_concept, c1.concept_name as pre_cr_cn,
m_combined.measurement_concept_id as post_cr_concept_id, c2.standard_concept as post_cr_standard_concept, c2.concept_name as post_cr_cn,
(LOWER(c2.domain_id) LIKE '%measurement%') as post_cr_domain_correct,
COUNT(*) as count, COUNT(DISTINCT mm.src_hpo_id) as num_sites_w_change

-- linking the 'pre-cr' to the 'post-cr counterpart
FROM
`{ref}.measurement` m
JOIN
`{combined}.measurement` m_combined
ON
m.measurement_id = m_combined.measurement_id

-- to determine how many sites are affected by the change
JOIN
`{ref}._mapping_measurement` mm
ON
m.measurement_id = mm.measurement_id

-- trying to figure out the status of the 'pre-cr' concept ID
JOIN
`{ref}.concept` c1
ON
m.measurement_concept_id = c1.concept_id

-- trying to figure out the status of the 'post-CR' concept ID
JOIN
`{combined}.concept` c2
ON
m_combined.measurement_concept_id = c2.concept_id


GROUP BY 1, 2, 3, 4, 5, 6, 7
ORDER BY count DESC
""".format(combined=combined, ref=ref)

m_results = utils.bq.query(m_query)
# -

m_results

m_cleaning_rule_failure = m_results.loc[(m_results['post_cr_standard_concept']
                                         not in ('S', 'C'))]

m_cleaning_rule_failure

m_no_domain_enforcement = m_results.loc[~m_results['post_cr_domain_correct']]

m_no_domain_enforcement

# +
total_m_failures = m_cleaning_rule_failure['count'].sum()

print("There is/are {num} instances where a measurement concept ID was not \n"
      "mapped to a standard concept ID.".format(num=total_m_failures))

# +
total_m_domain_failures = m_no_domain_enforcement['count'].sum()

print(
    "There is/are {num} instances where a measurement concept ID from the combined dataset was not of \n"
    "the 'Measurement' domain.".format(num=total_m_domain_failures))
# -

# ### The below query is used to determine visit_concept_id in the combined dataset to see if it is both standard and of the domain 'visit'

# +
v_query = """
SELECT
DISTINCT
v.visit_concept_id as pre_cr_concept_id, c1.standard_concept as pre_cr_standard_concept, c1.concept_name as pre_cr_cn,
v_combined.visit_concept_id as post_cr_concept_id, c2.standard_concept as post_cr_standard_concept, c2.concept_name as post_cr_cn,
(LOWER(c2.domain_id) LIKE '%visit%') as post_cr_domain_correct,
COUNT(*) as count, COUNT(DISTINCT mv.src_hpo_id) as num_sites_w_change

-- linking the 'pre-cr' to the 'post-cr counterpart
FROM
`{ref}.visit_occurrence` v
JOIN
`{combined}.visit_occurrence` v_combined
ON
v.visit_occurrence_id = v_combined.visit_occurrence_id

-- to determine how many sites are affected by the change
JOIN
`{ref}._mapping_visit_occurrence` mv
ON
v.visit_occurrence_id = mv.visit_occurrence_id

-- trying to figure out the status of the 'pre-cr' concept ID
JOIN
`{ref}.concept` c1
ON
v.visit_concept_id = c1.concept_id

-- trying to figure out the status of the 'post-CR' concept ID
JOIN
`{combined}.concept` c2
ON
v_combined.visit_concept_id = c2.concept_id


GROUP BY 1, 2, 3, 4, 5, 6, 7
ORDER BY count DESC
""".format(combined=combined, ref=ref)

v_results = utils.bq.query(v_query)
# -

v_results

v_cleaning_rule_failure = v_results.loc[(v_results['post_cr_standard_concept']
                                         not in ('S', 'C')]

v_cleaning_rule_failure

v_no_domain_enforcement = v_results.loc[~v_results['post_cr_domain_correct']]

v_no_domain_enforcement

# +
total_v_failures = v_cleaning_rule_failure['count'].sum()

print("There is/are {num} instances where a visit concept ID was not \n"
      "mapped to a standard concept ID.".format(num=total_v_failures))

# +
total_v_domain_failures = v_no_domain_enforcement['count'].sum()

print(
    "There is/are {num} instances where a visit concept ID from the combined dataset was not of \n"
    "the 'Visit' domain.".format(num=total_v_domain_failures))
# -

# ### The below query is used to determine procedure_concept_id in the combined dataset to see if it is both standard and of the domain 'Procedure'

# +
p_query = """
SELECT
DISTINCT
p.procedure_concept_id as pre_cr_concept_id, c1.standard_concept as pre_cr_standard_concept, c1.concept_name as pre_cr_cn,
p_combined.procedure_concept_id as post_cr_concept_id, c2.standard_concept as post_cr_standard_concept, c2.concept_name as post_cr_cn,
(LOWER(c2.domain_id) LIKE '%procedure%') as post_cr_domain_correct,
COUNT(*) as count, COUNT(DISTINCT mp.src_hpo_id) as num_sites_w_change

-- linking the 'pre-cr' to the 'post-cr counterpart
FROM
`{ref}.procedure_occurrence` p
JOIN
`{combined}.procedure_occurrence` p_combined
ON
p.procedure_occurrence_id = p_combined.procedure_occurrence_id

-- to determine how many sites are affected by the change
JOIN
`{ref}._mapping_procedure_occurrence` mp
ON
p.procedure_occurrence_id = mp.procedure_occurrence_id

-- trying to figure out the status of the 'pre-cr' concept ID
JOIN
`{ref}.concept` c1
ON
p.procedure_concept_id = c1.concept_id

-- trying to figure out the status of the 'post-CR' concept ID
JOIN
`{combined}.concept` c2
ON
p_combined.procedure_concept_id = c2.concept_id

GROUP BY 1, 2, 3, 4, 5, 6, 7
ORDER BY count DESC
""".format(combined=combined, ref=ref)

p_results = utils.bq.query(p_query)
# -

p_results

p_cleaning_rule_failure = p_results.loc[(p_results['post_cr_standard_concept']
                                         != 'S')]

p_cleaning_rule_failure

p_no_domain_enforcement = p_results.loc[~p_results['post_cr_domain_correct']]

p_no_domain_enforcement

# +
total_p_failures = p_cleaning_rule_failure['count'].sum()

print("There is/are {num} instances where a procedure concept ID was not \n"
      "mapped to a standard concept ID.".format(num=total_p_failures))

# +
total_p_domain_failures = p_no_domain_enforcement['count'].sum()

print(
    "There is/are {num} instances where a procedure concept ID from the combined dataset was not of \n"
    "the 'Procedure' domain.".format(num=total_p_domain_failures))
# -

# ### The below query is used to determine observation_concept_id in the combined dataset to see if it is both standard and of the domain 'Observation'

# +
o_query = """
SELECT
DISTINCT
o.observation_concept_id as pre_cr_concept_id, c1.standard_concept as pre_cr_standard_concept, c1.concept_name as pre_cr_cn,
o_combined.observation_concept_id as post_cr_concept_id, c2.standard_concept as post_cr_standard_concept, c2.concept_name as post_cr_cn,
(LOWER(c2.domain_id) LIKE '%observation%') as post_cr_domain_correct,
COUNT(*) as count, COUNT(DISTINCT mo.src_hpo_id) as num_sites_w_change

-- linking the 'pre-cr' to the 'post-cr counterpart
FROM
`{ref}.observation` o
JOIN
`{combined}.observation` o_combined
ON
o.observation_id = o_combined.observation_id

-- to determine how many sites are affected by the change
JOIN
`{ref}._mapping_observation` mo
ON
o.observation_id = mo.observation_id

-- trying to figure out the status of the 'pre-cr' concept ID
JOIN
`{ref}.concept` c1
ON
o.observation_concept_id = c1.concept_id

-- trying to figure out the status of the 'post-CR' concept ID
JOIN
`{combined}.concept` c2
ON
o_combined.observation_concept_id = c2.concept_id

GROUP BY 1, 2, 3, 4, 5, 6, 7
ORDER BY count DESC
""".format(combined=combined, ref=ref)

o_results = utils.bq.query(o_query)
# -

o_results

o_cleaning_rule_failure = o_results.loc[(o_results['post_cr_standard_concept']
                                         != 'S')]

o_cleaning_rule_failure

o_no_domain_enforcement = o_results.loc[~o_results['post_cr_domain_correct']]

o_no_domain_enforcement

# +
total_o_failures = o_cleaning_rule_failure['count'].sum()

print("There is/are {num} instances where a observation concept ID was not \n"
      "mapped to a standard concept ID.".format(num=total_o_failures))

# +
total_o_domain_failures = o_no_domain_enforcement['count'].sum()

print(
    "There is/are {num} instances where a visit concept ID from the combined dataset was not of \n"
    "the 'Observation' domain.".format(num=total_o_domain_failures))
