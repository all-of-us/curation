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

from notebooks import bq, render, parameters

# +
ref = parameters.UNIONED_EHR_DATASET_COMBINED
combined = parameters.COMBINED_DATASET_ID

print(
"""
Reference Dataset: {ref}
Combined Dataset: {combined}
""".format(ref = ref, combined = combined))
# -

# ### The below query is used to determine rows where the condition_occurrence_concept_id in the unioned EHR dataset is NOT standard. With this information, we then look at the condition_occurrence_concept_id in the combined dataset 

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

-- checking instances where the unioned_ehr is non standard
WHERE
c1.standard_concept NOT IN ('S')

GROUP BY 1, 2, 3, 4, 5, 6, 7
ORDER BY count DESC
""".format(combined = combined, ref = ref)

co_results = bq.query(co_query)
# -

co_results

# #### Check instances where the final concept is not standard

co_cleaning_rule_failure = co_results.loc[(co_results['post_cr_standard_concept'] != 'S')]

# #### Check instances where the final concept is not of the correct domain idea

co_no_domain_enforcement = co_results.loc[~co_results['post_cr_domain_correct']]

co_no_domain_enforcement

# +
total_co_failures = co_cleaning_rule_failure['count'].sum()

print("There is/are {num} instances where a non-standard condition occurrence concept ID was not \n"
      "mapped to a standard condition concept ID.".format(num = total_co_failures))

# +
total_co_domain_failures = co_no_domain_enforcement['count'].sum()

print("There is/are {num} instances where a condition concept ID from the combined dataset was not of \n"
      "the 'Condition' domain.".format(num = total_co_domain_failures))
# -

# ### The below query is used to determine rows where the drug_concept_id in the unioned EHR dataset is NOT standard. With this information, we then look at the drug_concept_id in the combined dataset 

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

-- checking instances where the unioned_ehr is non standard
WHERE
c1.standard_concept NOT IN ('S')

GROUP BY 1, 2, 3, 4, 5, 6, 7
ORDER BY count DESC
""".format(combined = combined, ref = ref)

de_results = bq.query(de_query)
# -

de_results

# #### Check instances where the final concept is not standard

de_cleaning_rule_failure = de_results.loc[(de_results['post_cr_standard_concept'] != 'S')]

de_cleaning_rule_failure

# #### Check instances where the final concept is not of the correct domain idea

co_no_domain_enforcement = co_results.loc[~co_results['post_cr_domain_correct']]

co_no_domain_enforcement

# +
total_de_failures = de_cleaning_rule_failure['count'].sum()

print("There is/are {num} instances where a non-standard drug exopsure concept ID was not \n"
      "mapped to a standard condition concept ID.".format(num = total_de_failures))

# +
total_de_domain_failures = co_no_domain_enforcement['count'].sum()

print("There is/are {num} instances where a drug concept ID from the combined dataset was not of \n"
      "the 'Drug' domain.".format(num = total_de_domain_failures))
