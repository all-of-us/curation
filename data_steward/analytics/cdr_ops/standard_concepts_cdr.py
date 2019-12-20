# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.4'
#       jupytext_version: 1.2.3
#   kernelspec:
#     display_name: Python 2
#     language: python
#     name: python2
# ---

# ## Notebook allows us to see that the concept table in the CDR query only contains standard concepts:
# - Relevant JIRA ticket: '[DC-389](https://precisionmedicineinitiative.atlassian.net/browse/DC-389)'.

from notebooks import bq, render, parameters

# +
ref = parameters.LATEST_DATASET
combined = parameters.COMBINED_DATASET_ID

print(
"""
Reference Dataset: {}
Combined Dataset: {}
""".format(ref, combined))

# +
query = """
SELECT
combined_c.concept_id as combined_id, ref_c.concept_id as reference_id,
combined_c.concept_name, 
combined_c.standard_concept as combined_standard, ref_c.standard_concept as reference_standard,
LOWER(combined_c.domain_id) LIKE LOWER(ref_c.domain_id) as same_domain

FROM
`{}.concept` combined_c
LEFT JOIN
`{}.concept` ref_c
ON
combined_c.concept_id = ref_c.concept_id

WHERE  -- see where there is a non-standard concept

(ref_c.standard_concept NOT IN ('S')
AND
combined_c.concept_id <> 0
AND
combined_c.concept_id IS NOT NULL)

OR

(
ref_c.domain_id <> combined_c.domain_id
)

GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY same_domain ASC
""".format(combined, ref)

results = bq.query(query)
# -

results


