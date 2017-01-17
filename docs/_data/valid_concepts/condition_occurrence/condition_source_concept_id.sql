SELECT concept_id
FROM concept AS C
WHERE C.concept_code = '592.0'
  AND C.vocabulary_id IN ('ICD9CM', 'ICD10CM')
  AND C.invalid_reason IS NULL
  AND C.domain_id = 'Condition'