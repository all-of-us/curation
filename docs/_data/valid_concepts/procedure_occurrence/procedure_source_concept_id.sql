SELECT concept_id
FROM concept AS c
WHERE c.concept_code = '98.51'
  AND c.invalid_reason IS NULL
  AND c.domain_id = 'Procedure'