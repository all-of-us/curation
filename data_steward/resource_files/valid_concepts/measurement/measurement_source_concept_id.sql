SELECT concept_id
FROM concept AS c
WHERE c.concept_code = '2951-2'
  AND c.invalid_reason IS NULL
  AND c.domain_id = 'Measurement'