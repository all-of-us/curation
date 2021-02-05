SELECT concept_id
FROM concept AS c
WHERE c.concept_code = '311670'
  AND c.invalid_reason IS NULL
  AND c.domain_id = 'Drug'