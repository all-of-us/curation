SELECT concept_id
FROM concept AS c
WHERE c.vocabulary_id = 'UCUM'
  AND c.standard_concept = 'S'
  AND c.domain_id = 'Unit'