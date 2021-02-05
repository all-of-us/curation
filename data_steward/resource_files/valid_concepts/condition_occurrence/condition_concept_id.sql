SELECT c2.concept_id
FROM concept c1
  JOIN concept_relationship cr ON c1.concept_id = cr.concept_id_1
                                  AND cr.relationship_id = 'Maps to'
  JOIN concept c2 ON c2.concept_id = cr.concept_id_2
WHERE c1.concept_id = 'CONDITION_SOURCE_CONCEPT_ID'
      AND c2.standard_concept = 'S'
      AND c2.invalid_reason IS NULL
      AND c2.domain_id = 'Condition'