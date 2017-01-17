# Valid Concepts

These resources identify the concepts permitted in CDM fields that reference a concept.
 
## Small domains
The values allowed for `person.ethnicity_concept_id` are listed in `person/ethnicity_concept_id`.

## Large domains

### condition_occurrence
condition_concept_id

    SELECT concept_id
    FROM concept 
    WHERE domain_id = 'Condition'

