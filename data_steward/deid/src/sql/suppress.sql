--
--
-- This file implements suppression rules against OMOP database
-- NOTE:
--	The OMOP database is a meta-database i.e it stores information about data and data identically.
--	This is a typical design that offers great flexibility (it's a post NoSQL-era design)
--


--
-- In order to perform suppression against a meta-table we must answer the following:
--
--	1. What are we suppressing by specifying vocabulary_id (PPI). These fields can/should be specified in [rules.ppi]
--	
--	2. The group of things to suppress must be equally specified (Question, Answer, PPI Modifier). 
--	This is because we do NOT want to accidentally remove meta data. This step matters for logs

--	3. Join a target table against field_code|concept_code
--		observations.observation_source_value
--		measurement.measurement_source_value
--
--
select concept_code,vocabulary_id,concept_class_id 
from [rules.ppi] spec inner join [raw.concept] ref on ref.concept_code = spec.field_code


--
-- This is the suppression of PPI in the observation table, 
--
SELECT observation_source_value,value_as_string FROM [raw.observation] 
where observation_source_value not in (select field_code from [rules.ppi]) and person_id = 562270
--
-- @TODO: We need to suppress certain ICD9, ICD10
-- suppression of person, will just require fields to be 
