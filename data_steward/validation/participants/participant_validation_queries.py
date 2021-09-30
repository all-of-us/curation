# Project imports
from common import JINJA_ENV

CREATE_SEX_COMPARISON_FUNCTION = JINJA_ENV.from_string("""
CREATE FUNCTION IF NOT EXISTS
  `{{project_id}}.{{drc_dataset_id}}.CompareSexAtBirth`(rdr_sex string,
    ehr_sex string)
  RETURNS string AS ((
    SELECT
      CASE
        {{gender_case_when_conditions}}
      ELSE
      '{{missing_ehr}}'
    END
      AS sex;
""")

CREATE_EMAIL_COMPARISON_FUNCTION = JINJA_ENV.from_string("""
    CREATE FUNCTION IF NOT EXISTS `{{project_id}}.{{drc_dataset_id}}.CompareEmail`(rdr_email string, ehr_email string)
    RETURNS string
    AS ((
        WITH normalized_rdr_email AS (
            SELECT LOWER(TRIM(rdr_email)) AS rdr_email
        )
        , normalized_ehr_email AS (
            SELECT LOWER(TRIM(ehr_email)) AS ehr_email
        )
        SELECT
            CASE 
                WHEN normalized_rdr_email.rdr_email = normalized_ehr_email.ehr_email
                    AND REGEXP_CONTAINS(normalized_rdr_email.rdr_email, '@') THEN '{{match}}'
                WHEN normalized_rdr_email.rdr_email IS NOT NULL AND normalized_ehr_email.ehr_email IS NOT NULL THEN '{{no_match}}'
                WHEN normalized_rdr_email.rdr_email IS NULL THEN '{{missing_rdr}}'
                ELSE '{{missing_ehr}}'
            END AS email
        FROM normalized_rdr_email, normalized_ehr_email 

    ));
""")

CREATE_PHONE_NUMBER_COMPARISON_FUNCTION = JINJA_ENV.from_string("""
CREATE FUNCTION IF NOT EXISTS
  `{{project_id}}.{{drc_dataset_id}}.ComparePhoneNumber`(rdr_phone_number string,
    ehr_phone_number string)
  RETURNS string AS ((
    WITH
      normalized_rdr_phone_number AS (
      SELECT
        REPLACE(REPLACE(REPLACE(REPLACE(rdr_phone_number,'-',''),'+', ''),'(',''),')','') AS rdr_phone_number ),
      normalized_ehr_phone_number AS (
      SELECT
        REPLACE(REPLACE(REPLACE(REPLACE(ehr_phone_number,'-',''),'+', ''),'(',''),')','') AS ehr_phone_number )
    SELECT
      CASE
        WHEN normalized_rdr_phone_number.rdr_phone_number = normalized_ehr_phone_number.ehr_phone_number THEN '{{match}}'
        WHEN normalized_rdr_phone_number.rdr_phone_number IS NOT NULL
      AND normalized_ehr_phone_number.ehr_phone_number IS NOT NULL THEN '{{no_match}}'
        WHEN normalized_rdr_phone_number.rdr_phone_number IS NULL THEN '{{missing_rdr}}'
      ELSE
      '{{missing_ehr}}'
    END
      AS phone_number
    FROM
      normalized_rdr_phone_number,
      normalized_ehr_phone_number ));
""")

# Contains list of create function queries to execute
CREATE_COMPARISON_FUNCTION_QUERIES = [{
    'name': 'CompareEmail',
    'query': CREATE_EMAIL_COMPARISON_FUNCTION
}, {
    'name': 'ComparePhoneNumber',
    'query': CREATE_PHONE_NUMBER_COMPARISON_FUNCTION
}, {
    'name': 'CompareSexAtBirth',
    'query': CREATE_SEX_COMPARISON_FUNCTION
}]
