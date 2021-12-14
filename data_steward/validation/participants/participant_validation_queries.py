# Project imports
from common import JINJA_ENV
"""
Compares rdr and ehr name fields (first_name, middle_name, last_name) and returns one of
'match', 'no_match', 'missing_rdr', or 'missing_ehr'.

Before comparison, the following string normalizations are performed on the input strings:
  1. Strip leading and trailing whitespace
  2. Remove any character that is not an alphabetic character
  3. Lower case all characters

A current limitation of this function is its inability to consider special characters with accents
and tildes as matches. Additionally, there is currently no capability for fuzzy matching or matching
phonetically similar strings.
"""
CREATE_NAME_COMPARISON_FUNCTION = JINJA_ENV.from_string("""
    CREATE FUNCTION IF NOT EXISTS `{{project_id}}.{{drc_dataset_id}}.CompareName`(rdr_name string, ehr_name string)
    RETURNS string
    AS ((
        WITH normalized_rdr_name AS (
            SELECT REGEXP_REPLACE(LOWER(TRIM(rdr_name)), '[^A-Za-z]', '') AS rdr_name
        )
        , normalized_ehr_name AS (
            SELECT REGEXP_REPLACE(LOWER(TRIM(ehr_name)), '[^A-Za-z]', '') AS ehr_name
        )
        SELECT
            CASE 
                WHEN normalized_rdr_name.rdr_name = normalized_ehr_name.ehr_name THEN '{{match}}'
                WHEN normalized_rdr_name.rdr_name IS NOT NULL AND normalized_ehr_name.ehr_name IS NOT NULL THEN '{{no_match}}'
                WHEN normalized_rdr_name.rdr_name IS NULL THEN '{{missing_rdr}}'
                ELSE '{{missing_ehr}}'
            END AS name
        FROM normalized_rdr_name, normalized_ehr_name 

    ));
""")

CREATE_STREET_ADDRESS_COMPARISON_FUNCTION = JINJA_ENV.from_string("""
CREATE FUNCTION IF NOT EXISTS
  `{{project_id}}.{{drc_dataset_id}}.CompareCity`(rdr_street_address string, ehr_street_address string)
  RETURNS string AS ((
        WITH normalized_rdr_street_address AS (
            SELECT LOWER(TRIM(rdr_xyz)) AS rdr_street_address
        )
        , normalized_ehr_street_address AS (
            SELECT LOWER(TRIM(ehr_xyz)) AS ehr_street_address
        )
    SELECT
      CASE
        WHEN normalized_rdr_street_address.rdr_street_address = normalized_ehr_street_address.ehr_street_address THEN '{{match}}'
        WHEN normalized_rdr_street_address.rdr_street_address IS NOT NULL AND normalized_ehr_street_address.ehr_street_address IS NOT NULL THEN '{{no_match}}'
        WHEN normalized_rdr_street_address.rdr_street_address IS NULL THEN '{{missing_rdr}}'
        ELSE '{{missing_ehr}}'
      END AS street_address
    FROM normalized_rdr_street_address, normalized_ehr_street_address));
""")

CREATE_CITY_COMPARISON_FUNCTION = JINJA_ENV.from_string("""
CREATE FUNCTION IF NOT EXISTS
  `{{project_id}}.{{drc_dataset_id}}.CompareCity`(rdr_city string, ehr_city string)
  RETURNS string AS ((
        WITH normalized_rdr_city AS (
            SELECT LOWER(TRIM(rdr_xyz)) AS rdr_city
        )
        , normalized_ehr_city AS (
            SELECT LOWER(TRIM(ehr_xyz)) AS ehr_city
        )
    SELECT
      CASE
        WHEN normalized_rdr_city.rdr_city = normalized_ehr_city.ehr_city THEN '{{match}}'
        WHEN normalized_rdr_city.rdr_city IS NOT NULL AND normalized_ehr_city.ehr_city IS NOT NULL THEN '{{no_match}}'
        WHEN normalized_rdr_city.rdr_city IS NULL THEN '{{missing_rdr}}'
        ELSE '{{missing_ehr}}'
      END AS city
    FROM normalized_rdr_city, normalized_ehr_city));
""")

CREATE_STATE_COMPARISON_FUNCTION = JINJA_ENV.from_string("""
CREATE FUNCTION IF NOT EXISTS
  `{{project_id}}.{{drc_dataset_id}}.CompareState`(rdr_state string, ehr_state string)
  RETURNS string AS ((
        WITH normalized_rdr_state AS (
            SELECT LOWER(TRIM(rdr_xyz)) AS rdr_state
        )
        , normalized_ehr_state AS (
            SELECT LOWER(TRIM(ehr_xyz)) AS ehr_state
        )
    SELECT
      CASE
        WHEN normalized_rdr_state.rdr_state = normalized_ehr_state.ehr_state THEN '{{match}}'
        WHEN normalized_rdr_state.rdr_state IS NOT NULL AND normalized_ehr_state.ehr_state IS NOT NULL THEN '{{no_match}}'
        WHEN normalized_rdr_state.rdr_state IS NULL THEN '{{missing_rdr}}'
        ELSE '{{missing_ehr}}'
      END AS state
    FROM normalized_rdr_state, normalized_ehr_state));
""")

CREATE_ZIP_CODE_COMPARISON_FUNCTION = JINJA_ENV.from_string("""
CREATE FUNCTION IF NOT EXISTS
  `{{project_id}}.{{drc_dataset_id}}.CompareZipCode`(rdr_zip_code string, ehr_zip_code string)
  RETURNS string AS ((
        WITH normalized_rdr_zip_code AS (
            SELECT LPAD(SPLIT(SPLIT(rdr_zip_code, '-')[OFFSET(0)], ' ')[OFFSET(0)], 5, '0') AS rdr_zip_code
        )
        , normalized_ehr_zip_code AS (
            SELECT LPAD(SPLIT(SPLIT(ehr_zip_code, '-')[OFFSET(0)], ' ')[OFFSET(0)], 5, '0') AS ehr_zip_code
        )
    SELECT
      CASE
        WHEN normalized_rdr_zip_code.rdr_zip_code = normalized_ehr_zip_code.ehr_zip_code THEN '{{match}}'
        WHEN normalized_rdr_zip_code.rdr_zip_code IS NOT NULL AND normalized_ehr_zip_code.ehr_zip_code IS NOT NULL THEN '{{no_match}}'
        WHEN normalized_rdr_zip_code.rdr_zip_code IS NULL THEN '{{missing_rdr}}'
        ELSE '{{missing_ehr}}'
      END AS zip_code
    FROM normalized_rdr_zip_code, normalized_ehr_zip_code));
""")

CREATE_SEX_COMPARISON_FUNCTION = JINJA_ENV.from_string("""
CREATE FUNCTION IF NOT EXISTS
  `{{project_id}}.{{drc_dataset_id}}.CompareSexAtBirth`(rdr_sex string,
    ehr_sex string)
  RETURNS string AS ((
        WITH normalized_rdr_sex AS (
            SELECT LOWER(TRIM(rdr_sex)) AS rdr_sex
        )
        , normalized_ehr_sex AS (
            SELECT LOWER(TRIM(ehr_sex)) AS ehr_sex
        )
    SELECT
      CASE
        {{gender_case_when_conditions}}
      ELSE
      '{{missing_ehr}}'
    END
      AS sex
       FROM normalized_rdr_sex, normalized_ehr_sex));
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

CREATE_DOB_COMPARISON_FUNCTION = JINJA_ENV.from_string("""
 CREATE FUNCTION IF NOT EXISTS 
 `{{project_id}}.{{drc_dataset_id}}.CompareDateOfBirth`(rdr_birth_date date, ehr_birth_date date)
    RETURNS string AS
    (( 
        SELECT
        CASE 
            WHEN rdr_birth_date = ehr_birth_date THEN 'match'
            WHEN rdr_birth_date IS NOT NULL AND ehr_birth_date IS NOT NULL THEN 'no_match'
            WHEN rdr_birth_date IS NULL THEN 'missing_rdr'
            ELSE 'missing_ehr'
        END AS birth_date
    ));
""")

# Contains list of create function queries to execute
CREATE_COMPARISON_FUNCTION_QUERIES = [{
    'name': 'CompareName',
    'query': CREATE_NAME_COMPARISON_FUNCTION
}, {
    'name': 'CompareStreetAddress',
    'query': CREATE_STREET_ADDRESS_COMPARISON_FUNCTION
}, {
    'name': 'CompareCity',
    'query': CREATE_CITY_COMPARISON_FUNCTION
}, {
    'name': 'CompareState',
    'query': CREATE_STATE_COMPARISON_FUNCTION
}, {
    'name': 'CompareZipCode',
    'query': CREATE_ZIP_CODE_COMPARISON_FUNCTION
}, {
    'name': 'CompareEmail',
    'query': CREATE_EMAIL_COMPARISON_FUNCTION
}, {
    'name': 'ComparePhoneNumber',
    'query': CREATE_PHONE_NUMBER_COMPARISON_FUNCTION
}, {
    'name': 'CompareSexAtBirth',
    'query': CREATE_SEX_COMPARISON_FUNCTION
}, {
    'name': 'CompareDateOfBirth',
    'query': CREATE_DOB_COMPARISON_FUNCTION
}]
