"""
Constants file for validate

Name Comparison:
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
# Python imports

# Project imports
from common import JINJA_ENV

RDR_SEX = 'rdr_sex'
EHR_SEX = 'ehr_sex'
MATCH_STATUS = 'match_status'
MATCH = 'match'
NO_MATCH = 'no_match'
MISSING_EHR = 'missing_ehr'
MISSING_RDR = 'missing_rdr'
MATCH_STATUS_PAIRS = 'match_status_pairs'
GENDER_MATCH = [{
    MATCH_STATUS:
        MATCH,
    MATCH_STATUS_PAIRS: [{
        RDR_SEX: ["SexAtBirth_Male"],
        EHR_SEX: ["MALE"]
    }, {
        RDR_SEX: ["SexAtBirth_Female"],
        EHR_SEX: ["FEMALE"]
    }, {
        RDR_SEX: ["SexAtBirth_SexAtBirthNoneOfThese"],
        EHR_SEX: ["UNKNOWN", "OTHER", "AMBIGUOUS"]
    }]
}, {
    MATCH_STATUS:
        NO_MATCH,
    MATCH_STATUS_PAIRS: [{
        RDR_SEX: ["SexAtBirth_Male"],
        EHR_SEX: [
            "UNKNOWN", "Gender unknown", "AMBIGUOUS", "Gender unspecified",
            "OTHER", "FEMALE"
        ]
    }, {
        RDR_SEX: ["SexAtBirth_Female"],
        EHR_SEX: [
            "UNKNOWN", "Gender unknown", "AMBIGUOUS", "Gender unspecified",
            "OTHER", "MALE"
        ]
    }, {
        RDR_SEX: ["SexAtBirth_Intersex"],
        EHR_SEX: [
            "AMBIGUOUS", "Gender unknown", "Gender unspecified", "FEMALE",
            "MALE", "UNKNOWN", "OTHER"
        ]
    }, {
        RDR_SEX: ["SexAtBirth_SexAtBirthNoneOfThese"],
        EHR_SEX: ["FEMALE", "MALE", "Gender unspecified", "Gender unknown"]
    }]
}, {
    MATCH_STATUS:
        MISSING_EHR,
    MATCH_STATUS_PAIRS: [{
        RDR_SEX: [
            "SexAtBirth_Male", "SexAtBirth_Female", "SexAtBirth_Intersex",
            "SexAtBirth_SexAtBirthNoneOfThese"
        ],
        EHR_SEX: ["No matching concept"]
    }]
}, {
    MATCH_STATUS:
        MISSING_RDR,
    MATCH_STATUS_PAIRS: [{
        RDR_SEX: ["UNSET", "PMI_Skip", "PMI_PreferNotToAnswer"],
        EHR_SEX: [
            "MALE", "OTHER", "Gender unspecified", "AMBIGUOUS", "FEMALE",
            "UNKNOWN", "Gender unknown", "No matching concept"
        ]
    }]
}]

STATE_ABBREVIATIONS = [
    'al',
    'ak',
    'az',
    'ar',
    'ca',
    'co',
    'ct',
    'de',
    'fl',
    'ga',
    'hi',
    'id',
    'il',
    'in',
    'ia',
    'ks',
    'ky',
    'la',
    'me',
    'md',
    'ma',
    'mi',
    'mn',
    'ms',
    'mo',
    'mt',
    'ne',
    'nv',
    'nh',
    'nj',
    'nm',
    'ny',
    'nc',
    'nd',
    'oh',
    'ok',
    'or',
    'pa',
    'ri',
    'sc',
    'sd',
    'tn',
    'tx',
    'ut',
    'vt',
    'va',
    'wa',
    'wv',
    'wi',
    'wy',
    # Commonwealth/Territory:
    'as',
    'dc',
    'fm',
    'gu',
    'mh',
    'mp',
    'pw',
    'pr',
    'vi',
    # Military "State":
    'aa',
    'ae',
    'ap',
]
ADDRESS_ABBREVIATIONS = {
    'aly': 'alley',
    'anx': 'annex',
    'apt': 'apartment',
    'ave': 'avenue',
    'bch': 'beach',
    'bldg': 'building',
    'blvd': 'boulevard',
    'bnd': 'bend',
    'btm': 'bottom',
    'cir': 'circle',
    'ct': 'court',
    'co': 'county',
    'ctr': 'center',
    'dr': 'drive',
    'e': 'east',
    'expy': 'expressway',
    'hts': 'heights',
    'hwy': 'highway',
    'is': 'island',
    'jct': 'junction',
    'lk': 'lake',
    'ln': 'lane',
    'mtn': 'mountain',
    'n': 'north',
    'ne': 'northeast',
    'num': 'number',
    'nw': 'northwest',
    'pkwy': 'parkway',
    'pl': 'place',
    'plz': 'plaza',
    'po': 'post office',
    'rd': 'road',
    'rdg': 'ridge',
    'rr': 'rural route',
    'rm': 'room',
    's': 'south',
    'se': 'southeast',
    'sq': 'square',
    'st': 'street',
    'str': 'street',
    'sta': 'station',
    'ste': 'suite',
    'sw': 'southwest',
    'ter': 'terrace',
    'tpke': 'turnpike',
    'trl': 'trail',
    'vly': 'valley',
    'w': 'west',
    'way': 'way',
}
CITY_ABBREVIATIONS = {
    'st': 'saint',
    'afb': 'air force base',
}

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

CREATE_STREET_COMPARISON_FUNCTION = JINJA_ENV.from_string("""
CREATE FUNCTION IF NOT EXISTS
  `{{project_id}}.{{drc_dataset_id}}.CompareStreetAddress`(rdr_street string, ehr_street string)
  RETURNS string AS ((
    {{street_with_clause}}
    SELECT
      CASE
        WHEN normalized_rdr_street.rdr_street = normalized_ehr_street.ehr_street THEN '{{match}}'
        WHEN normalized_rdr_street.rdr_street IS NOT NULL AND normalized_ehr_street.ehr_street IS NOT NULL THEN '{{no_match}}'
        WHEN normalized_rdr_street.rdr_street IS NULL THEN '{{missing_rdr}}'
        ELSE '{{missing_ehr}}'
      END AS street
    FROM normalized_rdr_street, normalized_ehr_street));
""")

CREATE_CITY_COMPARISON_FUNCTION = JINJA_ENV.from_string("""
CREATE FUNCTION IF NOT EXISTS
  `{{project_id}}.{{drc_dataset_id}}.CompareCity`(rdr_city string, ehr_city string)
  RETURNS string AS ((
    {{city_with_clause}}
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
            SELECT 
              CASE
                WHEN REPLACE(LOWER(TRIM(rdr_state)), 'piistate_', '') IN ({{state_abbreviations}})
                THEN REPLACE(LOWER(TRIM(rdr_state)), 'piistate_', '')
                WHEN rdr_state IS NULL THEN NULL
                ELSE ''
              END AS rdr_state
        )
        , normalized_ehr_state AS (
            SELECT
              CASE
                WHEN LOWER(TRIM(ehr_state)) IN ({{state_abbreviations}})
                THEN LOWER(TRIM(ehr_state))
                WHEN ehr_state IS NULL THEN NULL
                ELSE ''
              END AS ehr_state
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
            SELECT LPAD(SPLIT(SPLIT(TRIM(rdr_zip_code), '-')[OFFSET(0)], ' ')[OFFSET(0)], 5, '0') AS rdr_zip_code
        )
        , normalized_ehr_zip_code AS (
            SELECT LPAD(SPLIT(SPLIT(TRIM(ehr_zip_code), '-')[OFFSET(0)], ' ')[OFFSET(0)], 5, '0') AS ehr_zip_code
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
    'query': CREATE_STREET_COMPARISON_FUNCTION
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

MATCH_FIELDS_QUERY = JINJA_ENV.from_string("""
    UPDATE `{{project_id}}.{{drc_dataset_id}}.{{id_match_table_id}}` upd
    SET upd.first_name = `{{project_id}}.{{drc_dataset_id}}.CompareName`(ps.first_name, ehr_name.first_name),
        upd.middle_name = `{{project_id}}.{{drc_dataset_id}}.CompareName`(ps.middle_name, ehr_name.middle_name),
        upd.last_name = `{{project_id}}.{{drc_dataset_id}}.CompareName`(ps.last_name, ehr_name.last_name),
        upd.address_1 = `{{project_id}}.{{drc_dataset_id}}.CompareStreetAddress`(ps.street_address, ehr_location.address_1),
        upd.address_2 = `{{project_id}}.{{drc_dataset_id}}.CompareStreetAddress`(ps.street_address2, ehr_location.address_2),
        upd.city = `{{project_id}}.{{drc_dataset_id}}.CompareCity`(ps.city, ehr_location.city),
        upd.state = `{{project_id}}.{{drc_dataset_id}}.CompareState`(ps.state, ehr_location.state),
        upd.zip = `{{project_id}}.{{drc_dataset_id}}.CompareZipCode`(ps.zip_code, ehr_location.zip),
        upd.email = `{{project_id}}.{{drc_dataset_id}}.CompareEmail`(ps.email, ehr_email.email),
        upd.phone_number = `{{project_id}}.{{drc_dataset_id}}.ComparePhoneNumber`(ps.phone_number, ehr_phone.phone_number),
        upd.birth_date = `{{project_id}}.{{drc_dataset_id}}.CompareDateOfBirth`(ps.date_of_birth, ehr_dob.date_of_birth),
        upd.sex = `{{project_id}}.{{drc_dataset_id}}.CompareSexAtBirth`(ps.sex, ehr_sex.sex),
        upd.algorithm = 'yes'
    FROM `{{project_id}}.{{drc_dataset_id}}.{{ps_api_table_id}}` ps
    LEFT JOIN `{{project_id}}.{{ehr_ops_dataset_id}}.{{hpo_pii_email_table_id}}` ehr_email
        ON ehr_email.person_id = ps.person_id
    LEFT JOIN `{{project_id}}.{{ehr_ops_dataset_id}}.{{hpo_pii_phone_number_table_id}}` ehr_phone
        ON ehr_phone.person_id = ps.person_id
    LEFT JOIN ( SELECT person_id, DATE(birth_datetime) AS date_of_birth
               FROM `{{project_id}}.{{ehr_ops_dataset_id}}.{{hpo_person_table_id}}` ) AS ehr_dob
        ON ehr_dob.person_id = ps.person_id
    LEFT JOIN ( SELECT person_id, cc.concept_name as sex
                FROM `{{project_id}}.{{ehr_ops_dataset_id}}.{{hpo_person_table_id}}`
                JOIN `{{project_id}}.{{ehr_ops_dataset_id}}.concept` cc
                    ON gender_concept_id = concept_id ) AS ehr_sex
        ON ehr_sex.person_id = ps.person_id
    LEFT JOIN `{{project_id}}.{{ehr_ops_dataset_id}}.{{hpo_pii_name_table_id}}` ehr_name
        ON ehr_name.person_id = ps.person_id
    LEFT JOIN `{{project_id}}.{{ehr_ops_dataset_id}}.{{hpo_pii_address_table_id}}` ehr_address
        ON ehr_address.person_id = ps.person_id
    LEFT JOIN `{{project_id}}.{{ehr_ops_dataset_id}}.{{hpo_location_table_id}}` ehr_location
        ON ehr_location.location_id = ehr_address.location_id
    WHERE upd.person_id = ps.person_id
        AND upd._PARTITIONTIME = ps._PARTITIONTIME
""")

SUMMARY_QUERY = JINJA_ENV.from_string("""
SELECT COUNT(*) AS row_count
FROM `{{project_id}}.{{drc_dataset_id}}.{{hpo_id}}`
""")
