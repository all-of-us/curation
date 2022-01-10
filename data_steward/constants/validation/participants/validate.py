from common import JINJA_ENV

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
