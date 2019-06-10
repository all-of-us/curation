# Create queries here and name them
GET_PERSON_IDS = 'SELECT person_id FROM `{project}.{dataset}.person`'

GET_YEAR_OF_BIRTH_PERSON_IDS = '''
    SELECT *
    FROM `{project}.{dataset}.{table}`
    WHERE person_id
    IN
        (SELECT
            person_id
        FROM
            `{project}.{dataset}.person` p
        WHERE
            p.year_of_birth < 1800
        OR
            p.year_of_birth > 2019)
    '''

REMOVE_YEAR_OF_BIRTH_PERSON_IDS = '''
    SELECT *
    FROM
        `{project}.{dataset}.person` p
    WHERE
        p.year_of_birth < 1800
    OR
        p.year_of_birth > 2019
    '''

# Ensure query order is correct in this list
SQL_QUERIES = [
    GET_PERSON_IDS,
    GET_YEAR_OF_BIRTH_PERSON_IDS,
    REMOVE_YEAR_OF_BIRTH_PERSON_IDS
]
