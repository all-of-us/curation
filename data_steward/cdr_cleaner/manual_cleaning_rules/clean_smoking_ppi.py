"""
Several answers to smoking questions were incorrectly coded as questions
This rule generates corrected rows and deletes incorrect rows
"""
import bq_utils
from constants.cdr_cleaner import clean_cdr as cdr_consts
import sandbox

SMOKING_LOOKUP_TABLE = 'smoking_lookup'
NEW_SMOKING_ROWS = 'new_smoking_rows'


SMOKING_LOOKUP_FIELDS = [
    {
        "type": "string",
        "name": "type",
        "mode": "nullable",
        "description": ""
    },
    {
        "type": "string",
        "name": "observation_source_value_info",
        "mode": "nullable",
        "description": ""
    },
    {
        "type": "integer",
        "name": "rank",
        "mode": "nullable",
        "description": ""
    },
    {
        "type": "integer",
        "name": "observation_source_concept_id",
        "mode": "nullable",
        "description": ""
    },
    {
        "type": "integer",
        "name": "observation_source_concept_id",
        "mode": "nullable",
        "description": ""
    },
    {
        "type": "integer",
        "name": "value_as_concept_id",
        "mode": "nullable",
        "description": ""
    },
    {
        "type": "integer",
        "name": "new_observation_concept_id",
        "mode": "nullable",
        "description": ""
    },
    {
        "type": "integer",
        "name": "new_observation_source_concept_id",
        "mode": "nullable",
        "description": ""
    },
    {
        "type": "integer",
        "name": "new_value_as_concept_id",
        "mode": "nullable",
        "description": ""
    },
    {
        "type": "integer",
        "name": "new_value_source_concept_id",
        "mode": "nullable",
        "description": ""
    }
]


SANDBOX_CREATE_QUERY = """
CREATE TABLE `{project_id}.{sandbox_dataset_id}.{new_smoking_rows}`
AS
SELECT
    observation_id,
    person_id,
    observation_concept_id,
    observation_date,
    observation_datetime,
    observation_type_concept_id,
    value_as_number,
    value_as_string,
    value_as_concept_id,
    qualifier_concept_id,
    unit_concept_id,
    provider_id,
    visit_occurrence_id,
    observation_source_value,
    observation_source_concept_id,
    unit_source_value,
    qualifier_source_value,
    value_source_concept_id,
    value_source_value,
    questionnaire_response_id
FROM 
(SELECT
    observation_id,
    person_id,
    new_observation_concept_id as observation_concept_id, 
    observation_date,
    observation_datetime,
    observation_type_concept_id,
    value_as_number,
    value_as_string,
    new_value_as_concept_id as value_as_concept_id,
    qualifier_concept_id,
    unit_concept_id,
    provider_id,
    visit_occurrence_id,
    observation_source_value,
    new_observation_source_concept_id as observation_source_concept_id,
    unit_source_value,
    qualifier_source_value,
    new_value_source_concept_id as value_source_concept_id,
    value_source_value,
    questionnaire_response_id, 
    ROW_NUMBER() OVER(PARTITION BY person_id, new_observation_source_concept_id ORDER BY rank ASC) AS this_row
FROM
    `{project_id}.{sandbox_dataset_id}.{smoking_lookup_table}`
JOIN `{project_id}.{combined_dataset_id}.observation` 
    USING (observation_source_concept_id, value_as_concept_id)
)
WHERE this_row=1
"""

DELETE_INCORRECT_RECORDS = """
DELETE
FROM `{project_id}.{combined_dataset_id}.observation`
WHERE observation_source_concept_id IN
(SELECT
  observation_source_concept_id
FROM `{project_id}.{sandbox_dataset_id}.{smoking_lookup_table}`
)
"""

INSERT_CORRECTED_RECORDS = """
INSERT INTO `{project_id}.{combined_dataset_id}.observation` 
    (observation_id,
    person_id,
    observation_concept_id,
    observation_date,
    observation_datetime,
    observation_type_concept_id,
    value_as_number,
    value_as_string,
    value_as_concept_id,
    qualifier_concept_id,
    unit_concept_id,
    provider_id,
    visit_occurrence_id,
    observation_source_value,
    observation_source_concept_id,
    unit_source_value,
    qualifier_source_value,
    value_source_concept_id,
    value_source_value,
    questionnaire_response_id)
SELECT
    *
FROM `{project_id}.{sandbox_dataset_id}.{new_smoking_rows}`
"""


def get_queries_clean_smoking(project_id, dataset_id, sandbox_dataset_id):
    """
    Queries to run for deleting incorrect smoking rows and inserting corrected rows

    :param project_id: project id associated with the dataset to run the queries on
    :param dataset_id: dataset id to run the queries on
    :param sandbox_dataset_id: dataset id of the sandbox
    :return: list of query dicts
    """
    queries = []

    # fetch sandbox_dataset_id
    if sandbox_dataset_id is None:
        sandbox_dataset_id = sandbox.get_sandbox_dataset_id(dataset_id)

    # load smoking_lookup table
    bq_utils.load_table_from_csv(project_id,
                                 sandbox_dataset_id,
                                 SMOKING_LOOKUP_TABLE,
                                 csv_path=None,
                                 fields=SMOKING_LOOKUP_FIELDS)

    sandbox_query = dict()
    sandbox_query[cdr_consts.QUERY] = SANDBOX_CREATE_QUERY.format(project_id=project_id,
                                                                  combined_dataset_id=dataset_id,
                                                                  sandbox_dataset_id=sandbox_dataset_id,
                                                                  new_smoking_rows=NEW_SMOKING_ROWS,
                                                                  smoking_lookup_table=SMOKING_LOOKUP_TABLE)
    queries.append(sandbox_query)

    delete_query = dict()
    delete_query[cdr_consts.QUERY] = DELETE_INCORRECT_RECORDS.format(project_id=project_id,
                                                                     combined_dataset_id=dataset_id,
                                                                     sandbox_dataset_id=sandbox_dataset_id,
                                                                     smoking_lookup_table=SMOKING_LOOKUP_TABLE)
    queries.append(delete_query)

    insert_query = dict()
    insert_query[cdr_consts.QUERY] = INSERT_CORRECTED_RECORDS.format(project_id=project_id,
                                                                     combined_dataset_id=dataset_id,
                                                                     sandbox_dataset_id=sandbox_dataset_id,
                                                                     new_smoking_rows=NEW_SMOKING_ROWS)
    queries.append(insert_query)

    return queries


def parse_args():
    """
    Add sandbox_dataset_id to the default cdr_cleaner.args_parser argument list

    :return: an expanded argument list object
    """
    import cdr_cleaner.args_parser as parser

    additional_argument = {parser.SHORT_ARGUMENT: '-n',
                           parser.LONG_ARGUMENT: '--sandbox_dataset_id',
                           parser.ACTION: 'store',
                           parser.DEST: 'sandbox_dataset_id',
                           parser.HELP: 'Please specify the sandbox_dataset_id',
                           parser.REQUIRED: True}
    args = parser.default_parse_args([additional_argument])
    return args


if __name__ == '__main__':
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parse_args()

    clean_engine.add_console_logging(ARGS.console_log)
    query_list = get_queries_clean_smoking(ARGS.project_id, ARGS.dataset_id, ARGS.sandbox_dataset_id)
    clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id, query_list)
