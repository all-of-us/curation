"""
Background

Some values for procedure_source_concept_id in the procedure_occurrence table have been identified as CPT modifiers,
which are supposed to be in the modifier_concept_id field.

While the CPT modifiers could be moved to the correct field, this would leave these rows with
procedure_source_concept_id=0, which has little utility. Therefore, these rows should be dropped.

Cleaning rule to remove records where:
-procedure_concept_id is not a standard concept in the procedure domain
AND
-procedure_source_concept_id is not in the procedure domain (they ARE allowed to be non-standard).

"""

# Project imports
import constants.cdr_cleaner.clean_cdr as cdr_consts
from sandbox import get_sandbox_dataset_id

INTERMEDIARY_TABLE_NAME = 'procedure_occurrence_dc583'

INVALID_PROCEDURE_SOURCE_CONCEPT_IDS_QUERY = """
CREATE OR REPLACE TABLE
`{project}.{sandbox_dataset}.{intermediary_table}` AS
SELECT *
FROM
  `{project}.{dataset}.procedure_occurrence` p
-- procedure_concept_id is not a standard concept in the procedure domain
WHERE p.procedure_concept_id NOT IN (
  SELECT
    concept_id
  FROM
    `{project}.{dataset}.concept`
  WHERE
    domain_id = 'Procedure'
    AND TRIM(concept_class_id) IN ('Procedure', 'CPT4')
    AND standard_concept = 'S'
)
AND
-- procedure_source_concept_id is not in the procedure domain
p.procedure_source_concept_id IN (
 SELECT
    concept_id
  FROM
    `{project}.{dataset}.concept`
  WHERE
    TRIM(concept_class_id) = 'CPT4 Modifier'
)
"""

VALID_PROCEDURE_SOURCE_CONCEPT_IDS_QUERY = """
SELECT * FROM
`{project}.{dataset}.procedure_occurrence`
WHERE
procedure_occurrence_id
IN (SELECT procedure_occurrence_id FROM `{project}.{sandbox_dataset}.{intermediary_table}`)
"""


def get_remove_invalid_procedure_source_queries(project_id, dataset_id):
    """
    runs the query which removes records that contain incorrect values in the procedure_source_concept_id field
    invalid procedure_source_concept_ids are where it is not in the procedure domain and
    procedure_concept_id is not standard in the procedure domain

    :param project_id: Name of the project
    :param dataset_id: Name of the dataset where the queries should be run
    :return:
    """
    queries_list = []

    # queries to sandbox
    invalid_records = dict()
    invalid_records[
        cdr_consts.QUERY] = INVALID_PROCEDURE_SOURCE_CONCEPT_IDS_QUERY.format(
            project=project_id,
            dataset=dataset_id,
            sandbox_dataset=get_sandbox_dataset_id(dataset_id),
            intermediary_table=INTERMEDIARY_TABLE_NAME)
    queries_list.append(invalid_records)

    # queries to delete invalid procedure source records
    valid_records = dict()
    valid_records[
        cdr_consts.QUERY] = VALID_PROCEDURE_SOURCE_CONCEPT_IDS_QUERY.format(
            project=project_id,
            dataset=dataset_id,
            sandbox_dataset=get_sandbox_dataset_id(dataset_id),
            intermediary_table=INTERMEDIARY_TABLE_NAME)
    queries_list.append(valid_records)

    return queries_list


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()
    clean_engine.add_console_logging(ARGS.console_log)
    query_list = get_remove_invalid_procedure_source_queries(
        ARGS.project_id, ARGS.dataset_id)
    clean_engine.clean_dataset(ARGS.project_id, query_list)
