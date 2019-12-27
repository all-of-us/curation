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

REMOVE_INVALID_PROCEDURE_SOURCE_CONCEPT_IDS_QUERY = """
DELETE
FROM
  `{project}.{dataset}.procedure_occurrence`
WHERE p.procedure_source_concept_id IN (
  SELECT
    concept_id
  FROM
    `{project}.{dataset}.concept`
  WHERE
    TRIM(domain_id) != 'Procedure'
)
OR 
p.procedure_source_concept_id IN (
  SELECT
    concept_id
  FROM
    unioned_ehr20191004.concept
  WHERE
    TRIM(domain_id) = 'Procedure'
    AND TRIM(concept_class_id) = 'CPT4 Modifier'
)
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

    query = dict()
    query[cdr_consts.QUERY] = REMOVE_INVALID_PROCEDURE_SOURCE_CONCEPT_IDS_QUERY.format(dataset=dataset_id,
                                                                                project=project_id,
                                                                                )
    queries_list.append(query)

    return queries_list


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()
    clean_engine.add_console_logging(ARGS.console_log)
    query_list = get_remove_invalid_procedure_source_queries(ARGS.project_id, ARGS.dataset_id)
    clean_engine.clean_dataset(ARGS.project_id, query_list)

