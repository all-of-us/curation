# Python imports
import logging

# Project imports
import bq_utils
from validation.ehr_union import mapping_table_for
import constants.cdr_cleaner.clean_cdr as cdr_consts
from common import AOU_REQUIRED
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from validation.participants import readers
from utils.bq import JINJA_ENV as jinja_env

LOGGER = logging.getLogger(__name__)

COUNT_HPO_SOURCE_RECORDS_TEMPLATE = jinja_env.from_string("""
SELECT
    '{{hpo_id}}' AS src_hpo_id,
    COUNT(*) AS num_of_records
FROM `{{project_id}}.{{dataset}}.{{table_name}}`
""")

COUNT_HPO_RECORDS_TEMPLATE = jinja_env.from_string("""
SELECT
    m.src_hpo_id,
    COUNT(*) AS num_of_records
FROM `{{project_id}}.{{dataset}}.{{table}}` AS t
JOIN `{{project_id}}.{{dataset}}.{{mapping_table}}` AS m 
    ON t.{{table_id}} = m.{{table_id}}
GROUP BY m.src_hpo_id

""")

COMPARE_COUNT_QUERY_TEMPLATE = jinja_env.from_string("""
CREATE OR REPLACE TABLE `{{project_id}}.{{sandbox_dataset}}.{{sandbox_table}}` AS 
SELECT
    hpo.src_hpo_id,
    hpo.num_of_records AS records_in_ehr,
    ehr.num_of_records AS records_in_union_ehr,
    combined.num_of_records AS records_in_combined,
    '{{table}}' AS table
FROM (
    {{source_query}}
) hpo 
LEFT JOIN (
    {{union_ehr_query}}
) AS ehr
    ON hpo.src_hpo_id = ehr.src_hpo_id
LEFT JOIN (
    {{combined_query}}
) AS combined
    ON hpo.src_hpo_id = combined.src_hpo_id
""")


class ValidateCombinedDataset(BaseCleaningRule):

    def __init__(self, project_id, dataset_id, sandbox_dataset_id,
                 union_ehr_dataset_id, ehr_dataset_id):

        desc = ('Compare the row counts per HPO site across {dataset_id}, '
                '{union_ehr_dataset_id}, {ehr_dataset_id}'.format(
                    dataset_id=dataset_id,
                    union_ehr_dataset_id=union_ehr_dataset_id,
                    ehr_dataset_id=ehr_dataset_id))

        super().__init__(issue_numbers=[],
                         description=desc,
                         affected_datasets=[],
                         affected_tables=AOU_REQUIRED,
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id)

        self._ehr_dataset_id = ehr_dataset_id
        self._union_ehr_dataset_id = union_ehr_dataset_id

    def get_query_specs(self):
        """
        Return a list of dictionary query specifications.

        :return:  A list of dictionaries. Each dictionary contains a single query
            and a specification for how to execute that query. The specifications
            are optional but the query is required.
        """

        query_dicts = []
        for cdm_table in AOU_REQUIRED:
            count_hpo_source_records_query = self.get_count_hpo_table_query(
                cdm_table)
            id_col = '{table_name}_id'.format(table_name=cdm_table)
            mapping_table = mapping_table_for(cdm_table)

            count_combined_hpo_records_query = COUNT_HPO_RECORDS_TEMPLATE.render(
                project_id=self._project_id,
                dataset=self._dataset_id,
                table=cdm_table,
                mapping_table=mapping_table,
                table_id=id_col)

            count_ehr_union_hpo_records_query = COUNT_HPO_RECORDS_TEMPLATE.render(
                project_id=self._project_id,
                dataset=self._union_ehr_dataset_id,
                table=cdm_table,
                mapping_table=mapping_table,
                table_id=id_col)

            comparison_query = COMPARE_COUNT_QUERY_TEMPLATE.render(
                project_id=self._project_id,
                sandbox_dataset=self._sandbox_dataset_id,
                sandbox_table=cdm_table + '_comparison',
                source_query=count_hpo_source_records_query,
                union_ehr_query=count_ehr_union_hpo_records_query,
                combined_query=count_combined_hpo_records_query,
                table=cdm_table)
            LOGGER.info(comparison_query)
            query_dicts.append({cdr_consts.QUERY: comparison_query})

        return query_dicts

    def get_count_hpo_table_query(self, cdm_table):
        hpo_count_query_list = []
        for hpo_id in readers.get_hpo_site_names():
            hpo_id_cdm_table = bq_utils.get_table_id(hpo_id, cdm_table)
            if bq_utils.table_exists(hpo_id_cdm_table, self._ehr_dataset_id):
                hpo_count_query = COUNT_HPO_SOURCE_RECORDS_TEMPLATE.render(
                    project_id=self._project_id,
                    dataset=self._ehr_dataset_id,
                    table_name=hpo_id_cdm_table,
                    hpo_id=hpo_id)
                hpo_count_query_list.append(hpo_count_query)

        return '\nUNION ALL\n'.join(hpo_count_query_list)

    def setup_rule(self, client):
        """
        Function to run any data upload options before executing a query.
        """
        pass

    def setup_validation(self, client):
        """
        Run required steps for validation setup
        """
        raise NotImplementedError("Please fix me.")

    def validate_rule(self, client):
        """
        Validates the cleaning rule which deletes or updates the data from the tables
        """
        raise NotImplementedError("Please fix me.")

    def get_sandbox_tablenames(self):
        return f'{self._issue_numbers[0].lower()}_{self._affected_tables[0]}'


def parse_args():
    """
    This function expands the default argument list defined in cdr_cleaner.args_parser
    :return: an expanded argument list object
    """

    import cdr_cleaner.args_parser as parser

    additional_arguments = [{
        parser.SHORT_ARGUMENT: '-u',
        parser.LONG_ARGUMENT: '--union_ehr_dataset_id',
        parser.ACTION: 'store',
        parser.DEST: 'union_ehr_dataset_id',
        parser.HELP: 'union_ehr_dataset_id',
        parser.REQUIRED: True
    }, {
        parser.SHORT_ARGUMENT: '-e',
        parser.LONG_ARGUMENT: '--ehr_dataset_id',
        parser.ACTION: 'store',
        parser.DEST: 'ehr_dataset_id',
        parser.HELP: 'ehr_dataset_id',
        parser.REQUIRED: True
    }]
    args = parser.default_parse_args(additional_arguments)
    return args


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id,
            ARGS.dataset_id,
            ARGS.sandbox_dataset_id, [(ValidateCombinedDataset,)],
            union_ehr_dataset_id=ARGS.union_ehr_dataset_id,
            ehr_dataset_id=ARGS.ehr_dataset_id)
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(
            ARGS.project_id,
            ARGS.dataset_id,
            ARGS.sandbox_dataset_id,
            ARGS.ehr_dataset_id, [(ValidateCombinedDataset,)],
            union_ehr_dataset_id=ARGS.union_ehr_dataset_id,
            ehr_dataset_id=ARGS.ehr_dataset_id)
