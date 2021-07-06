"""
Adding COPE survey version info to the observation_ext table.

Original Issue:  DC-1040

The intent is to provide COPE survey version info as part of the observation_ext
table.  In an ad-hoc form, this relies on receiving questionnaire_response_id to
COPE month mapping information from the RDR team.  It also currently relies on
the static mapping of an AoU custom concept id to a COPE survey month.  Once
these two processes are well defined and automated, this can be integrated
into the pipeline as a well-formed cleaning rule.
"""
# Python Imports
import logging

# Third party imports
from google.api_core.exceptions import BadRequest, NotFound

# Project imports
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from cdr_cleaner.cleaning_rules.generate_ext_tables import GenerateExtTables
from common import OBSERVATION, JINJA_ENV
from constants.cdr_cleaner import clean_cdr as cdr_consts

LOGGER = logging.getLogger(__name__)

ISSUE_NUMBERS = ['DC-1040']

VERSION_COPE_SURVEYS_QUERY = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project_id}}.{{out_dataset_id}}.observation_ext` AS (
    SELECT
    oe.src_id,
    oe.observation_id,
    -- this will work for a one off solution, but needs a mapping to accurately --
    -- map concept-ids to cope-months for a sustainable, long-term solution --
    CASE
      WHEN cssf.cope_month = 'may' THEN 2100000002
      WHEN cssf.cope_month = 'june' THEN 2100000003
      WHEN cssf.cope_month = 'july' THEN 2100000004
      WHEN cssf.cope_month = 'nov' THEN 2100000005
      WHEN cssf.cope_month = 'dec' THEN 2100000006
      WHEN cssf.cope_month = 'feb' THEN 2100000007
      ELSE survey_version_concept_id
    END AS survey_version_concept_id
    FROM `{{project_id}}.{{out_dataset_id}}.observation_ext` AS oe
    JOIN `{{project_id}}.{{out_dataset_id}}.observation` AS o
    USING (observation_id)
    LEFT JOIN `{{project_id}}.{{qrid_map_dataset_id}}._deid_questionnaire_response_map` AS m
    ON o.questionnaire_response_id = m.research_response_id
    -- the file generating this table is manually imported from the RDR. --
    -- Curation and RDR should automate this process. --
    LEFT JOIN `{{project_id}}.{{cope_table_dataset_id}}.{{cope_survey_mapping_table}}` AS cssf
    ON cssf.questionnaire_response_id = m.questionnaire_response_id
)""")


class COPESurveyVersionTask(BaseCleaningRule):
    """
    Map COPE survey version to AoU custom concept id.

    Given a mapping of questionnaire_concept_ids to COPE month from the RDR
    team, assign a custom COPE month concept id for COPE survey information.
    """

    def __init__(self, project_id, dataset_id, sandbox_dataset_id,
                 cope_lookup_dataset_id, cope_table_name):
        """
        Initialize the class with proper info.

        Set the issue numbers, description and affected datasets.  As other
        tickets may affect this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = (
            f'Add COPE survey information after the data is de-identified.  '
            f'Only applies to COPE surveys.')
        super().__init__(
            issue_numbers=ISSUE_NUMBERS,
            description=desc,
            # first seen in the `deid_base` dataset, although
            # not explicitly part of this set of cleaning rules yet
            affected_datasets=[cdr_consts.DEID_BASE],
            project_id=project_id,
            dataset_id=dataset_id,
            sandbox_dataset_id=sandbox_dataset_id,
            affected_tables=[OBSERVATION + '_ext'],
            depends_on=[GenerateExtTables])

        self.qrid_map_dataset_id = sandbox_dataset_id
        self.cope_lookup_dataset_id = cope_lookup_dataset_id
        self.cope_survey_table = cope_table_name

    def get_query_specs(self):
        """
        Return a list of dictionary query specifications.

        :return:  A list of dictionaries.  Each dictionary contains a
            single query and a specification for how to execute that query.
            The specifications are optional but the query is required.
        """
        apply_cope_survey_versions = {
            cdr_consts.QUERY:
                VERSION_COPE_SURVEYS_QUERY.render(
                    project_id=self.project_id,
                    out_dataset_id=self.dataset_id,
                    qrid_map_dataset_id=self.qrid_map_dataset_id,
                    cope_table_dataset_id=self.cope_lookup_dataset_id,
                    cope_survey_mapping_table=self.cope_survey_table)
        }

        return [apply_cope_survey_versions]

    def setup_rule(self, client):
        """
        Verifies the cope survey table actually exists where it is specified.

        Because adding the cope survey is a manual process, this extra check
        allows the software to validate a table actually exists before attempting
        to use it in a cleaning query.
        """
        msg = ''
        tables = client.list_tables(self.cope_lookup_dataset_id)
        try:
            table_ids = [table.table_id for table in tables]
        except (BadRequest):
            msg = (f"{self.__class__.__name__} cannot execute because "
                   f"'{self.project_id}' is not a valid project identifier")
            LOGGER.exception(msg)
        except (NotFound):
            msg = (f"{self.__class__.__name__} cannot execute because dataset: "
                   f"'{self.cope_lookup_dataset_id}' "
                   f"does not exist in project: '{self.project_id}'")
            LOGGER.exception(msg)
        else:
            if self.cope_survey_table not in table_ids:
                msg = (f"{self.__class__.__name__} cannot execute because the "
                       f"cope survey mapping table: '{self.cope_survey_table}' "
                       f"does not exist in "
                       f"'{self.project_id}.{self.cope_lookup_dataset_id}'")
                LOGGER.error(msg)
        finally:
            # raise the message to error out of a running shell script
            if msg:
                raise RuntimeError(msg)

    def setup_validation(self, client):
        """
        Run required steps for validation setup

        This abstract method was added to the base class after this rule was authored.
        This rule needs to implement logic to setup validation on cleaning rules that
        will be updating or deleting the values.
        Until done no issue exists for this yet.
        """
        raise NotImplementedError("Please fix me.")

    def validate_rule(self):
        """
        Validates the cleaning rule which deletes or updates the data from the tables

        This abstract method was added to the base class after this rule was authored.
        This rule needs to implement logic to run validation on cleaning rules that will
        be updating or deleting the values.
        Until done no issue exists for this yet.
        """
        raise NotImplementedError("Please fix me.")

    def get_sandbox_tablenames(self):
        """
        Does not remove any records.  Adds info to records.
        """
        return []


def add_console_logging(add_handler):
    """

    This config should be done in a separate module, but that can wait
    until later.  Useful for debugging.

    Copied from the clean engine to break the dependency on the clean engine
    in preparation for the upcoming pipeline execution.
    """
    logging.basicConfig(
        level=logging.INFO,
        filename=FILENAME,
        filemode='a',
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    if add_handler:
        handler = logging.StreamHandler()
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(name)s - %(message)s')
        handler.setFormatter(formatter)
        logging.getLogger('').addHandler(handler)


if __name__ == '__main__':
    import cdr_cleaner.args_parser as ap
    # import cdr_cleaner.clean_cdr_engine as clean_engine
    from constants.cdr_cleaner.clean_cdr_engine import FILENAME
    from utils import bq

    parser = ap.get_argument_parser()
    parser.add_argument(
        '--cope_survey_dataset',
        action='store',
        dest='cope_survey_dataset_id',
        help=('Dataset containing the mapping table provided by RDR team.  '
              'These maps questionnaire_response_ids to cope_months.'),
        required=True)
    parser.add_argument(
        '--cope_survey_table',
        action='store',
        dest='cope_survey_table',
        required=True,
        help='Name of the table cotaining the cope survey mapping information')
    parser.add_argument(
        '--mapping_dataset',
        dest='mapping_dataset_id',
        action='store',
        help=('Dataset name for the dataset containing deid mapping tables.  '
              'For example, _deid_map and _deid_questionnaire_response_id.'))

    ARGS = parser.parse_args()
    if not ARGS.mapping_dataset_id:
        parser.error("The deid mapping dataset is required to run this script.")

    # clean_engine.add_console_logging(ARGS.console_log)
    add_console_logging(ARGS.console_log)
    version_task = COPESurveyVersionTask(ARGS.project_id, ARGS.dataset_id,
                                         ARGS.sandbox_dataset_id,
                                         ARGS.cope_survey_dataset_id,
                                         ARGS.cope_survey_table)
    query_list = version_task.get_query_specs()

    if ARGS.list_queries:
        version_task.log_queries()
    else:
        client_obj = bq.get_client(ARGS.project_id)
        version_task.setup_rule(client_obj)
        # clean_engine.clean_dataset(ARGS.project_id, query_list)
        for query in query_list:
            q = query.get(cdr_consts.QUERY)
            if q:
                query_job = client_obj.query(q)
                query_job.result()
                if query_job.exception():
                    LOGGER.error(
                        "BAIL OUT!!  SURVEY VERSION TASK encountered an exception"
                    )
                    LOGGER.error(query_job.exception())
            else:
                LOGGER.error("NO QUERY GENERATED for survey_version_info taks")
