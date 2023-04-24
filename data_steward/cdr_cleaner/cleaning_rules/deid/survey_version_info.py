"""
Adding COPE and Minute survey version info to the observation_ext table.

Original Issue:  DC-1040, DC-2730

The intent is to provide COPE and Minute survey version info as part of the observation_ext
table.  In an ad-hoc form, this relies on receiving questionnaire_response_id to
COPE month mapping information from the RDR team.  It also currently relies on
the static mapping of an AoU custom concept id to a COPE survey month.  Once
these two processes are well defined and automated, this can be integrated
into the pipeline as a well-formed cleaning rule.

This rule has been re-ordered in the pipeline.  This allows us to drop the questionnaire_id remapping
dataset identifier.  Because the signature is being updated, we also removed the parameterization
of the cope mapping table name, as it is already stored in a variable and used by other rules.
"""
# Python Imports
import logging

# Third party imports
from google.api_core.exceptions import BadRequest, NotFound

# Project imports
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from cdr_cleaner.cleaning_rules.generate_ext_tables import GenerateExtTables
from common import (COPE_SURVEY_MAP, OBSERVATION, JINJA_ENV)
from constants.cdr_cleaner import clean_cdr as cdr_consts

LOGGER = logging.getLogger(__name__)

ISSUE_NUMBERS = ['DC-1040', 'DC-2609', 'DC2730']

VERSION_COPE_SURVEYS_QUERY = JINJA_ENV.from_string("""
UPDATE `{{project_id}}.{{dataset_id}}.observation_ext` oe
    -- this will work for a one off solution, but needs a mapping to accurately --
    -- map concept-ids to cope-months for a sustainable, long-term solution --
    -- In a future CDR, this implementation will be replaced by the survey_conduct table --
    SET survey_version_concept_id = CASE
      WHEN lower(v.cope_month) = 'may' THEN 2100000002
      WHEN lower(v.cope_month) = 'june' THEN 2100000003
      WHEN lower(v.cope_month) = 'july' THEN 2100000004
      WHEN lower(v.cope_month) = 'nov' THEN 2100000005
      WHEN lower(v.cope_month) = 'dec' THEN 2100000006
      WHEN lower(v.cope_month) = 'feb' THEN 2100000007
      WHEN lower(v.cope_month) = 'vaccine1' THEN 905047
      WHEN lower(v.cope_month) = 'vaccine2' THEN 905055
      WHEN lower(v.cope_month) = 'vaccine3' THEN 765936
      WHEN lower(v.cope_month) = 'vaccine4' THEN 1741006
    END
    FROM (
        SELECT cope_month, o.observation_id
        FROM `{{project_id}}.{{dataset_id}}.observation` AS o
        JOIN `{{project_id}}.{{cope_table_dataset_id}}.{{cope_survey_mapping_table}}` AS cssf
        ON cssf.questionnaire_response_id = o.questionnaire_response_id) v
    WHERE v.observation_id = oe.observation_id
""")


class COPESurveyVersionTask(BaseCleaningRule):
    """
    Map COPE and Minute survey versions to AoU custom concept id.

    Given a mapping of questionnaire_concept_ids to COPE and Minute survey versions from the RDR
    team, assign a custom survey concept id for survey information.
    """

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 clean_survey_dataset_id=None,
                 table_namer=None,
                 run_for_synthetic=True):
        """
        Initialize the class with proper info.

        Set the issue numbers, description and affected datasets.  As other
        tickets may affect this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = (
            f'Add COPE and Minute survey information after the data is de-identified.  '
            f'Only applies to COPE and Minute surveys.')
        super().__init__(
            issue_numbers=ISSUE_NUMBERS,
            description=desc,
            # first seen in the `deid_base` dataset, although
            # not explicitly part of this set of cleaning rules yet
            affected_datasets=[
                cdr_consts.REGISTERED_TIER_DEID, cdr_consts.CONTROLLED_TIER_DEID
            ],
            project_id=project_id,
            dataset_id=dataset_id,
            sandbox_dataset_id=sandbox_dataset_id,
            affected_tables=[f'{OBSERVATION}_ext'],
            depends_on=[GenerateExtTables],
            table_namer=table_namer,
            run_for_synthetic=True)

        if not clean_survey_dataset_id:
            raise RuntimeError("'clean_survey_dataset_id' must be set")

        self.clean_survey_dataset_id = clean_survey_dataset_id

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
                    dataset_id=self.dataset_id,
                    cope_table_dataset_id=self.clean_survey_dataset_id,
                    cope_survey_mapping_table=COPE_SURVEY_MAP)
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
        tables = client.list_tables(self.clean_survey_dataset_id)
        try:
            table_ids = [table.table_id for table in tables]
        except (BadRequest):
            msg = (f"{self.__class__.__name__} cannot execute because "
                   f"'{self.project_id}' is not a valid project identifier")
            LOGGER.exception(msg)
        except (NotFound):
            msg = (f"{self.__class__.__name__} cannot execute because dataset: "
                   f"'{self.clean_survey_dataset_id}' "
                   f"does not exist in project: '{self.project_id}'")
            LOGGER.exception(msg)
        else:
            if COPE_SURVEY_MAP not in table_ids:
                msg = (f"{self.__class__.__name__} cannot execute because the "
                       f"cope survey mapping table: '{COPE_SURVEY_MAP}' "
                       f"does not exist in "
                       f"'{self.project_id}.{self.clean_survey_dataset_id}'")
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
        return [self.sandbox_table_for(table) for table in self.affected_tables]


if __name__ == '__main__':
    import cdr_cleaner.args_parser as ap
    import cdr_cleaner.clean_cdr_engine as clean_engine

    parser = ap.get_argument_parser()
    parser.add_argument(
        '--clean_survey_dataset',
        action='store',
        dest='clean_survey_dataset_id',
        help=('Dataset containing the mapping table provided by RDR team.  '
              'These maps questionnaire_response_ids to cope_months.'),
        required=True)

    ARGS = parser.parse_args()

    clean_engine.add_console_logging(ARGS.console_log)

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id,
            ARGS.dataset_id,
            ARGS.sandbox_dataset_id, [(COPESurveyVersionTask,)],
            clean_survey_dataset_id=ARGS.clean_survey_dataset_id)
        for query_dict in query_list:
            LOGGER.info(query_dict.get(cdr_consts.QUERY))
    else:
        clean_engine.clean_dataset(
            ARGS.project_id,
            ARGS.dataset_id,
            ARGS.sandbox_dataset_id, [(COPESurveyVersionTask,)],
            clean_survey_dataset_id=ARGS.clean_survey_dataset_id)
