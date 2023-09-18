# Project imports
from cdr_cleaner.cleaning_rules.sandbox_and_remove_pids import SandboxAndRemovePids
from constants.cdr_cleaner import clean_cdr as cdr_consts

ISSUE_NUMBERS = ['DC3442']


class SandboxAndRemovePidsList(SandboxAndRemovePids):
    """
    Removes all participant data using a list of participants.
    """

    def __init__(self, project_id, dataset_id, sandbox_dataset_id):
        """
        Initialize the class with proper information.

        Set the issue numbers, description and affected datasets. As other tickets may affect
        this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """

        desc = 'Sandbox and remove participant data from a list of participants.'

        super().__init__(issue_numbers=ISSUE_NUMBERS,
                         description=desc,
                         affected_datasets=[cdr_consts.COMBINED],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         affected_tables=[])


def parse_args():
    """
    This function expands the default argument list defined in cdr_cleaner.args_parser
    :return: an expanded argument list object
    """
    pass
