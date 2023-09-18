# Project imports
from cdr_cleaner.cleaning_rules.sandbox_and_remove_pids import SandboxAndRemovePids

ISSUE_NUMBERS = ['DC3442']


class SandboxAndRemovePidsList(SandboxAndRemovePids):
    """
    Removes all participant data using a list of participants.
    """

    def __init__(self, project_id, dataset_id, sandbox_dataset_id):
        """
        Initialize the class with proper information.
        """

        desc = (
            'Sandbox and remove participant data from a list of participants.')

        super().__init__(issue_numbers=ISSUE_NUMBERS,
                         description=desc,
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id)
