# Project imports
from cdr_cleaner.cleaning_rules.sandbox_and_remove_pids import SandboxAndRemovePids


class SandboxAndRemovePidsList(SandboxAndRemovePids):
    """
    Removes all participant data using a list of participants.
    """

    def __init__(self, project_id, dataset_id, sandbox_dataset_id):
        """
        Initialize the class with proper information.
        """
        super().__init__()