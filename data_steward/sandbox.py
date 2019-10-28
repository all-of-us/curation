from bq_utils import create_dataset
from constants import bq_utils as bq_consts

SANDBOX_SUFFIX = 'sandbox'


def create_sandbox_dataset(project_id, dataset_id):
    """
    A helper function create a sandbox dataset if the sandbox dataset doesn't exist
    :param project_id: project_id
    :param dataset_id: any dataset_id
    :return:
    """
    sandbox_dataset_id = get_sandbox_dataset_id(dataset_id)
    friendly_name = 'Sandbox for {dataset_id}'.format(dataset_id=dataset_id)
    return create_dataset(project_id=project_id,
                          dataset_id=sandbox_dataset_id,
                          friendly_name=friendly_name,
                          overwrite_existing=bq_consts.FALSE)


def get_sandbox_dataset_id(dataset_id):
    """
    A helper function to create the sandbox dataset_id
    :param dataset_id: any dataset_id
    :return:
    """
    return '{dataset_id}_{sandbox_suffix}'.format(dataset_id=dataset_id, sandbox_suffix=SANDBOX_SUFFIX)
