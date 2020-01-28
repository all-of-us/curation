from bq_utils import create_dataset
from constants import bq_utils as bq_consts
import re

SANDBOX_SUFFIX = 'sandbox'


def create_sandbox_dataset(project_id, dataset_id):
    """
    A helper function create a sandbox dataset if the sandbox dataset doesn't exist
    :param project_id: project_id
    :param dataset_id: any dataset_id
    :return: the sandbox dataset_id
    """
    sandbox_dataset_id = get_sandbox_dataset_id(dataset_id)
    friendly_name = 'Sandbox for {dataset_id}'.format(dataset_id=dataset_id)
    description = 'Sandbox created for storing records affected by the cleaning rules applied to {dataset_id}'.format(
        dataset_id=dataset_id)
    create_dataset(project_id=project_id,
                   dataset_id=sandbox_dataset_id,
                   friendly_name=friendly_name,
                   description=description,
                   overwrite_existing=bq_consts.FALSE)

    return sandbox_dataset_id


def get_sandbox_dataset_id(dataset_id):
    """
    A helper function to create the sandbox dataset_id
    :param dataset_id: any dataset_id
    :return:
    """
    return '{dataset_id}_{sandbox_suffix}'.format(dataset_id=dataset_id,
                                                  sandbox_suffix=SANDBOX_SUFFIX)


def get_sandbox_table_name(dataset_id, rule_name):
    """
    A helper function to create a table in the sandbox dataset

    :param dataset_id: the dataset_id to which the rule is applied
    :param rule_name: the name of the cleaning rule
    :return: the concatenated table name
    """
    return '{dataset_id}_{rule_name}'.format(dataset_id=dataset_id,
                                             rule_name=re.sub(
                                                 r'\W', '_', rule_name))
