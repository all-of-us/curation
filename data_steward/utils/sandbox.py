from utils.bq import list_datasets, create_dataset

SANDBOX_SUFFIX = 'sandbox'


def create_sandbox_dataset(project_id, dataset_id):
    """
    A helper function create a sandbox dataset if the sandbox dataset doesn't exist
    :param project_id: project_id
    :param dataset_id: any dataset_id
    :return: the sandbox dataset_id
    """
    sandbox_dataset_id = get_sandbox_dataset_id(dataset_id)
    friendly_name = f'Sandbox for {dataset_id}'
    description = f'Sandbox created for storing records affected by the cleaning rules applied to {dataset_id}'
    label_or_tag = {'label': '', 'tag': ''}
    create_dataset(project_id=project_id,
                   dataset_id=sandbox_dataset_id,
                   friendly_name=friendly_name,
                   description=description,
                   label_or_tag=label_or_tag,
                   overwrite_existing=False)

    return sandbox_dataset_id


def get_sandbox_dataset_id(dataset_id):
    """
    A helper function to create the sandbox dataset_id
    :param dataset_id: any dataset_id
    :return:
    """
    return '{dataset_id}_{sandbox_suffix}'.format(dataset_id=dataset_id,
                                                  sandbox_suffix=SANDBOX_SUFFIX)


def get_sandbox_table_name(table_namer, base_name):
    """
    A helper function to create a table in the sandbox dataset

    :param table_namer: the string value returned from the table_namer setter method
    :param base_name: the name of the cleaning rule
    :return: the concatenated table name
    """
    if table_namer and not table_namer.isspace():
        return f'{table_namer}_{base_name}'
    else:
        return base_name


def check_and_create_sandbox_dataset(project_id, dataset_id):
    """
    A helper function to check if sandbox dataset exisits. If it does not, it will create.

    :param project_id: the project_id that the dataset is in
    :param dataset_id: the dataset_id to verify
    :return: the sandbox dataset_name that either exists or was created
    """
    sandbox_dataset = get_sandbox_dataset_id(dataset_id)
    dataset_objs = list_datasets(project_id)
    datasets = [d.dataset_id for d in dataset_objs]

    if sandbox_dataset not in datasets:
        create_sandbox_dataset(project_id, dataset_id)
    return sandbox_dataset
