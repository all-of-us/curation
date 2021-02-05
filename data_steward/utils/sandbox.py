from utils.bq import list_datasets, create_dataset
from common import JINJA_ENV
from collections import OrderedDict

SANDBOX_SUFFIX = 'sandbox'

TABLE_LABELS_STRING = JINJA_ENV.from_string("""
    {%- if labels %}
    labels=[
        {% for label_key, label_value in labels.items() %}
        ("{{ label_key }}", "{{ label_value }}"){{ ", " if not loop.last}}
        {% endfor %}
    ]
    {%- endif %}
""")

TABLE_DESCRIPTION_STRING = JINJA_ENV.from_string("""
    {%- if description %}
    description="{{ description }}"
    {%- endif %}
""")

TABLE_OPTIONS_CLAUSE = JINJA_ENV.from_string("""
{%- block options %}
OPTIONS(
{{ contents }}
)
{%- endblock %}
""")


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
    return f'{table_namer}_{base_name}'


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


def get_sandbox_labels_string(src_dataset_name,
                              class_name,
                              table_tag,
                              shared_lookup=False):
    STRING_LABELS = ['src_dataset', 'class_name', 'table_tag']
    BOOL_LABELS = ['shared_lookup']

    labels = OrderedDict([('src_dataset', src_dataset_name),
                          ('class_name', class_name), ('table_tag', table_tag),
                          ('shared_lookup', shared_lookup)])

    for label in STRING_LABELS:
        value = labels[label]
        if not value or value.isspace():
            raise ValueError(
                f"Label '{label}' requires a non-empty string. Received the value '{value}'."
            )

    for label in BOOL_LABELS:
        value = labels[label]

        if type(value) != bool:
            raise ValueError(
                f"Label '{label}' requires a boolean value of True or False. Received the value '{value}'."
            )

    return TABLE_LABELS_STRING.render(labels=labels)


def get_sandbox_table_description_string(description):
    return TABLE_DESCRIPTION_STRING.render(description=description)


def get_sandbox_options(dataset_name,
                        class_name,
                        table_tag,
                        desc,
                        shared_lookup=False):
    labels_text = get_sandbox_labels_string(dataset_name, class_name, table_tag,
                                            shared_lookup)
    description_text = get_sandbox_table_description_string(desc)

    contents = ',\n'.join([description_text, labels_text])
    return TABLE_OPTIONS_CLAUSE.render(contents=contents)
