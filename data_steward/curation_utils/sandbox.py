# Python imports
from collections import OrderedDict

# Project Imports
from common import JINJA_ENV

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


def create_sandbox_dataset(client, dataset_id):
    """
    A helper function create a sandbox dataset if the sandbox dataset doesn't exist

    :param project_id: a BigQueryClient
    :param dataset_id: any dataset_id
    :return: the sandbox dataset_id
    """
    sandbox_dataset_id = get_sandbox_dataset_id(dataset_id)
    friendly_name = f'Sandbox for {dataset_id}'
    description = f'Sandbox created for storing records affected by the cleaning rules applied to {dataset_id}'
    label_or_tag = {'label': '', 'tag': ''}
    sandbox_dataset = client.define_dataset(dataset_id=sandbox_dataset_id,
                                            description=description,
                                            label_or_tag=label_or_tag)
    sandbox_dataset.friendly_name = friendly_name
    client.create_dataset(sandbox_dataset, exists_ok=False)
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
    return base_name


def check_and_create_sandbox_dataset(client, dataset_id):
    """
    A helper function to check if sandbox dataset exisits. If it does not, it will create.

    :param project_id: a BigQueryClient
    :param dataset_id: the dataset_id to verify
    :return: the sandbox dataset_name that either exists or was created
    """
    sandbox_dataset = get_sandbox_dataset_id(dataset_id)
    dataset_objs = list(client.list_datasets())
    datasets = [d.dataset_id for d in dataset_objs]

    if sandbox_dataset not in datasets:
        create_sandbox_dataset(client, dataset_id)
    return sandbox_dataset


def get_sandbox_labels_string(src_dataset_name,
                              class_name,
                              table_tag,
                              shared_lookup=False):
    """
    A helper function that formats a set of labels for BigQuery

    :param str src_dataset_name: A dataset name
    :param str class_name: A class name
    :param str table_tag: A table tag
    :param bool shared_lookup: A boolean that indicates if a table is a shared lookup, defaults to False
    :raises ValueError: Raises if one of first three arguments is whitespace or None
    :raises ValueError: Raises if the `shared_lookup` is not a boolean
    :return: A formatted string of the arguments concatenated as labels
    :rtype: str
    """
    string_labels = ['src_dataset', 'class_name', 'table_tag']
    bool_labels = ['shared_lookup']

    labels = OrderedDict([('src_dataset', src_dataset_name),
                          ('class_name', class_name), ('table_tag', table_tag),
                          ('shared_lookup', shared_lookup)])

    for label in string_labels:
        value = labels[label]
        if not value or value.isspace():
            raise ValueError(
                f"Label '{label}' requires a non-empty string. Received the value '{value}'."
            )

    for label in bool_labels:
        value = labels[label]

        if type(value) != bool:
            raise ValueError(
                f"Label '{label}' requires a boolean value of True or False. Received the value '{value}'."
            )

        #Convert bools to lowered strings for BQ
        labels[label] = str(value).lower()

    return TABLE_LABELS_STRING.render(labels=labels)


def get_sandbox_table_description_string(description):
    """
    A helper function that returns a formatted description for BigQuery

    :param str description: A table description
    :return: A formatted table description
    :rtype: str
    """

    if not description or description.isspace():
        raise ValueError(
            f"Description must be a non-empty string. Received the value '{description}'."
        )

    return TABLE_DESCRIPTION_STRING.render(description=description)


def get_sandbox_options(dataset_name,
                        class_name,
                        table_tag,
                        desc,
                        shared_lookup=False):
    """
    A function that assembles a BigQuery table options clause from labels and descriptions

    :param str dataset_name: A dataset name
    :param str class_name: A class name
    :param str table_tag: A table tag
    :param str desc: A table description
    :param bool shared_lookup: A boolean that indicates if a table is a shared lookup, defaults to False
    :return: A BigQuery table options clause
    :rtype: str
    """
    labels_text = get_sandbox_labels_string(dataset_name, class_name, table_tag,
                                            shared_lookup)
    description_text = get_sandbox_table_description_string(desc)

    contents = ',\n'.join([description_text, labels_text])
    return TABLE_OPTIONS_CLAUSE.render(contents=contents)
