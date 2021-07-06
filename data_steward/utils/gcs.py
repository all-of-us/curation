# Python import

# Third party imports
from google.api_core import page_iterator


def list_sub_prefixes(client, bucket, prefix):
    """
    List sub folders in folder specified by prefix

    SO link: https://stackoverflow.com/a/59008580
    :param client: GCS client
    :param bucket: GCS bucket name as string
    :param prefix: path to directory to look into e.g. a/b/c/
    :return: list of strings of sub-directories e.g. [a/b/c/v1/, a/b/c/v2/]
    """
    if not prefix.endswith('/'):
        prefix += '/'

    extra_params = {"projection": "noAcl", "prefix": prefix, "delimiter": '/'}

    path = f"/b/{bucket}/o"

    pg_iterator = page_iterator.HTTPIterator(
        client=client,
        api_request=client._connection.api_request,
        path=path,
        items_key='prefixes',
        item_to_value=lambda _, x: x,
        extra_params=extra_params,
    )
    return list(pg_iterator)
