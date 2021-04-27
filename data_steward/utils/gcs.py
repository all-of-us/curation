# Python import
from io import BytesIO

# Third party imports
from google.api_core import page_iterator


def list_sub_prefixes(client, bucket, prefix):
    """
    List sub folders in folder specified by prefix

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


def retrieve_file_contents(client, bucket, prefix, file_name):
    """
    Retrieves contents of file in GCS as string

    :param client: GCS client
    :param bucket: GCS bucket name as string
    :param prefix: folder prefix for the file to read
    :param file_name: name of the file to read, with extension
    :return: string from the file
    """
    file_bytes = BytesIO()
    gcs_bucket = client.bucket(bucket)
    blob = gcs_bucket.blob(prefix + file_name)
    blob.download_to_file(file_bytes)
    file_bytes.seek(0)
    file_str = file_bytes.getvalue()
    return file_str


def retrieve_file_contents_as_list(client, bucket, prefix, file_name):
    """
    Retrieves contents of file in GCS as list of lines

    :param client: GCS client
    :param bucket: GCS bucket name as string
    :param prefix: folder prefix for the file to read
    :param file_name: name of the file to read, with extension
    :return: list of lines from the file
    """
    return retrieve_file_contents(client, bucket, prefix,
                                  file_name).split(b'\n')


def upload_file_to_gcs(client,
                       bucket,
                       prefix,
                       file_name,
                       file_obj,
                       rewind=False,
                       content_type='text/csv'):
    """
    Uploads file object as BytesIO string to GCS. Overwrites if it exists

    :param client: GCS client
    :param bucket: GCS bucket name as string
    :param prefix: folder prefix for the file to read
    :param file_name: name of the file to read, with extension
    :param file_obj: BytesIO object with content written
    :param rewind: seek to the top of file object. Defaults to False
    :param content_type: type of content being uploaded. Defaults to 'text/csv'
    """
    gcs_bucket = client.bucket(bucket)
    blob = gcs_bucket.blob(prefix + file_name)
    blob.upload_from_file(file_obj, rewind=rewind, content_type=content_type)
    return
