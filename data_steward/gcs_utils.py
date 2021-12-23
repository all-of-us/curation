"""
Wraps Google Cloud Storage JSON API (adapted from https://goo.gl/dRKiYz)
"""

import mimetypes
import os
from io import BytesIO

import googleapiclient.discovery
from deprecated import deprecated

from validation.app_errors import BucketDoesNotExistError

MIMETYPES = {
    'json': 'application/json',
    'woff': 'application/font-woff',
    'ttf': 'application/font-sfnt',
    'eot': 'application/vnd.ms-fontobject'
}
GCS_DEFAULT_RETRY_COUNT = 5


@deprecated(reason='use StorageClient().get_drc_bucket() instead')
def get_drc_bucket():
    result = os.environ.get('DRC_BUCKET_NAME')
    return result


@deprecated(reason='use StorageClient().get_hpo_bucket(str) instead')
def get_hpo_bucket(hpo_id):
    """
    Get the name of an HPO site's private bucket

    Empty/unset bucket indicates that the bucket is intentionally left blank and can be ignored
    :param hpo_id: id of the HPO site
    :return: name of the bucket
    """
    # TODO reconsider how to map bucket name
    bucket_env = 'BUCKET_NAME_' + hpo_id.upper()
    hpo_bucket_name = os.getenv(bucket_env)

    # App engine converts an env var set but left empty to be the string 'None'
    if not hpo_bucket_name or hpo_bucket_name.lower() == 'none':
        # should not use hpo_id in message if sent to end user.  If the
        # error is logged as a WARNING or higher, this will trigger a
        # GCP alert.
        raise BucketDoesNotExistError(
            f"Failed to fetch bucket '{hpo_bucket_name}' for hpo_id '{hpo_id}'",
            hpo_bucket_name)

    return hpo_bucket_name


def hpo_gcs_path(hpo_id):
    """
    Get the fully qualified GCS path where HPO files will be located
    :param hpo_id: the id for an HPO
    :return: fully qualified GCS path
    """
    bucket_name = get_hpo_bucket(hpo_id)
    return '/%s/' % bucket_name


@deprecated(
    reason='Discovery client is being replaced by gcloud.gcs.StorageClient()')
def create_service():
    return googleapiclient.discovery.build('storage', 'v1', cache={})


@deprecated(reason=(
    'see: https://googleapis.dev/python/storage/latest/client.html#google.cloud.storage.client.Client.list_blobs '
    'and https://googleapis.dev/python/storage/latest/blobs.html#google.cloud.storage.blob.Blob.metadata'
))
def list_bucket_dir(gcs_path):
    """
    Get metadata for each object within the given GCS path
    :param gcs_path: full GCS path (e.g. `/<bucket_name>/path/to/person.csv`)
    :return: list of metadata objects
    """
    service = create_service()
    gcs_path_parts = gcs_path.split('/')
    if len(gcs_path_parts) < 2:
        raise ValueError('%s is not a valid GCS path' % gcs_path)
    bucket = gcs_path_parts[0]
    prefix = '/'.join(gcs_path_parts[1:]) + '/'
    req = service.objects().list(bucket=bucket, prefix=prefix, delimiter='/')

    all_objects = []
    while req:
        resp = req.execute(num_retries=GCS_DEFAULT_RETRY_COUNT)
        items = [
            item for item in resp.get('items', []) if item['name'] != prefix
        ]
        all_objects.extend(items or [])
        req = service.objects().list_next(req, resp)
    return all_objects


@deprecated(
    reason=
    'See https://googleapis.dev/python/storage/latest/blobs.html#google.cloud.storage.blob.Blob.metadata'
)
def get_metadata(bucket, name, default=None):
    """
    Get the metadata for an object with the given name if it exists, return None otherwise
    :param bucket: the bucket to find the object
    :param name: the name of the object (i.e. file name)
    :param default: alternate value to return if object with the given name is not found
    :return: the object metadata if it exists, None otherwise
    """
    all_objects = list_bucket(bucket)
    for obj in all_objects:
        if obj['name'] == name:
            return obj
    return default


@deprecated(reason=(
    'See https://googleapis.dev/python/storage/latest/client.html#google.cloud.storage.client.Client.list_blobs '
    'and use gcloud.gcs.StorageClient() instead'))
def list_bucket(bucket):
    """
    Get metadata for each object within a bucket
    :param bucket: name of the bucket
    :return: list of metadata objects
    """
    service = create_service()
    req = service.objects().list(bucket=bucket)
    all_objects = []
    while req:
        resp = req.execute(num_retries=GCS_DEFAULT_RETRY_COUNT)
        all_objects.extend(resp.get('items', []))
        req = service.objects().list_next(req, resp)
    return all_objects


@deprecated(
    reason=
    'Use gcloud.gcs.StorageClient.list_sub_prefixes(bucket: str, prefix: str) instead'
)
def list_bucket_prefixes(gcs_path):
    """
    Get metadata for each object within the given GCS path
    :param gcs_path: GCS path upto folder (e.g. `/<bucket_name>/path/`)
    :return: list of prefixes (e.g. [`/<bucket_name>/path/a/`, `/<bucket_name>/path/b/`, `/<bucket_name>/path/c/`]
    """
    service = create_service()
    gcs_path_parts = gcs_path.split('/')
    if len(gcs_path_parts) < 2:
        raise ValueError('%s is not a valid GCS path' % gcs_path)
    bucket = gcs_path_parts[0]
    prefix = '/'.join(gcs_path_parts[1:]) + '/'
    req = service.objects().list(bucket=bucket, prefix=prefix, delimiter='/')

    all_objects = []
    while req:
        resp = req.execute(num_retries=GCS_DEFAULT_RETRY_COUNT)
        all_objects.extend(resp.get('prefixes', []))
        req = service.objects().list_next(req, resp)
    return all_objects


@deprecated(reason=(
    'See https://googleapis.dev/python/storage/latest/buckets.html#google.cloud.storage.bucket.Bucket.get_blob '
    'and use gcloud.gcs.StorageClient() instead'))
def get_object(bucket, name, as_text=True):
    """
    Download object from a bucket
    :param bucket: the bucket containing the file
    :param name: name of the file to download
    :param as_text: True if result should be decoded as text (default) otherwise bytes are returned
    :return: file contents
    """
    service = create_service()
    req = service.objects().get_media(bucket=bucket, object=name)
    out_file = BytesIO()
    downloader = googleapiclient.http.MediaIoBaseDownload(out_file, req)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    result_bytes = out_file.getvalue()
    out_file.close()
    if as_text:
        return result_bytes.decode()
    return result_bytes


@deprecated(reason=(
    'See https://googleapis.dev/python/storage/latest/blobs.html#google.cloud.storage.blob.Blob.upload_from_string '
    'and use gcloud.gcs.StorageClient() instead'))
def upload_object(bucket, name, fp):
    """
    Upload file to a GCS bucket
    :param bucket: name of the bucket
    :param name: name for the file
    :param fp: a file-like object containing file contents
    :return: metadata about the uploaded file
    """
    service = create_service()
    body = {'name': name}
    ext = name.split('.')[-1]
    if ext in MIMETYPES:
        mimetype = MIMETYPES[ext]
    else:
        (mimetype, encoding) = mimetypes.guess_type(name)
    media_body = googleapiclient.http.MediaIoBaseUpload(fp, mimetype)
    req = service.objects().insert(bucket=bucket,
                                   body=body,
                                   media_body=media_body)
    return req.execute(num_retries=GCS_DEFAULT_RETRY_COUNT)


@deprecated(reason=(
    'See https://googleapis.dev/python/storage/latest/blobs.html#google.cloud.storage.blob.Blob.delete '
    'and use gcloud.gcs.StorageClient() instead'))
def delete_object(bucket, name):
    """
    Delete an object from a bucket
    :param bucket: name of the bucket
    :param name: name of the file in the bucket
    :return: empty string
    """
    service = create_service()
    req = service.objects().delete(bucket=bucket, object=name)
    resp = req.execute(num_retries=GCS_DEFAULT_RETRY_COUNT)
    # TODO return something useful
    return resp


@deprecated(
    reason=
    'See https://googleapis.dev/python/storage/latest/buckets.html?highlight=copy#google.cloud.storage.bucket.Bucket.copy_blob'
)
def copy_object(source_bucket, source_object_id, destination_bucket,
                destination_object_id):
    """
    copies files from one place to another
    :returns: response of request
    """
    service = create_service()
    req = service.objects().copy(sourceBucket=source_bucket,
                                 sourceObject=source_object_id,
                                 destinationBucket=destination_bucket,
                                 destinationObject=destination_object_id,
                                 body=dict())
    return req.execute(num_retries=GCS_DEFAULT_RETRY_COUNT)
