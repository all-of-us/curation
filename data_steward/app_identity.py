import os

PROJECT_ID = 'GOOGLE_CLOUD_PROJECT'


def get_application_id():
    """
    Get the associated Google Cloud Project ID

    :return:
    """
    # NOTE: Google interchangeably refers to this identifier as application_id or project_id
    project_id = os.environ.get(PROJECT_ID)

    if project_id:
        return project_id
    else:
        raise RuntimeError('{} is not set.  Set and retry.'.format(PROJECT_ID))
