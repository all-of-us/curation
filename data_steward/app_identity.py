import os

PROJECT_ID = 'GOOGLE_CLOUD_PROJECT'


def get_application_id():
    """
    Get the associated Google Cloud Project ID

    :return:
    """
    # NOTE: Google interchangeably refers to this identifier as application_id or project_id
    project_id = os.getenv(PROJECT_ID, '')

    # ensure project id is a non-empty string
    if project_id:
        return project_id

    # in all other cases, OBJECTION!
    raise RuntimeError(f'{PROJECT_ID} is not set.  Set and retry.')
