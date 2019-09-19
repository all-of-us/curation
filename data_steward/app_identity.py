import os

GAE_APPLICATION = 'GAE_APPLICATION'


def get_application_id():
    """
    Get the ID of the app engine application
    :return:
    """
    return os.environ.get(GAE_APPLICATION)
