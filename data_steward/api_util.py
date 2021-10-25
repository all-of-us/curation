"""Utilities used by the API definition, and authentication/authorization/roles."""
import logging
from flask import request
from werkzeug.exceptions import Forbidden

SCOPE = 'https://www.googleapis.com/auth/userinfo.email'


def auth_required_cron(func):
    """A decorator that ensures that the user is a cron job."""

    def wrapped(*args, **kwargs):
        check_cron()
        return func(*args, **kwargs)

    return wrapped


def check_cron():
    """Raises Forbidden if the current user is not a cron job."""
    if request.headers.get('X-Appengine-Cron'):
        logging.info('Appengine-Cron ALLOWED for cron endpoint.')
        return
    logging.info(f'User NOT ALLOWED for cron endpoint')
    raise Forbidden()
