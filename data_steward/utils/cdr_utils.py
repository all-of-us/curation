import json
import subprocess


def get_project_id(key_file):
    """
    gets project_id from the google credentials file.
    :param key_file: path to service account key file
    :return: project_id in string format
    """
    with open(key_file) as key:
        obj = json.load(key)
    return obj["project_id"]


def get_git_tag():
    """
    gets latest git tag.
    :return: git tag in string format
    """
    git_tag = subprocess.check_output(
        ["git", "describe", "--abbrev=0", "--tags"]).strip().decode()
    return git_tag
