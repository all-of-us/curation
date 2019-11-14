import jinja2
import os

import resources
import datetime
from io import open

CRON_DATE_FMT = '%-d of %b 00:00'
CRON_YAML_PATH = os.path.join(resources.base_path, 'cron.yaml')
# TODO add flag to force run all sites with 20 min diff, while excluding
# some sites using "latest site upload times" or using the archived folders for them


def get_yesterday_expr():
    """
    Get a cron expression for yesterday at midnight

    :return: a str representation of yesterday
    """
    yesterday = datetime.datetime.today() - datetime.timedelta(days=1)
    return yesterday.strftime(CRON_DATE_FMT).lower()


def render():
    """
    Render cron file

    :return: a str representation of the cron file
    """
    j2_env = jinja2.Environment(loader=jinja2.FileSystemLoader(resources.TEMPLATES_PATH))
    tpl = j2_env.get_template(resources.CRON_TPL_YAML)
    # TODO obtain cron urls from validation.main/app_base.yaml instead of through template
    hpos = resources.hpo_csv()
    yesterday = get_yesterday_expr()
    result = tpl.render(hpos=hpos, yesterday=yesterday)
    return result


def generate():
    """
    Generate and save cron.yaml

    :raises IOError if the cron.yaml file already exists
    """
    if os.path.exists(CRON_YAML_PATH):
        raise IOError('The file "%s" already exists. Please remove or rename it and retry.' % CRON_YAML_PATH)
    else:
        cron_yaml = render()
        with open(CRON_YAML_PATH, 'w') as cron_yaml_fp:
            cron_yaml_fp.write(cron_yaml)


if __name__ == '__main__':
    generate()
