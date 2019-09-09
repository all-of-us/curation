import jinja2
import os

import resources
import datetime

CRON_DATE_FMT = '%-d of %B 00:00'
CRON_YAML_PATH = os.path.join(resources.base_path, 'cron.yaml')


def get_yesterday_expr():
    """
    Get a cron expression for yesterday at midnight

    :return: a str representation of yesterday
    """
    yesterday = datetime.datetime.today() - datetime.timedelta(days=1)
    return yesterday.strftime(CRON_DATE_FMT)


def render():
    """
    Render cron file

    :return: a str representation of the cron file
    """
    j2_env = jinja2.Environment(loader=jinja2.FileSystemLoader(resources.TEMPLATES_PATH))
    tpl = j2_env.get_template(resources.CRON_TPL_YAML)
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
