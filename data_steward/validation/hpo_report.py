import jinja2

import resources


def render(**kwargs):
    j2_env = jinja2.Environment(loader=jinja2.FileSystemLoader(resources.TEMPLATES_PATH), trim_blocks=True)
    tpl = j2_env.get_template(resources.HPO_REPORT_HTML)
    html = tpl.render(**kwargs)
    return html
