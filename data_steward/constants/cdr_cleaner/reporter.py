"""
A package to generate a csv file type report for cleaning rules.
"""
FIELDS_PROPERTIES_MAP = {
    'jira-issues': 'issue_numbers',
    'description': 'description',
    'affected-datasets': 'affected_datasets',
    'affected-tables': 'affected_tables',
    'issue-urls': 'issue_urls',
    'dependencies': 'depends_on_classes',
}

FIELDS_METHODS_MAP = {
    'sql': 'get_query_specs',
    'sandbox-tables': 'get_sandbox_tablenames',
}

CLASS_ATTRIBUTES_MAP = {
    'name': '__class__.__name__',
    'module': '__class__.__module__',
}
