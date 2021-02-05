"""
A package to generate a csv file type report for cleaning rules.
"""
NAME = 'name'
MODULE = 'module'
UNKNOWN = 'unknown'
DESCRIPTION = 'description'
SQL = 'sql'
QUERY = 'query'

FIELDS_PROPERTIES_MAP = {
    'jira-issues': 'issue_numbers',
    DESCRIPTION: 'description',
    'affected-datasets': 'affected_datasets',
    'affected-tables': 'affected_tables',
    'issue-urls': 'issue_urls',
    'dependencies': 'depends_on_classes',
}

FIELDS_METHODS_MAP = {
    SQL: 'get_query_specs',
    'sandbox-tables': 'get_sandbox_tablenames',
}

CLASS_ATTRIBUTES_MAP = {
    NAME: '__class__.__name__',
    MODULE: '__class__.__module__',
}
