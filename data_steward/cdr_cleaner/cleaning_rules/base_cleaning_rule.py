"""
A base class for cleaning rules to extend and implement.

DC-???

This is a base class that all cleaning rules should extend.  This base class
should contain data relevant to each cleaning rule.  It's attributes and
functions should be used to ensure that new cleaning rules are added with
an adequate amount of information.  This is useful for things like, reporting
features.
"""
# Python imports
from typing import List

# Third party imports

# Project imports


class BaseCleaningRule:
    """
    Contains attributes and functions relevant to all cleaning rules.
    """
    string_list = List[str]

    def __init__(self,
                 jira_issue_numbers: string_list = None,
                 description: str = None,
                 affected_datasets: string_list = None):
        """
        Instantiate a cleaning rule with basic attributes.

        Inheriting classes must set issue numbers, description and affected
        datasets.  As other tickets may affect the SQL of a cleaning rule,
        add them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!

        :param jira_issue_numbers:  a list of strings inidcating jira issues
            impacting a cleaning rule.
        :param description:  a written description of the cleaning rule.
            describes why the rule exists and what it hopes to accomplish.
        :param affected_datasets:  a list of strings describing the types of
            datasets this rule is expected to impact, (e.g. rdr, unioned_ehr, combined)
        """
        self._jira_issue_numbers = jira_issue_numbers
        self._description = description
        self._affected_datasets = affected_datasets

        self.__validate_arguments()

    def __validate_list_of_strings(self, arg, arg_name):
        """
        Validate the given argument is a list of strings.

        :param arg:  the argument to validate as a list of strings
        :param arg_name:  the argument name that was passed.  useful for error messages

        :raises NotImplementedError if arguments are not set.
        :raises TypeError if arguments are not set to expected types
        """
        if arg is None:
            raise NotImplementedError(
                '{} cleaning rule must set {} variable'.format(
                    self.__class__.__name__, arg_name))

        if not isinstance(arg, list):
            raise TypeError(
                '{} is expected to be a list of strings.  offending type is:  {}'
                .format(arg_name, type(arg)))
        else:
            for list_item in arg:
                if not isinstance(list_item, str):
                    raise TypeError(
                        '{} is expected to be a list of strings.  offending list item {} is of type:  {}'
                        .format(arg_name, list_item, type(list_item)))

    def __validate_arguments(self):
        """
        Validate arguments passed to the base class were set.

        :raises NotImplementedError if arguments are not set.
        :raises TypeError if arguments are not set to expected types
        """
        # validate jira_issue_numbers is a list of strings
        self.__validate_list_of_strings(self._jira_issue_numbers,
                                        'jira_issue_numbers')

        # validate description is a string
        if self._description is None:
            raise NotImplementedError(
                '{} cleaning rule must set description variable'.format(
                    self.__class__.__name__))

        if not isinstance(self._description, str):
            raise TypeError(
                'description is expected to be a string.  offending description: <{}> is of type:  {}'
                .format(self._description, type(self._description)))

        # validate affected datasets is a list of strings.
        self.__validate_list_of_strings(self._affected_datasets,
                                        'affected_datasets')

    def get_issue_numbers(self):
        """
        Return the jira_issue_numbers instance variable.
        """
        return self._jira_issue_numbers

    def get_description(self):
        """
        Get the common language description of a cleaning rule.
        """
        return self._description

    def get_affected_datasets(self):
        """
        Get the list of datasets a rule affects.
        """
        return self._affected_datasets

    def get_query_dictionary_list(self, *args, **keyword_args):
        """
        Interface to return a list of query dictionaries.

        Interface function each inheriting class should override.  Returns a
        list of query dictionaries to describe each query that should
        execute.

        :returns:  a list of query dictionaries
        :raises: NotImplementedError
        """
        raise NotImplementedError(
            '{} has not implemented get_query_dictionary_list.  Must be implemented.'
            .format(self.__class__.__name__))
