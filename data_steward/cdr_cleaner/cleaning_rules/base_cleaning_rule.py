"""
A base class for cleaning rules to extend and implement.

DC-718

This is a base class that all cleaning rules should extend.  This base class
should contain data relevant to each cleaning rule.  It's attributes and
functions should be used to ensure that new cleaning rules are added with
an adequate amount of information.  This is useful for things like, reporting
features.
"""
# Python imports
import logging
from abc import ABC, abstractmethod
from typing import List

# Third party imports
import googleapiclient
import oauth2client

# Project imports
import constants.cdr_cleaner.clean_cdr as cdr_consts

LOGGER = logging.getLogger(__name__)


class AbstractBaseCleaningRule(ABC):
    """
    Contains attributes and functions relevant to all cleaning rules.
    """
    string_list = List[str]

    def __init__(self):
        """
        Instantiate a cleaning rule with basic attributes.
        """
        super().__init__()

    @abstractmethod
    def get_query_dictionary_list(self, *args, **keyword_args):
        """
        Interface to return a list of query dictionaries.

        :returns:  a list of query dictionaries
        """
        pass

    @abstractmethod
    def print_queries(self, *args, **keyword_args):
        """
        Helper function to print the SQL a class geenerates.
        """
        pass


class BaseCleaningRule(AbstractBaseCleaningRule):
    """
    Contains attributes and functions relevant to all cleaning rules.
    """
    string_list = List[str]
    cleaning_class_list = List[AbstractBaseCleaningRule]

    def __init__(self,
                 jira_issue_numbers: string_list = None,
                 description: str = None,
                 affected_datasets: string_list = None,
                 project_id: str = None,
                 dataset_id: str = None,
                 sandbox_dataset_id: str = None,
                 depends_on: cleaning_class_list = None):
        """
        Instantiate a cleaning rule with basic attributes.

        Inheriting classes must set issue numbers, description and affected
        datasets.  As other tickets may affect the SQL of a cleaning rule,
        append them to the list of Jira Issues.
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
        self._project_id = project_id
        self._dataset_id = dataset_id
        self._sandbox_dataset_id = sandbox_dataset_id
        self._depends_on_classes = depends_on if depends_on else []

        super().__init__()

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
                        ('{} is expected to be a list of strings.  '
                         'offending list item {} is of type:  {}').format(
                             arg_name, list_item, type(list_item)))

    def __validate_string(self, arg, arg_name):
        """
        Validate string parameters.

        :param arg:  The actual argument value to validate is a string.
        :param arg_name:  The name of the variable being validated.  Used
            in error messages, if needed.

        :raises NotImplementedError if arguments are not set.
        :raises TypeError if arguments are not set to expected types
        """
        if arg is None:
            raise NotImplementedError(
                '{} cleaning rule must set {} variable'.format(
                    self.__class__.__name__, arg_name))

        if not isinstance(arg, str):
            raise TypeError(
                '{0} is expected to be a string.  offending {0}: <{1}> is of type:  {2}'
                .format(arg_name, arg, type(arg)))

    def __validate_arguments(self):
        """
        Validate arguments passed to the base class were set.

        :raises NotImplementedError if arguments are not set.
        :raises TypeError if arguments are not set to expected types
        """
        # validate jira_issue_numbers is a list of strings
        self.__validate_list_of_strings(self._jira_issue_numbers,
                                        'jira_issue_numbers')

        # validate affected datasets is a list of strings.
        self.__validate_list_of_strings(self._affected_datasets,
                                        'affected_datasets')

        # validate description is a string
        self.__validate_string(self._description, 'description')

        # validate project_id is a string
        self.__validate_string(self._project_id, 'project_id')

        # validate dataset_id is a string
        self.__validate_string(self._dataset_id, 'dataset_id')

        # validate sandbox_dataset_id is a string
        self.__validate_string(self._sandbox_dataset_id, 'sandbox_dataset_id')

        # depends_on_classes is allowed to be null,
        # so is not validated against None
        for clazz in self._depends_on_classes:
            if not isinstance(clazz, BaseCleaningRule):
                raise TypeError(
                    '{} is expected to inherit from BaseCleaningRule'.format(
                        self.__class__.__name__))

    def get_depends_on_classes(self):
        """
        Return the names of classes the rule depends on.
        """
        return self._depends_on_classes

    def get_jira_issue_numbers(self):
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

    def get_project_id(self):
        """
        Get the project id for this class instance.
        """
        return self._project_id

    def get_dataset_id(self):
        """
        Get the dataset id for this class instance.
        """
        return self._dataset_id

    def get_sandbox_dataset_id(self):
        """
        Get the sandbox dataset id for this class instance.
        """
        return self._sandbox_dataset_id

    def print_queries(self):
        """
        Helper function to print the SQL a class geenerates.

        If the inheriting class builds tables inside the
        get_query_dictionary_list function, the inheriting class will need
        to override this function.
        """
        try:
            query_list = self.get_query_dictionary_list()
        except (oauth2client.client.HttpAccessTokenRefreshError,
                googleapiclient.errors.HttpError, KeyError):
            LOGGER.exception("Cannot list queries for %s",
                             self.__class__.__name__)
            raise

        for query in query_list:
            LOGGER.info('Generated SQL Query:\n%s',
                        query.get(cdr_consts.QUERY, 'NO QUERY FOUND'))

    @abstractmethod
    def setup_query_execution(self, *args, **keyword_args):
        """
        Function to run any data upload options before executing a query.
        """
        pass
