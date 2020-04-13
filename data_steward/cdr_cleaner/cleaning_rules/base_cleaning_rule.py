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
from typing import List, NewType

# Third party imports
from googleapiclient.errors import HttpError
from oauth2client.client import HttpAccessTokenRefreshError

# Project imports
import constants.cdr_cleaner.clean_cdr as cdr_consts

LOGGER = logging.getLogger(__name__)

query_spec = NewType('QuerySpec', {})
query_spec_list = List[query_spec]


class AbstractBaseCleaningRule(ABC):
    """
    Contains attributes and functions relevant to all cleaning rules.

    Anything that should be applied to all cleaning rules can be defined here.
    """
    string_list = List[str]

    def __init__(self):
        """
        Instantiate a cleaning rule with basic attributes.
        """
        super().__init__()

    @abstractmethod
    def setup_rule(self, *args, **keyword_args):
        """
        Load required resources prior to executing cleaning rule queries.

        Method to run data upload options before executing the first cleaning
        rule of a class.  For example, if your class requires loading a static
        table, that load operation should be defined here.  It SHOULD NOT BE
        defined as part of get_query_specs().
        """
        pass

    @abstractmethod
    def get_query_specs(self, *args, **keyword_args) -> query_spec_list:
        """
        Interface to return a list of query dictionaries.

        :returns:  a list of query dictionaries.  Each dictionary specifies
            the query to execute and how to execute.  The dictionaries are
            stored in list order and returned in list order to maintain
            an ordering.
        """
        pass

    @abstractmethod
    def log_queries(self, *args, **keyword_args):
        """
        Helper function to print the SQL a class generates.
        """
        pass


class BaseCleaningRule(AbstractBaseCleaningRule):
    """
    Contains attributes and functions relevant to all cleaning rules.
    """
    string_list = List[str]
    cleaning_class_list = List[AbstractBaseCleaningRule]

    def __init__(self,
                 issue_numbers: string_list = None,
                 issue_urls: string_list = None,
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

        :param issue_numbers:  a list of strings indicating jira issues
            that impact the SQL a cleaning rule creates.  If the issue is to
            fix a bug that doesn't impact the SQL, this would not need to be
            updated.  If the issue is to change the SQL to do more, less, or
            behave differently, that jira issue number should be appended as a
            string to this list.  DO NOT REMOVE ORIGINAL ISSUE NUMBERS!  THEY
            ARE HISTORY AND USED FOR REPORTS!
        :param issue_urls:  a list of string urls connecting to tickets.  They
            identify issues that impact the SQL a cleaning rule creates.  If the
            issue is to fix a bug that doesn't impact the created SQL, this should
            not be updated.  If the issue is to change the SQL to do more, less,
            or behave differently, the issue url should be appended as a string
            to this list.  DO NOT REMOVE ORIGINAL URLS!  THEY ARE HISTORY AND
            USED FOR REPORTS!
        :param description:  An explanation describing the intent of the
            cleaning rule. i.e. why does the rule exist, what does it accomplish.
        :param affected_datasets:  a list of strings naming the types of
            datasets this rule is expected to impact, (e.g. rdr, unioned_ehr,
            combined).  This info can be used as part of a report to ensure
            cleaning rules are running when and where they were designed to run.
        :param depends_on:  a list of rules a cleaning rule depends on when
            running.  Right now, it is used for reporting purposes, but the
            scope may expand in the future.  Default is an empty list.
        """
        self._issue_numbers = issue_numbers
        self._description = description
        self._affected_datasets = affected_datasets
        self._project_id = project_id
        self._dataset_id = dataset_id
        self._sandbox_dataset_id = sandbox_dataset_id
        self._issue_urls = issue_urls if issue_urls else []
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
        # validate issue_numbers is a list of strings
        self.__validate_list_of_strings(self._issue_numbers, 'issue_numbers')

        # validate issue_urls is a list of strings
        self.__validate_list_of_strings(self._issue_urls, 'issue_urls')

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

        # depends_on_classes is allowed to be unset,  defaults to empty list
        for clazz in self._depends_on_classes:
            message = None
            try:
                if not issubclass(clazz, BaseCleaningRule):
                    message = ('{} is expected to inherit from BaseCleaningRule'
                               .format(clazz.__name__))
                    raise TypeError(message)
            except TypeError:
                if message:
                    raise TypeError(message)
                else:
                    message = ('{} is not a class.  depends_on takes a list of '
                               'classes that inherit from BaseCleaningRule'.
                               format(clazz))
                    raise TypeError(message)

    def get_depends_on_classes(self):
        """
        Return the names of classes the rule depends on.
        """
        return self._depends_on_classes

    def get_issue_urls(self):
        """
        Return the issue_urls instance variable.
        """
        return self._issue_urls

    def get_issue_numbers(self):
        """
        Return the issue_numbers instance variable.
        """
        return self._issue_numbers

    def get_description(self):
        """
        Get the common language explanation of the intent of a cleaning rule.
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

    def log_queries(self):
        """
        Helper function to print the SQL a class generates.

        If the inheriting class builds tables inside the
        get_query_specs function, the inheriting class will need
        to override this function.
        """
        try:
            query_list = self.get_query_specs()
        except (KeyError, HttpAccessTokenRefreshError, HttpError) as err:
            LOGGER.exception("Cannot list queries for %s",
                             self.__class__.__name__)
            raise

        for query in query_list:
            LOGGER.info('Generated SQL Query:\n%s',
                        query.get(cdr_consts.QUERY, 'NO QUERY FOUND'))
