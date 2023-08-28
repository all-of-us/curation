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
import re
from abc import ABC, abstractmethod
from typing import List, NewType

# Third party imports
from googleapiclient.errors import HttpError
from oauth2client.client import HttpAccessTokenRefreshError

# Project imports
import constants.cdr_cleaner.clean_cdr as cdr_consts
from utils.sandbox import get_sandbox_table_name, get_sandbox_options
from common import JINJA_ENV

LOGGER = logging.getLogger(__name__)

query_spec = NewType('QuerySpec', {})
query_spec_list = List[query_spec]

# A query for dropping all empty sandbox tables
DROP_EMPTY_SANDBOX_TABLES_QUERY = JINJA_ENV.from_string("""
DECLARE i INT64 DEFAULT 0;
DECLARE tables DEFAULT (
  SELECT
    ARRAY_AGG(FORMAT("`%s.%s.%s`", project_id, dataset_id, table_id))
  FROM
    `{{project}}.{{dataset}}.__TABLES__`
  WHERE
    row_count = 0 AND table_id IN ({{table_ids}}));

LOOP
  SET i = i + 1;
  IF i > ARRAY_LENGTH(tables) THEN 
    LEAVE; 
  END IF;
  EXECUTE IMMEDIATE '''DROP TABLE ''' || tables[ORDINAL(i)];
END LOOP
""")


def get_delete_empty_sandbox_tables_queries(project_id, sandbox_dataset_id,
                                            sandbox_tablenames):
    """
    Generate the query that drop the empty sandbox tables
    
    :param project_id: 
    :param sandbox_dataset_id: 
    :param sandbox_tablenames: 
    :return: 
    """

    # If there is not sandbox tables associated with the cleaning rule, return an empty list
    if not sandbox_tablenames:
        return list()

    table_ids = ','.join(
        map(lambda sandbox_table: f'"{sandbox_table}"', sandbox_tablenames))
    return [{
        cdr_consts.QUERY:
            DROP_EMPTY_SANDBOX_TABLES_QUERY.render(project=project_id,
                                                   dataset=sandbox_dataset_id,
                                                   table_ids=table_ids),
        cdr_consts.DESTINATION_DATASET:
            sandbox_dataset_id
    }]


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
    def setup_rule(self, client, *args, **keyword_args):
        """
        Load required resources prior to executing cleaning rule queries.

        Method to run data upload options before executing the first cleaning
        rule of a class.  For example, if your class requires loading a static
        table, that load operation should be defined here.  It SHOULD NOT BE
        defined as part of get_query_specs().
        """
        pass

    @abstractmethod
    def setup_validation(self, client, *args, **keyword_args):
        """
        Run required steps for validation setup

        Method to run to setup validation on cleaning rules that will be updating or deleting the values.
        For example:
        if your class updates all the datetime fields you should be implementing the
        logic to get the initial list of values which adhere to a condition we are looking for.

        if your class deletes a subset of rows in the tables you should be implementing
        the logic to get the row counts of the tables prior to applying cleaning rule

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
    def validate_rule(self, client, *args, **keyword_args):
        """
        Validates the cleaning rule which deletes or updates the data from the tables

        Method to run validation on cleaning rules that will be updating the values.
        For example:
        if your class updates all the datetime fields you should be implementing the
        validation that checks if the date time values that needs to be updated no
        longer exists in the table.

        if your class deletes a subset of rows in the tables you should be implementing
        the validation that checks if the count of final row counts + deleted rows
        should be equal to initial row counts of the affected tables.

        Raises RunTimeError if the validation fails.
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
    TABLE_COUNT_QUERY = ''' SELECT COALESCE(COUNT(*), 0) AS row_count FROM `{dataset}.{table}` '''

    def __init__(self,
                 issue_numbers: string_list = None,
                 issue_urls: string_list = None,
                 description: str = None,
                 affected_datasets: string_list = None,
                 project_id: str = None,
                 dataset_id: str = None,
                 sandbox_dataset_id: str = None,
                 depends_on: cleaning_class_list = None,
                 affected_tables: List = None,
                 table_namer: str = None,
                 table_tag: str = None,
                 run_for_synthetic: bool = False):
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
        :param affected_tables: a list of tables that are affected by
            running this cleaning rule.
        :param table_namer: string used to help programmatically create
            sandbox table names
        :param table_tag: string used to create a label in the sandbox options
        """
        self._issue_numbers = issue_numbers
        self._description = description
        self._affected_datasets = affected_datasets
        self._project_id = project_id
        self._dataset_id = dataset_id
        self._sandbox_dataset_id = sandbox_dataset_id
        self._issue_urls = issue_urls if issue_urls else []
        self._depends_on_classes = depends_on if depends_on else []
        self._affected_tables = affected_tables
        self._table_namer = self.table_namer = table_namer
        self._table_tag = table_tag
        self._run_for_synthetic = run_for_synthetic

        # fields jinja template
        self.fields_templ = JINJA_ENV.from_string("""
            {{name}} {{col_type}} {{mode}} OPTIONS(description="{{desc}}")
        """)

        super().__init__()

        self.__validate_arguments()

    def __validate_argument(self, arg, arg_name, arg_type):
        """
        Validate the given argument has a value of the type specified.

        Prevents a lot of repetitive code.

        :param arg:  the argument object to validate as the type provided in arg_type
        :param arg_name:  the argument name that was passed for validation.  useful for error messages
        :param arg_type:  the expected type of the argument

        :raises NotImplementedError if arguments are not set.
        :raises TypeError if arguments are not set to expected types
        """
        if arg is None:
            raise NotImplementedError(
                f'{self.__class__.__name__} cleaning rule must set {arg_name} variable'
            )

        if not isinstance(arg, arg_type):
            raise TypeError(
                f'{arg_name} is expected to be a {str(arg_type)}.  offending {arg_name}: <{arg}> is of type:  {type(arg)}'
            )

    def __validate_list_of_strings(self, arg, arg_name):
        """
        Validate the given argument is a list of strings.

        :param arg:  the argument to validate as a list of strings
        :param arg_name:  the argument name that was passed.  useful for error messages

        :raises NotImplementedError if arguments are not set.
        :raises TypeError if arguments are not set to expected types
        """
        self.__validate_argument(arg, arg_name, list)

        # if no errors are raised in validating the argurment is a list, then validate each
        # list item is a string
        for list_item in arg:
            if not isinstance(list_item, str):
                raise TypeError((
                    f'{arg_name} is expected to be a list of strings.  '
                    f'offending list item {list_item} is of type:  {type(list_item)}'
                ))

    def __validate_string(self, arg, arg_name):
        """
        Validate string parameters.

        :param arg:  The actual argument value to validate is a string.
        :param arg_name:  The name of the variable being validated.  Used
            in error messages, if needed.

        :raises NotImplementedError if arguments are not set.
        :raises TypeError if arguments are not set to expected types
        """
        self.__validate_argument(arg, arg_name, str)

    def __validate_bool(self, arg, arg_name):
        """
        Validate boolean parameters are boolean.

        :param arg:  The actual argument value to validate is a string.
        :param arg_name:  The name of the variable being validated.  Used
            in error messages, if needed.

        :raises NotImplementedError if arguments are not set.
        :raises TypeError if arguments are not set to expected types
        """
        self.__validate_argument(arg, arg_name, bool)

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

        self.__validate_bool(self._run_for_synthetic, 'run_for_synthetic')

    @property
    def depends_on_classes(self):
        """
        Return the names of classes the rule depends on.
        """
        return self._depends_on_classes

    @property
    def issue_urls(self):
        """
        Return the issue_urls instance variable.
        """
        return self._issue_urls

    @property
    def issue_numbers(self):
        """
        Return the issue_numbers instance variable.
        """
        return self._issue_numbers

    @property
    def description(self):
        """
        Get the common language explanation of the intent of a cleaning rule.
        """
        return self._description

    @property
    def affected_datasets(self):
        """
        Get the list of datasets a rule affects.
        """
        return self._affected_datasets

    @property
    def project_id(self):
        """
        Get the project id for this class instance.
        """
        return self._project_id

    @property
    def dataset_id(self):
        """
        Get the dataset id for this class instance.
        """
        return self._dataset_id

    @property
    def sandbox_dataset_id(self):
        """
        Get the sandbox dataset id for this class instance.
        """
        return self._sandbox_dataset_id

    @property
    def affected_tables(self):
        """
        Method to get tables that will be modified by the cleaning rule.
        If getting the tables_affected dynamically logic needs to be
        implemented in this method.
        """
        return self._affected_tables

    @property
    def table_namer(self):
        """
        Get the table name of the sandbox for this class instance.
        """
        return self._table_namer

    @property
    def table_tag(self):
        """
        Get the table tag of the sandbox for this class instance.
        """
        return self._table_tag

    @property
    def run_for_synthetic(self):
        """
        Get the command to run a rule for synthetic data.
        """
        return self._run_for_synthetic

    @affected_tables.setter
    def affected_tables(self, affected_tables):
        """
        Set the affected_tables for this class instance, raises error if affected_tables is not a list
        """
        if affected_tables:
            if not isinstance(affected_tables, list):
                raise TypeError('affected_tables must be of type List')
            else:
                self._affected_tables = affected_tables
        else:
            self._affected_tables = []

    @table_namer.setter
    def table_namer(self, table_namer):
        """
        Set the table_namer for this class instance. If no value is provided, it is set to a default value.
        """
        if not table_namer:
            self._table_namer = re.sub('\d{4}q\dr\d', '', self.dataset_id)
            LOGGER.info(f"'table_namer' was not set.  "
                        f"Using default value of `{self._table_namer}`.")
        else:
            self._table_namer = table_namer

    def get_table_counts(self, client, dataset, tables):
        """
        Method to get the row counts of the list of tables

        :param dataset: dataset identifier
        :param client: big query client that has been instantiated
        :param tables: list of tables
        :return: returns a dictionary with table name as key and row count as value
                counts_dict -> {'measurement' : 100000000, 'observation': 2000000000000}
        """
        counts_dict = dict()
        for table in tables:
            query = self.TABLE_COUNT_QUERY.format(dataset=dataset, table=table)
            count = client.query(query).to_dataframe()
            counts_dict[table] = count['row_count'][0]
        return counts_dict

    def validate_delete_rule(self, dataset, sandbox_dataset, sandbox_tables,
                             tables_affected, initial_counts, client):
        """
        Validates the cleaning rule which deletes the rows in table

        :param dataset: dataset identifier
        :param sandbox_dataset: sandbox dataset identifier
        :param sandbox_tables: list of sandbox dataset names for generated for this cleaning rule.
        :param tables_affected: list of table names affected by this cleaning rule
        :param initial_counts: dictionary with row counts of tables prior applying the cleaning rule
        :param client: Bigquery client
        :return: returns success message when the validation is success full else
        raises a RuntimeError.
        """
        final_row_counts = self.get_table_counts(dataset, tables_affected,
                                                 client)
        sandbox_row_counts = self.get_table_counts(
            sandbox_dataset, list(sandbox_tables.values()), client)

        for k, v in initial_counts.items():
            if v == final_row_counts[k] + sandbox_row_counts[sandbox_tables[k]]:
                return LOGGER.info(
                    f'{self._issue_numbers[0]} cleaning rule has run successfully on {dataset}.{k} table.'
                )
            else:
                return RuntimeError(
                    f'{self._issue_numbers[0]} cleaning rule is failed on {dataset}.{k} table.\
                     There is a discrepancy in no.of records that\'s been deleted'
                )

    @abstractmethod
    def get_sandbox_tablenames(self):
        pass

    def sandbox_table_for(self, affected_table):
        """
        A helper function to retrieve the sandbox table name for the affected_table
        :param affected_table: 
        :return: 
        """
        base_name = f'{"_".join(self.issue_numbers).lower()}_{affected_table}'
        return get_sandbox_table_name(self.table_namer, base_name)

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
            LOGGER.exception(
                f"Cannot list queries for {self.__class__.__name__}")
            raise

        for query in query_list:
            LOGGER.info(
                f"Generated SQL Query:\n{query.get(cdr_consts.QUERY, 'NO QUERY FOUND')}"
            )

    def get_sandbox_options_string(self, shared=False):
        """
        Helper function to retrieve the sandbox options which includes the description and labels

        :param shared: boolean if the sandbox table will be shared
        :return: string of the sandbox options
        """
        return get_sandbox_options(self.dataset_id,
                                   self.__class__.__name__,
                                   self.table_tag,
                                   self.description,
                                   shared_lookup=shared)

    def get_bq_col_type(self, col_type):
        """
        Return correct SQL column type representation.

        :param col_type: The type of column as defined in json schema files.

        :return: A SQL column type compatible with BigQuery
        """
        lower_col_type = col_type.lower()
        if lower_col_type == 'integer':
            return 'INT64'

        if lower_col_type == 'string':
            return 'STRING'

        if lower_col_type == 'float':
            return 'FLOAT64'

        if lower_col_type == 'numeric':
            return 'DECIMAL'

        if lower_col_type == 'time':
            return 'TIME'

        if lower_col_type == 'timestamp':
            return 'TIMESTAMP'

        if lower_col_type == 'date':
            return 'DATE'

        if lower_col_type == 'datetime':
            return 'DATETIME'

        if lower_col_type == 'bool':
            return 'BOOL'

        return 'UNSET'

    def get_bq_mode(self, mode):
        """
        Return correct SQL for column mode.

        :param mode:  either nullable or required as defined in json schema files.

        :return: NOT NULL or empty string
        """
        lower_mode = mode.lower()
        if lower_mode == 'nullable':
            return ''

        if lower_mode == 'required':
            return 'NOT NULL'

        return 'UNSET'

    def get_bq_fields_sql(self, fields):
        """
        Get the SQL compliant fields definition from json fields object.

        :param fields: table schema in json format

        :return: a string that can be added to SQL to generate a correct
            table.
        """
        fields_list = []
        for field in fields:
            rendered = self.fields_templ.render(
                name=field.get('name'),
                col_type=self.get_bq_col_type(field.get('type')),
                mode=self.get_bq_mode(field.get('mode')),
                desc=field.get('description'))

            fields_list.append(rendered)

        fields_str = ','.join(fields_list)
        return fields_str
