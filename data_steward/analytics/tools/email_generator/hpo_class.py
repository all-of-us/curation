"""
File is intended to establish an 'HPO class' that can be used
to store data quality metrics in an easy and
identifiable fashion.

Class was used as a means for storing information as the ability
to add functions could prove useful in future iterations of the
script.

Please note that many of the functions and parameters outlined
in this class are highly contingent upon the DataQualityMetric
class. Please familiarize yourself with the aforementioned
class before learning more about the HPO class.
"""

from dictionaries_and_lists import thresholds
import sys
import constants


class HPO:
    """
    Class is used to associated data quality issues with a particular
    HPO.
    """

    def __init__(
            self,

            # basic attributes
            name, full_name, date,

            # data quality metrics
            concept_success, duplicates,
            end_before_begin, data_after_death,
            route_success, unit_success, measurement_integration,
            ingredient_integration, date_datetime_disp,
            erroneous_dates, person_id_failure, achilles_errors,
            visit_date_disparity, visit_id_failure,

            # number of rows for the 6 canonical tables
            num_measurement_rows=0,
            num_visit_rows=0,
            num_procedure_rows=0,
            num_condition_rows=0,
            num_drug_rows=0,
            num_observation_rows=0):

        """
        Used to establish the attributes of the HPO object being instantiated.

        Parameters
        ----------
        self (HPO object): the object to be created
        name (str): name of the HPO ID to create (e.g. nyc_cu)

        full_name (str): full name of the HPO

        date (datetime): 'date' that the HPO represents (in
            other words, the corresponding analytics
            report from which it hails)

        concept_success (list): list of DataQuality metric objects
            that should all have the metric_type relating
            to concept success rate. each index should also
            represent a different table.

        duplicates (list): list of DataQuality metric objects
            that should all have the metric_type relating
            to the number of duplicates. each index should
            also represent a different table

        end_before_begin (list): list of DataQuality metric objects
            that should all have the metric_type relating
            to the number of end dates preceding start dates.
            each index should also represent a different table

        data_after_death (list): list of DataQuality metric objects
            that should all have the metric_type relating
            to the percentage of data points that follow a
            patient's death date. each index should also
            represent a different table

        date_datetime_disp (list): list of the DataQualityMetric
            objects that have the metric_type relating the
            percentage of rows where the date and datetime
            objects are not in agreement

        route_success (list): list of DataQuality metric objects
            that should all have the metric_type relating
            to the concept success rate for the route_concept_id
            field. should have a length of one (for the drug
            exposure table)

        unit_success (list): list of DataQuality metric objects
            that should all have the metric_type relating
            to the concept success rate for the route_concept_id
            field. should have a length of one (for the
            measurement table)

        measurement_integration (list): list of DataQuality metric
            objects that should all have the metric_type relating
            to the integration of certain measurement concepts.
            should have a length of one (for the measurement
            table).

        ingredient_integration (list): list of DataQuality metric
            objects that should all have the metric_type relating
            to the integration of certain drug ingredients.
            should have a length of one (for the drug exposure
            table).

        erroneous_dates (list): list of DataQualityMetric
            objects that should all have the metric_type
            relating to the percentage of dates that are before
            1980 (or before 1900 for the observation table)

        person_id_failure (list): list of DataQualityMetric
            objects that relate what percentage of rows in
            each of the tables have a 'failing' person_id
            (one that does not exist in the person table)

        achilles_errors (list): list of DataQualityMetric
            objects that show the number of ACHILLES errors
            for the particular date

        visit_date_disparity (list): list of DataQualityMetric
            objects that show the percentage of rows that have
            inconsistencies with respect to the visit date

        visit_id_failure (list): list of DataQualityMetric
            objects that show the percentage of rows with an
            invalid visit_occurrence_id

        num_measurement_rows (float): number of rows in the
            measurement table

        num_visit_rows (float): number of rows in the
            visit_occurrence table

        num_procedure_rows (float): number of rows in the
            procedure_occurrence table

        num_condition_rows (float): number of rows in the
            condition_occurrence table

        num_drug_rows (float): number of rows in the drug
            exposure table

        number_observation_rows (float): number of rows
            in the observation table
        """
        # inherent attributes
        self.name = name
        self.full_name = full_name
        self.date = date

        # relates to multiple tables
        self.concept_success = concept_success
        self.duplicates = duplicates
        self.end_before_begin = end_before_begin
        self.data_after_death = data_after_death
        self.date_datetime_disp = date_datetime_disp
        self.erroneous_dates = erroneous_dates
        self.person_id_failure = person_id_failure
        self.visit_date_disparity = visit_date_disparity
        self.visit_id_failure = visit_id_failure

        # only relates to one table / entity
        self.route_success = route_success
        self.unit_success = unit_success
        self.measurement_integration = measurement_integration
        self.ingredient_integration = ingredient_integration
        self.achilles_errors = achilles_errors

        # number of rows in each table
        self.num_measurement_rows = num_measurement_rows
        self.num_visit_rows = num_visit_rows
        self.num_procedure_rows = num_procedure_rows
        self.num_condition_rows = num_condition_rows
        self.num_drug_rows = num_drug_rows
        self.num_observation_rows = num_observation_rows

    def print_attributes(self):
        """
        Function is used to generate a string that can be
        used to display the various attributes as a string.

        This string is then printed to display on the
        program.
        """
        attributes_str = f"""
        HPO ID: {self.name}
        Full Name: {self.full_name}
        Date: {self.date}

        Number of Metrics:
            Concept Success Rate: {len(self.concept_success)}
            Duplicates: {len(self.duplicates)}
            End Dates Preceding Start Dates: {len(self.end_before_begin)}
            Data After Death: {len(self.data_after_death)}
            Route Success: {len(self.route_success)}
            Unit Success: {len(self.unit_success)}
            Measurement Integration: {len(self.measurement_integration)}
            Ingredient Integration: {len(self.ingredient_integration)}
            Date/Datetime Disagreement: {len(self.date_datetime_disp)}
            Erroneous Dates: {len(self.erroneous_dates)}
            Person ID Failure: {len(self.person_id_failure)}
            Number of ACHILLES Errors: {len(self.achilles_errors)}
            Visit Date Disparity: {len(self.visit_date_disparity)}
            Visit ID Failure: {len(self.visit_id_failure)}


        Number of Rows:
            Measurement: {self.num_measurement_rows}
            Visit Occurrence: {self.num_visit_rows}
            Procedure Occurrence: {self.num_procedure_rows}
            Condition Occurrence: {self.num_condition_rows}
            Drug Exposure: {self.num_drug_rows}
            Observation: {self.num_observation_rows}
        """

        print(attributes_str)

    def add_metric_with_string(self, metric, dq_object):
        """
        Function is designed to enable the script to add
        a DataQualityMetric object to the attributes that
        define an HPO object. This will allow us to easily
        associate an HPO object with its constituent data
        quality metrics

        Parameters
        ----------
        metric (string): the name of the sheet that contains the
            dimension of data quality to be investigated

        dq_object (DataQualityMetric): object that contains
            the information for a particular aspect of the
            site's data quality (NOTE: dq_object.hpo should
            equal self.name whenever this is used)
        """

        if metric == constants.concept_full:
            self.concept_success.append(dq_object)

        elif metric == constants.duplicates_full:
            self.duplicates.append(dq_object)

        elif metric == constants.end_before_begin_full:
            self.end_before_begin.append(dq_object)

        elif metric == constants.data_after_death_full:
            self.data_after_death.append(dq_object)

        elif metric == constants.sites_measurement_full:
            self.measurement_integration.append(dq_object)

        elif metric == constants.drug_success_full:
            self.ingredient_integration.append(dq_object)

        elif metric == constants.drug_routes_full:
            self.route_success.append(dq_object)

        elif metric == constants.unit_success_full:
            self.unit_success.append(dq_object)

        elif metric == constants.date_datetime_disparity_full:
            self.date_datetime_disp.append(dq_object)

        elif metric == constants.erroneous_dates_full:
            self.erroneous_dates.append(dq_object)

        elif metric == constants.person_id_failure_rate_full:
            self.person_id_failure.append(dq_object)

        elif metric == constants.achilles_errors_full:
            self.achilles_errors.append(dq_object)

        elif metric == constants.visit_date_disparity_full:
            self.visit_date_disparity.append(dq_object)

        elif metric == constants.visit_id_failure_rate_full:
            self.visit_id_failure.append(dq_object)

        else:
            hpo_name = self.name
            print(f"Unrecognized metric input: {metric} for {hpo_name}")
            sys.exit(0)

    def add_row_count_with_string(self, table, value):
        """
        Function is designed to enable the script to add
        a row count (float) to the attributes that
        define an HPO object. This will allow us to easily
        associate an HPO object with its row count for different
        tables.

        Parameters
        ----------
        table (string): the name of the column that was used
            to determine the number of rows there are associated
            with each table

        value (float): number of rows for the particular
            table
        """

        if table == constants.observation_total_row:
            self.num_observation_rows = value
        elif table == constants.drug_total_row:
            self.num_drug_rows = value
        elif table == constants.procedure_total_row:
            self.num_procedure_rows = value
        elif table == constants.condition_total_row:
            self.num_condition_rows = value
        elif table == constants.measurement_total_row:
            self.num_measurement_rows = value
        elif table == constants.visit_total_row:
            self.num_visit_rows = value
        else:
            hpo_name = self.name
            print(f"Unrecognized table input: {table} for {hpo_name}")
            sys.exit(0)

    def use_table_or_class_name_to_find_rows(
            self, table_or_class, metric):
        """
        Function is intended to use the table name to find
        the 'total number of rows' associated with said
        table and the 'success rate' for said table.
        Both should which should be stored in the HPO object.

        Parameters
        ----------
        table_or_class (string): table (e.g. 'Measurement) or
            class (e.g. 'ACE Inhibitors') whose data quality
            metrics are to be determined

        metric (string): the metric (e.g. the concept
            success rate) that is being investigated

        Returns
        -------
        rel_rows (float): number of rows that pertain to the
            particular metric for the table

        total_rows (float): the total number of rows for the
            table being queried
        """

        if metric == constants.concept_full:
            for obj in self.concept_success:
                if obj.table_or_class == table_or_class:
                    succ_rate = obj.value

        elif metric == constants.duplicates_full:
            for obj in self.duplicates:
                if obj.table_or_class == table_or_class:
                    succ_rate = obj.value

        elif metric == constants.end_before_begin_full:
            for obj in self.end_before_begin:
                if obj.table_or_class == table_or_class:
                    succ_rate = obj.value

        elif metric == constants.data_after_death_full:
            for obj in self.data_after_death:
                if obj.table_or_class == table_or_class:
                    succ_rate = obj.value

        elif metric == constants.sites_measurement_full:
            for obj in self.measurement_integration:
                if obj.table_or_class == table_or_class:
                    succ_rate = obj.value

        elif metric == constants.drug_success_full:
            for obj in self.ingredient_integration:
                if obj.table_or_class == table_or_class:
                    succ_rate = obj.value

        elif metric == constants.drug_routes_full:
            for obj in self.route_success:
                if obj.table_or_class == table_or_class:
                    succ_rate = obj.value

        elif metric == constants.unit_success_full:
            for obj in self.unit_success:
                if obj.table_or_class == table_or_class:
                    succ_rate = obj.value

        elif metric == constants.date_datetime_disparity_full:
            for obj in self.date_datetime_disp:
                if obj.table_or_class == table_or_class:
                    succ_rate = obj.value

        elif metric == constants.erroneous_dates_full:
            for obj in self.erroneous_dates:
                if obj.table_or_class == table_or_class:
                    succ_rate = obj.value

        elif metric == constants.person_id_failure_rate_full:
            for obj in self.person_id_failure:
                if obj.table_or_class == table_or_class:
                    succ_rate = obj.value

        elif metric == constants.achilles_errors_full:
            for obj in self.achilles_errors:
                if obj.table_or_class == table_or_class:
                    succ_rate = obj.value

        elif metric == constants.visit_date_disparity_full:
            for obj in self.visit_date_disparity:
                if obj.table_or_class == table_or_class:
                    succ_rate = obj.value

        # NOTE: perhaps we should have a different variable
        # for 'failure rates' to avoid confusion
        elif metric == constants.visit_id_failure_rate_full:
            for obj in self.visit_id_failure:
                if obj.table_or_class == table_or_class:
                    succ_rate = obj.value

        else:
            raise Exception(f"""
                Unexpected metric type:
                {metric} found for table or class
                {table_or_class}""")

        if table_or_class == constants.measurement_full:
            total_rows = self.num_measurement_rows
        elif table_or_class == constants.visit_occurrence_full:
            total_rows = self.num_visit_rows
        elif table_or_class == constants.procedure_full:
            total_rows = self.num_procedure_rows
        elif table_or_class == constants.condition_occurrence_full:
            total_rows = self.num_condition_rows
        elif table_or_class == constants.drug_exposure_full:
            total_rows = self.num_drug_rows
        elif table_or_class == constants.observation_full:
            total_rows = self.num_observation_rows
        elif table_or_class == "Device Exposure":  # ignore for now
            total_rows = 0
        else:
            raise Exception(
                f"""Unexpected table type:
                {table_or_class} found for metric {metric}""")

        if metric == constants.duplicates_full:
            rel_rows = succ_rate  # want to report out the total #

        else:
            rel_rows = total_rows * (succ_rate / 100)

        return rel_rows, total_rows

    def get_row_count_from_table_and_metric(
            self, metric, table_or_class, relevant_objects):
        """
        Function is used to get the number of rows (either the
        amount of 'successful' or 'failed') for a particular
        metric in the HPO class. This is intended to primarily
        perform the computational work underlying the
        return_metric_row_count function.

        Parameters
        ----------
        metric (string): the metric (e.g. the concept
            success rate) that is being investigated

        table_or_class (string): table whose data quality metrics are
            to be determined

        relevant_objects (lst): list of DataQualityMetric
            objects that are to be iterated over. These
            DQM objects should all have the same
            'metric_type' attribute

        Returns
        -------
        row_count (float): total number of rows - merely
            a multiplier of the two aforementioned
            number converted from percent to rows
        """
        row_count = None

        for dqm in relevant_objects:
            dqm_table = dqm.table_or_class

            if dqm_table == table_or_class:  # discovered
                row_count, total_rows = \
                    self.use_table_or_class_name_to_find_rows(
                        table_or_class=table_or_class,
                        metric=metric)

        # making sure we could calculate the row_count
        assert row_count is not None, f"""The row count for the following
            data quality metric could not be found for the
            table {table_or_class} and the metric {metric}"""

        return row_count

    def use_string_to_get_relevant_objects(self, metric):
        """
        Function is designed to enable someone to use a
        string to access the relevant objects pertaining
        to the desire metric.

        Parameters
        ----------
        metric (string): the metric (e.g. the concept
            success rate) that is being investigated

        Returns
        -------
        relevant_objects (list): list of DataQualityMetric
            objects that are related to both the HPO
            and metric provided
        """
        if metric == constants.concept_full:
            relevant_objects = self.concept_success

        elif metric == constants.duplicates_full:
            relevant_objects = self.duplicates

        elif metric == constants.end_before_begin_full:
            relevant_objects = self.end_before_begin

        elif metric == constants.data_after_death_full:
            relevant_objects = self.data_after_death

        elif metric == constants.sites_measurement_full:
            relevant_objects = self.measurement_integration

        elif metric == constants.drug_success_full:
            relevant_objects = self.ingredient_integration

        elif metric == constants.drug_routes_full:
            relevant_objects = self.route_success

        elif metric == constants.unit_success_full:
            relevant_objects = self.unit_success

        elif metric == constants.date_datetime_disparity_full:
            relevant_objects = self.date_datetime_disp

        elif metric == constants.erroneous_dates_full:
            relevant_objects = self.erroneous_dates

        elif metric == constants.person_id_failure_rate_full:
            relevant_objects = self.person_id_failure

        elif metric == constants.achilles_errors_full:
            relevant_objects = self.achilles_errors

        elif metric == constants.visit_date_disparity_full:
            relevant_objects = self.visit_date_disparity

        elif metric == constants.visit_id_failure_rate_full:
            relevant_objects = self.visit_id_failure

        else:
            raise Exception(
                f"""The following was identified as a metric:
                {metric}""")

        return relevant_objects

    def return_metric_row_count(self, metric, table):
        """
        Function is used to return the 'row
        count' for a particular metric. This will be
        useful for determine 'aggregate metrics' which
        are contingent upon 'aggregate' successes over
        totals. This 'row count' could either refer to
        the number of 'successful' rows or the number
        of 'failed' rows depending on the nature of the
        metric that is being investigated.

        Parameters
        ----------
        metric (string): the metric (e.g. the concept
            success rate) that is being investigated

        table (string): table whose data quality metrics are
            to be determined

        Returns
        -------
        row_count (float): total number of rows - merely
            a multiplier of the two aforementioned
            number converted from percent to rows
        """

        relevant_objects = self.use_string_to_get_relevant_objects(
            metric=metric)

        row_count = self.get_row_count_from_table_and_metric(
            metric=metric, table_or_class=table,
            relevant_objects=relevant_objects)

        return row_count

    def find_failing_metrics(self):
        """
        Function is used to create a catalogue of the 'failing' data
        quality metrics at defined by the thresholds established by
        the appropriate dictionary from relevant_dictionaries.

        Parameters
        ----------
        self (HPO object): the object whose 'failing metrics' are to
            be determined

        Returns
        -------
        failing_metrics (list): has a list of the data quality metrics
            for the HPO that have 'failed' based on the thresholds
            provided

        NOTES
        -----
        1. if no data quality problems are found, however, the
        function will return 'None' to signify that no issues arose

        2. this funciton is not currently implemented in our current
        iteration of metrics_over_time. this function, however, holds
        potential to be useful in future iterations.
        """

        failing_metrics = []

        # below we can find the data quality metrics for several tables -
        # need to iterate through a list to get the objects for each table
        for concept_success_obj in self.concept_success:
            if concept_success_obj.value < \
                    thresholds[constants.concept_success_min]:
                failing_metrics.append(concept_success_obj)

        for duplicates_obj in self.duplicates:
            if duplicates_obj.value > \
                    thresholds[constants.duplicates_max]:
                failing_metrics.append(duplicates_obj)

        for end_before_begin_obj in self.end_before_begin:
            if end_before_begin_obj.value > \
                    thresholds[constants.end_before_begin_max]:
                failing_metrics.append(end_before_begin_obj)

        for data_after_death_obj in self.data_after_death:
            if data_after_death_obj.value > \
                    thresholds[constants.data_after_death_max]:
                failing_metrics.append(data_after_death_obj)

        for route_obj in self.route_success:
            if route_obj.value < \
                    thresholds[constants.route_success_min]:
                failing_metrics.append(route_obj)

        for unit_obj in self.unit_success:
            if unit_obj.value < \
                    thresholds[constants.unit_success_min]:
                failing_metrics.append(unit_obj)

        '''
        for measurement_integration_obj in self.measurement_integration:
            if measurement_integration_obj.value < \
                    thresholds[constants.measurement_integration_min]:
                failing_metrics.append(measurement_integration_obj)

        for ingredient_integration_obj in self.ingredient_integration:
            if ingredient_integration_obj.value < \
                    thresholds[constants.route_success_min]:
                failing_metrics.append(ingredient_integration_obj)
        '''

        for date_datetime_obj in self.date_datetime_disp:
            if date_datetime_obj.value > \
                    thresholds[constants.date_datetime_disparity_max]:
                failing_metrics.append(date_datetime_obj)

        for erroneous_date_obj in self.erroneous_dates:
            if erroneous_date_obj.value > \
                    thresholds[constants.erroneous_dates_max]:
                failing_metrics.append(erroneous_date_obj)

        for person_id_failure_obj in self.person_id_failure:
            if person_id_failure_obj.value > \
                    thresholds[constants.person_failure_rate_max]:
                failing_metrics.append(person_id_failure_obj)

        for achilles_error_obj in self.achilles_errors:
            if achilles_error_obj.value > \
                    thresholds[constants.achilles_errors_max]:
                failing_metrics.append(achilles_error_obj)

        for visit_date_disparity_obj in self.visit_date_disparity:
            if visit_date_disparity_obj.value > \
                    thresholds[constants.visit_date_disparity_max]:
                failing_metrics.append(visit_date_disparity_obj)

        for visit_id_failure_obj in self.visit_id_failure:
            if visit_id_failure_obj.value > \
                    thresholds[constants.visit_id_failure_rate_max]:
                failing_metrics.append(visit_id_failure_obj)

        if not failing_metrics:  # no errors logged
            return None
        else:
            return failing_metrics
