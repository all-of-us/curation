"""
File is intended to establish a 'HPO class' that can be used
to store data quality metrics for each HPO in an easy and
identifiable fashion.

Class was used as a means for storing information as the ability
to add functions could prove useful in future iterations of the
script.
"""

from dictionaries_and_lists import thresholds
from datetime import date
import sys


class DataQualityMetric:
    """
    Class is used to store data quality metrics.
    """

    def __init__(
        self, hpo='', table='', metric_type='', value=0,
            data_quality_dimension='', first_reported=date.today(),
            link=''):

        """
        Used to establish the attributes of the DataQualityMetric
        object being instantiated.

        Parameters
        ----------
        hpo (string): name of the HPO being associated with the
            data quality metric in question (e.g. nyc_cu)

        table (string): name of the table whose data quality metric
            is being determined (e.g. Measurement)

        metric_type (string): name of the metric that is being
            determined (e.g. duplicates)

        value (float): value that represents the quantitative value
            of the data quality metric being investigated

        data_quality_dimension (string): represents whether the
            metric_type being investigated is related to the
            conformance, completeness, or plausibility of data
            quality with respect to the Kahn framework

        first_reported (datetime.date): represents the time
            at which this metric (with all of the other parameters
            being exactly the same) was first reported

        link (string): link to the AoU EHR Operations page that
            can help the site troubleshoot its data quality
        """

        self.hpo = hpo
        self.table = table
        self.metric_type = metric_type
        self.value = value
        self.data_quality_dimension = data_quality_dimension
        self.first_reported = first_reported
        self.link = link

    def print_dqd_attributes(self):
        """
        Function is used to print out some of the attributes
        of a DataQualityMetric object in a manner that enables
        all of the information to be displayed in a
        human-readable format.
        """
        print(
            "HPO: {hpo}\n"
            "Table: {table}\n"
            "Metric Type: {metric_type}\n"
            "Value: {value}\n"
            "Data Quality Dimension: {dqd}\n"
            "First Reported: {date}\n"
            "Link: {link}".format(
                hpo=self.hpo, table=self.table,
                metric_type=self.metric_type,
                value=self.value, dqd=self.data_quality_dimension,
                date=self.first_reported,
                link=self.link))

    def get_list_of_attribute_names(self):
        """
        Function is used to get a list of the attributes that
        are associated with a DataQualityMetric object. This will
        ultimately be used to populate the columns of a
        pandas dataframe.

        Return
        ------
        attribute_names (list): list of the attribute names
            for a DataQualityMetric object
        """

        attribute_names = [
            "HPO", "Table", "Metric Type",
            "Value", "Data Quality Dimension", "First Reported",
            "Link"]

        return attribute_names

    def get_attributes_in_order(self):
        """
        Function is used to get the attributes of a particular
        DataQualityMetric object in an order that parallels
        the get_list_of_attribute_names function above. This
        will be used to populate the dataframe with data quality
        issues.

        Return
        ------
        attributes (list): list of the attributes (values, strings)
            for the object
        """

        attributes = [
            self.hpo, self.table, self.metric_type, self.value,
            self.data_quality_dimension, self.first_reported, self.link]

        return attributes


class HPO:
    """
    Class is used to associated data quality issues with a particular
    HPO.
    """

    def __init__(
            self, name, full_name, concept_success, duplicates,
            end_before_begin, data_after_death,
            route_success, unit_success, measurement_integration,
            ingredient_integration):

        """
        Used to establish the attributes of the HPO object being instantiated.

        Parameters
        ----------
        self (HPO object): the object to be created

        name (str): name of the HPO ID to create (e.g. nyc_cu)

        full_name (str): full name of the HPO

        all other optional parameters are intended to be lists. These
        lists should contain DataQualityMetric objects that have all
        of the relevant pieces pertaining to said metric object.

        the exact descriptions of the data quality metrics can be found
        on the AoU HPO website at the following link:
            sites.google.com/view/ehrupload
        """
        self.name = name
        self.full_name = full_name

        # relates to multiple tables - therefore should be list of objects
        self.concept_success = concept_success
        self.duplicates = duplicates
        self.end_before_begin = end_before_begin
        self.data_after_death = data_after_death

        # only relates to one table - therefore single float expected
        self.route_success = route_success
        self.unit_success = unit_success
        self.measurement_integration = measurement_integration
        self.ingredient_integration = ingredient_integration

    def add_attribute_with_string(self, metric, dq_object):
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

        if metric == 'Concept ID Success Rate':
            self.concept_success.append(dq_object)

        elif metric == 'Duplicate Records':
            self.duplicates.append(dq_object)

        elif metric == 'End Dates Preceding Start Dates':
            self.end_before_begin.append(dq_object)

        elif metric == 'Data After Death':
            self.data_after_death.append(dq_object)

        elif metric == 'Measurement Integration':
            self.measurement_integration.append(dq_object)

        elif metric == 'Drug Ingredient Integration':
            self.ingredient_integration.append(dq_object)

        elif metric == 'Route Concept ID Success Rate':
            self.route_success.append(dq_object)

        elif metric == 'Unit Concept ID Success Rate':
            self.unit_success.append(dq_object)

        else:
            print("Unrecognized metric input: {metric} for {hpo}".format(
                metric=metric, hpo=self.name))
            sys.exit(0)

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

        NOTE: if no data quality problems are found, however, the function
        will return 'None' to signify that no issues arose
        """

        failing_metrics = []

        # below we can find the data quality metrics for several tables -
        # need to iterate through a list to get the objects for each table
        for concept_success_obj in self.concept_success:
            if concept_success_obj.value < thresholds['concept_success_min']:
                failing_metrics.append(concept_success_obj)

        for duplicates_obj in self.duplicates:
            if duplicates_obj.value > thresholds['duplicates_max']:
                failing_metrics.append(duplicates_obj)

        for end_before_begin_obj in self.end_before_begin:
            if end_before_begin_obj.value > thresholds['end_before_begin_max']:
                failing_metrics.append(end_before_begin_obj)

        for data_after_death_obj in self.data_after_death:
            if data_after_death_obj.value > thresholds['data_after_death_max']:
                failing_metrics.append(data_after_death_obj)

        for route_obj in self.route_success:
            if route_obj.value < thresholds['route_success_min']:
                failing_metrics.append(route_obj)

        for unit_obj in self.unit_success:
            if unit_obj.value < thresholds['unit_success_min']:
                failing_metrics.append(unit_obj)

        for measurement_integration_obj in self.measurement_integration:
            if measurement_integration_obj.value < \
                    thresholds['measurement_integration_min']:
                failing_metrics.append(measurement_integration_obj)

        for ingredient_integration_obj in self.ingredient_integration:
            if ingredient_integration_obj.value < \
                    thresholds['route_success_min']:
                failing_metrics.append(ingredient_integration_obj)

        if not failing_metrics:  # no errors logged
            return None
        else:
            return failing_metrics
