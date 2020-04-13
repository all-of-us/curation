"""
File is intended to establish a 'Data Quality Metric class' that can be used
to store data quality metrics in an easy and
identifiable fashion.

Class was used as a means for storing information as the ability
to add functions could prove useful in future iterations of the
script.
"""


import datetime


class DataQualityMetric:
    """
    Class is used to store data quality metrics.
    """

    def __init__(
        self, hpo='', table_or_class='', metric_type='',
            value=0,
            data_quality_dimension='',
            date=datetime.datetime.today()
    ):

        """
        Used to establish the attributes of the DataQualityMetric
        object being instantiated.

        Parameters
        ----------
        hpo (string): name of the HPO being associated with the
            data quality metric in question (e.g. nyc_cu)

        table_or_class (string): name of the table/class whose data
            quality metric is being determined
            (e.g. Measurement or 'ACE Inhibitors')

        metric_type (string): name of the metric that is being
            determined (e.g. duplicates)

        value (float): value that represents the quantitative value
            of the data quality metric being investigated

        data_quality_dimension (string): represents whether the
            metric_type being investigated is related to the
            conformance, completeness, or plausibility of data
            quality with respect to the Kahn framework

        date (datetime): 'date' that the DQM represents (in
            other words, the corresponding analytics
            report from which it hails)
        """

        self.hpo = hpo
        self.table_or_class = table_or_class
        self.metric_type = metric_type
        self.value = value
        self.data_quality_dimension = data_quality_dimension
        self.date = date

    def print_attributes(self):
        """
        Function is used to print out the attributes
        of a DataQualityMetric object in a manner that enables
        all of the information to be displayed in a
        human-readable format.
        """
        print(
            "HPO: {hpo}\n"
            "Table/Class: {table_or_class}\n"
            "Metric Type: {metric_type}\n"
            "Value: {value}\n"
            "Data Quality Dimension: {dqd}\n"
            "Date: {date}\n\n".format(
                hpo=self.hpo, table_or_class=self.table_or_class,
                metric_type=self.metric_type,
                value=self.value, dqd=self.data_quality_dimension,
                date=self.date))

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
            "HPO", "Table/Class", "Metric Type",
            "Value", "Data Quality Dimension", "Date"]

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
            self.hpo, self.table_or_class, self.metric_type, self.value,
            self.data_quality_dimension, self.date]

        return attributes
