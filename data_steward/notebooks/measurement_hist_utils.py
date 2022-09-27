from matplotlib import pyplot as plt


def init_histogram(axis, sub_dataframe):
    centroids = sub_dataframe['bin_centroid']
    bins = len(sub_dataframe)
    weights = sub_dataframe['bin_count']
    min_bin = sub_dataframe['bin_lower_bound'].min()
    max_bin = sub_dataframe['bin_upper_bound'].max()
    counts_, bins_, _ = axis.hist(centroids,
                                  bins=bins,
                                  weights=weights,
                                  range=(min_bin, max_bin))


def get_measurement_concept_ids(df):
    """
    Retrieve a unique set of measurement_concept_ids from the given df
    
    :param df: dataframe
    :return: a unique set of measurement_concept_ids
    """
    return df['measurement_concept_id'].unique()


def get_unit_concept_ids(df, measurement_concept_id=None):
    """
    Retrieve a unique set of unit concept ids for a given df
    
    :param df: dataframe
    :param measurement_concept_id: an option measurement_concept_id
    :return: a unique set of unit_concept_ids
    """
    if measurement_concept_id is None:
        unit_concept_ids = df['unit_concept_id'].unique()
    else:
        unit_concept_ids = df.loc[df['measurement_concept_id'] ==
                                  measurement_concept_id,
                                  'unit_concept_id'].unique()
    return unit_concept_ids


def get_sub_dataframe(df, measurement_concept_id, unit_concept_id):
    """
    Retrieve subset of the dataframe given a measurement_concept_id and unit_concept_id
    
    :param df: dataframe
    :param measurement_concept_id: measurement_concept_id for which the subset is extracted
    :param unit_concept_id: the unit_concept_id for which the subset is extracted
    :return: a subset of the dataframe
    """

    indexes = (df['measurement_concept_id'] == measurement_concept_id) \
              & (df['unit_concept_id'] == unit_concept_id)
    return df[indexes]


def get_measurement_concept_dict(df):
    """
    Retrieve dictionary containing the measurement_concept_id and its corresponding measurement_concept_name
    
    :param df: dataframe
    :return: a ictionary containing the measurement_concept_id and its corresponding measurement_concept_name
    """

    return dict(zip(df.measurement_concept_id, df.measurement_concept_name))


def get_unit_concept_id_dict(df):
    """
    Retrieve dictionary containing the unit_concept_id and its corresponding unit_concept_name
    
    :param df: dataframe
    :return: a dictionary containing the unit_concept_id and its corresponding unit_concept_name
    """

    return dict(zip(df.unit_concept_id, df.unit_concept_name))


def generate_plot(measurement_concept_id,
                  measurement_concept_dict,
                  value_dists_1,
                  value_dists_2,
                  unit_dict_1,
                  unit_dict_2,
                  sharex=False,
                  sharey=False,
                  figure_name=None):
    """
    Generate n (the number of source units being transformed) by 2 
    grid to compare the value distributions of before and after unit transformation. 
    
    :param measurement_concept_id: the measurement_concept_id for which the distributions are displayed
    :param measurement_concept_dict: the dictionary containing the measurement name
    :param value_dists_1 dataframe containing the distribution for dataset 1
    :param value_dists_2 dataframe containing the distribution for dataset 2
    :param unit_dict_1 dictionary containing the unit names for dataset 1
    :param unit_dict_2 dictionary containing the unit names for dataset 2
    :param sharex a boolean indicating whether subplots share the x-axis
    :param sharey a boolean indicating whether subplots share the y-axis
    :param figure_name filename to save the figure under
    :return: a list of query dicts for rerouting the records to the corresponding destination table
    """

    measurement_concept_id = str(measurement_concept_id)

    units_before = get_unit_concept_ids(value_dists_1, measurement_concept_id)
    units_after = get_unit_concept_ids(value_dists_2, measurement_concept_id)

    # Automatically adjusting the height of the plot
    plt.rcParams['figure.figsize'] = [18, 4 * len(units_before)]

    for unit_after in units_after:

        unit_after_name = unit_dict_2[unit_after]
        # Generate the n * 2 grid to display the side by side distributions
        fig, axs = plt.subplots(len(units_before),
                                2,
                                sharex=sharex,
                                sharey=sharey)
        measurement_concept_name = measurement_concept_dict[
            measurement_concept_id]
        unit_concept_after = unit_dict_2[unit_after]

        fig.suptitle(
            'Measurement: {measurement}\n standard unit: {unit}'.format(
                measurement=measurement_concept_name, unit=unit_concept_after))

        counter = 0

        sub_df_after = get_sub_dataframe(value_dists_2, measurement_concept_id,
                                         unit_after)

        for unit_before in units_before:
            sub_df_before = get_sub_dataframe(value_dists_1,
                                              measurement_concept_id,
                                              unit_before)
            unit_before_name = unit_dict_1[unit_before]

            if len(units_before) == 1:
                axs_before = axs[0]
                axs_after = axs[1]
            else:
                axs_before = axs[counter][0]
                axs_after = axs[counter][1]

            init_histogram(axs_before, sub_df_before)
            axs_before.set_title('before unit: {}'.format(unit_before_name))
            init_histogram(axs_after, sub_df_after)
            axs_after.set_title('after unit: {}'.format(unit_after_name))

            counter += 1

    # Save figure optionally
    if figure_name:
        plt.savefig(figure_name)


def convert_to_sql_list(concept_ids):
    return f'({",".join(map(str, concept_ids))})'
