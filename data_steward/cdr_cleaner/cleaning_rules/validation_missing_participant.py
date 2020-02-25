import logging
from validation.participants import readers

LOGGER = logging.getLogger(__name__)


def exist_participant_match(project_id, dataset_id, hpo_id):
    """
    This function checks if the hpo has submitted the participant_match data 
    
    :param project_id: 
    :param dataset_id: 
    :param hpo_id: 
    :return: 
    """
    pass


def get_list_non_match_participants(project_id, dataset_id, hpo_id):
    """
    This function retrieves a list of non-match participants
    
    :param project_id: 
    :param dataset_id: 
    :param hpo_id: 
    :return: 
    """
    pass


def delete_records_for_non_matching_participants(project_id, dataset_id):
    """
    This function generates the queries that delete participants and their corresponding data points, for which the 
    participant_match data is missing and DRC matching algorithm flags it as a no match 
    
    :param project_id: 
    :param dataset_id: 
    :return: 
    """

    # Retrieving all hpo_ids
    for hpo_id in readers.get_hpo_site_names():
        if not exist_participant_match(project_id, dataset_id, hpo_id):

            LOGGER.log(
                'The hpo site {hpo_id} is missing the participant_match data'.
                format(hpo_id=hpo_id))

        else:
            LOGGER.log(
                'The hpo site {hpo_id} submitted the participant_match data'.
                format(hpo_id=hpo_id))
