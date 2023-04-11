#!/usr/bin/env bash

# Takes an RDR ETL results creates views to represent the PPI surveys.
# Assumes you have already activated a service account.

from argparse import ArgumentParser
import logging

from google.cloud import bigquery

from utils import auth
from gcloud.bq import BigQueryClient
from utils import pipeline_logging
from common import CDR_SCOPES, JINJA_ENV

LOGGER = logging.getLogger(__name__)


def parse_rdr_args(raw_args=None):
    parser = ArgumentParser(
        description=
        'Setting up a test RDR dataset and its views to distinguish between surveys.'
    )

    parser.add_argument('--run_as',
                        action='store',
                        dest='run_as_email',
                        help='Service account email address to impersonate',
                        required=True)
    parser.add_argument('--curation_project',
                        action='store',
                        dest='project_id',
                        help='Curation project to load the RDR data into.',
                        required=True)
    parser.add_argument('--vocab_dataset',
                        action='store',
                        dest='vocabulary',
                        help=('Vocabulary dataset used by '
                              'RDR to create this data dump.'),
                        required=True)
    parser.add_argument('-l',
                        '--console_log',
                        dest='console_log',
                        action='store_true',
                        required=False,
                        help='Log to the console as well as to a file.')
    parser.add_argument('-r',
                        '--rdr_dataset',
                        action='store',
                        dest='rdr_dataset_id',
                        help=('Dataset to copy for this POC'),
                        required=True)

    return parser.parse_args(raw_args)


PPI_NEEDS = [{
    "survey_name": "thebasics",
    "survey_concept_id": 1586134
}, {
    "survey_name": "lifestyle",
    "survey_concept_id": 1585855
}, {
    "survey_name": "consentpii",
    "survey_concept_id": 1585594
}, {
    "survey_name": "ehrconsentpii",
    "survey_concept_id": 1586098
}, {
    "survey_name": "overall_health",
    "survey_concept_id": 1585710
}, {
    "survey_name": "refresher_consent",
    "survey_concept_id": 1585550
}, {
    "survey_name": "dv_ehr_sharing",
    "survey_concept_id": 903506
}, {
    "survey_name": "family_history",
    "survey_concept_id": 43528698
}, {
    "survey_name": "healthcare_access",
    "survey_concept_id": 43528895
}, {
    "survey_name": "personal_medical_history",
    "survey_concept_id": 43529712
}, {
    "survey_name": "gror",
    "survey_concept_id": 903505
}, {
    "survey_name": "sdoh",
    "survey_concept_id": 40192389
}, {
    "survey_name": "cope_vaccine3",
    "survey_concept_id": 765936
}, {
    "survey_name": "personal_family_history",
    "survey_concept_id": 1740639
}, {
    "survey_name": "primary_consent_update",
    "survey_concept_id": 903507
}, {
    "survey_name": "cope_vaccine4",
    "survey_concept_id": 1741006
}, {
    "survey_name": "cope_vaccine1",
    "survey_concept_id": 905047
}, {
    "survey_name": "cope_vaccine2",
    "survey_concept_id": 905055
}, {
    "survey_name": "cope_dec",
    "cope_month": "dec"
}, {
    "survey_name": "cope_feb",
    "cope_month": "feb"
}, {
    "survey_name": "cope_may",
    "cope_month": "may"
}, {
    "survey_name": "cope_nov",
    "cope_month": "nov"
}, {
    "survey_name": "cope_july",
    "cope_month": "july"
}, {
    "survey_name": "cope_june",
    "cope_month": "june"
}, {
    "survey_name": "minute_1",
    "cope_month": "vaccine1"
}, {
    "survey_name": "minute_2",
    "cope_month": "vaccine2"
}, {
    "survey_name": "minute_3",
    "cope_month": "vaccine3"
}, {
    "survey_name": "minute_4",
    "cope_month": "vaccine4"
}]


def create_ppi_views(client, project, dataset):
    """
    Create views on the PPI data to separate streams.

    :param client: a BigQueryClient
    :param dataset: The existing dataset to create views inside
    """

    for ppi_info in PPI_NEEDS:
        survey_name = ppi_info.get("survey_name")
        survey_concept_id = ppi_info.get("survey_concept_id")
        cope_month = ppi_info.get("cope_month")
        view_id = f'{project}.{dataset}.v_observation_{survey_name}'
        view = bigquery.Table(view_id)

        # create a materialized view
        #view.mview_query = JINJA_ENV.from_string("""
        # create a view
        view.view_query = JINJA_ENV.from_string("""
        SELECT obs.*
        FROM `{{project}}.{{dataset}}.observation` obs
        {% if survey_concept_id %}
        JOIN `{{project}}.{{dataset}}.survey_conduct` sc
        ON obs.questionnaire_response_id = sc.survey_conduct_id
        WHERE sc.survey_concept_id = {{survey_concept_id}}
        {% elif cope_month %}
        JOIN `{{project}}.{{dataset}}.cope_survey_semantic_version_map` cssvm
        USING (questionnaire_response_id)
        WHERE cssvm.cope_month = '{{cope_month}}'
        {% else %}
        -- prevent getting the entire table if we can't tell which criteria to use --
        limit 1
        {% endif %}
        """).render(project=project,
                    dataset=dataset,
                    survey_concept_id=survey_concept_id,
                    cope_month=cope_month)

        view = client.create_table(view)
        LOGGER.info(f'Created {view.table_type}: {str(view.reference)}')

        sc_view_id = f'{project}.{dataset}.v_survey_conduct_{survey_name}'
        sc_view = bigquery.Table(sc_view_id)

        # create a materialized view
        #sc_view.mview_query = JINJA_ENV.from_string("""
        # create a view
        sc_view.view_query = JINJA_ENV.from_string("""
        SELECT sc.*
        FROM `{{project}}.{{dataset}}.survey_conduct` sc
        {% if survey_concept_id %}
        WHERE sc.survey_concept_id = {{survey_concept_id}}
        {% elif cope_month %}
        JOIN `{{project}}.{{dataset}}.cope_survey_semantic_version_map` cssvm
        ON cssvm.questionnaire_response_id = sc.survey_conduct_id
        WHERE cssvm.cope_month = '{{cope_month}}'
        {% else %}
        -- prevent getting the entire table if we can't tell which criteria to use --
        limit 1
        {% endif %}
        """).render(project=project,
                    dataset=dataset,
                    survey_concept_id=survey_concept_id,
                    cope_month=cope_month)

        sc_view = client.create_table(sc_view)
        LOGGER.info(f'Created {sc_view.table_type}: {str(sc_view.reference)}')


def main(raw_args=None):
    """
    Run a full RDR import.

    Assumes you are passing arguments either via command line or a
    list.
    """
    args = parse_rdr_args(raw_args)

    pipeline_logging.configure(level=logging.INFO,
                               add_console_handler=args.console_log)

    description = f'Test setup of differentiating PPI surveys via views'
    new_dataset_name = f'{args.rdr_dataset_id}_ppi_views'

    # get credentials and create client
    impersonation_creds = auth.get_impersonation_credentials(
        args.run_as_email, CDR_SCOPES)

    bq_client = BigQueryClient(args.project_id, credentials=impersonation_creds)

    dataset_object = bq_client.define_dataset(new_dataset_name, description, {
        'owner': 'curation',
        'jira_issue': 'dc2790'
    })
    # set table expiration options
    dataset_object.default_table_expiration_ms = 180 * 24 * 60 * 60 * 1000  # In milliseconds.
    # bq_client.create_dataset(dataset_object)

    # bq_client.copy_dataset(f'{args.project_id}.{args.rdr_dataset_id}',
    #                        f'{args.project_id}.{new_dataset_name}')
    # bq_client.copy_dataset(f'{args.project_id}.{args.vocabulary}',
    #                        f'{args.project_id}.{new_dataset_name}')
    create_ppi_views(bq_client, args.project_id, new_dataset_name)
    LOGGER.info("PPI Survey View creations completed.")


if __name__ == '__main__':
    main()
