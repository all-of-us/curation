import argparse
from time import time

from common import CDR_SCOPES

from utils import auth

from gcloud.bq import BigQueryClient
from app_identity import get_application_id


from common import JINJA_ENV





def parse_deid_args(args=None):
    parser = argparse.ArgumentParser(
        description='Parse deid command line arguments')
    parser.add_argument('-p',
                        '--project_id',
                        action='store',
                        dest='project_id',
                        help=('Project associated with the '
                              'input dataset.'),
                        required=True)
    parser.add_argument('-c',
                        '--credentials_filepath',
                        dest='credentials_filepath',
                        action='store',
                        default='',
                        help='file path to credentials for GCP to access BQ',
                        required=False)
    parser.add_argument('--run_as',
                        dest='target_principal',
                        action='store',
                        help='Email address of service account to impersonate.',
                        required=True)
    parser.add_argument('-i',
                        '--input_dataset',
                        action='store',
                        dest='in_dataset',
                        help='Name of the input dataset',
                        required=True)
    parser.add_argument('-l',
                        '--console_log',
                        dest='console_log',
                        action='store_true',
                        required=False,
                        help='Log to the console as well as to a file.')

    common_args, _ = parser.parse_known_args(args)
    return common_args


def main(project_id, dataset, table: list, interval: int, unit: str):
    # Parses the required arguments and keyword arguments required by cleaning rules
    # args, kwargs = parse_deid_args(raw_args)

    #? Stage
    project_id = get_application_id()
    client = BigQueryClient(project_id=project_id)

    #? create dataset
    dataset = client.create_dataset('aou-res-curation-test.foo',
                                    exists_ok=True)
    client.create_table('bar')
    time.sleep(200)
    delete_meta = client.delete_table()


    tpl = JINJA_ENV.from_string = """DELETE FROM
    SELECT *
        FROM `{{project_id}}.{{dataset}}.{{table}}`
            FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);
    """)

    query = tpl.render(project_id=project_id, dataset=dataset, table=table)






    # print(len(datasets))

    # for dataset in datasets:
    #     if dataset.dataset_id.count('foo'):
    #         label = f'{project_id}.{dataset.dataset_id}'
    #         label = 'aou-res-curation-test.all_of_us_michaelschmidt82_ms_dc_2037_ehr'
    #         client.delete_dataset(label,
    #                               delete_contents=True,
    #                               not_found_ok=True)

    return True


if __name__ == "__main__":
    main()
