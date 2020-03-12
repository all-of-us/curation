import collections
import re

from google.cloud import bigquery

client = bigquery.Client()

VOCABULARY_DATASET_RE = re.compile(r'^vocabulary\d{8}$')
RDR_DATASET_RE = re.compile(r'^rdr\d{8}$')
UNIONED_DATASET_RE = re.compile(r'^unioned_ehr\d{8}$')
COMBINED_DATASET_RE = re.compile(r'^combined\d{8}(?:v\d+)$')
DEID_DATASET_RE = re.compile(r'^combined\d{8}.*_deid(?:_clean)$')
TREND_N = 3


def is_vocabulary_dataset(dataset_id):
    return re.match(VOCABULARY_DATASET_RE, dataset_id) is not None


def is_rdr_dataset(dataset_id):
    return re.match(RDR_DATASET_RE, dataset_id) is not None


def is_unioned_dataset(dataset_id):
    return re.match(UNIONED_DATASET_RE, dataset_id) is not None


def is_combined_dataset(dataset_id):
    return re.match(COMBINED_DATASET_RE, dataset_id) is not None


def is_deid_dataset(dataset_id):
    return re.match(DEID_DATASET_RE, dataset_id) is not None


def _datasets():
    DefaultDatasets = collections.namedtuple('DefaultDatasets', 'latest trend')
    dataset_list = list(client.list_datasets())
    dataset_list.sort(key=lambda d: d.dataset_id, reverse=True)
    vocabulary = []
    rdr = []
    unioned = []
    combined = []
    deid = []

    for dataset in dataset_list:
        dataset_id = dataset.dataset_id
        if is_vocabulary_dataset(dataset_id):
            vocabulary.append(dataset_id)
        elif is_rdr_dataset(dataset_id):
            rdr.append(dataset_id)
        elif is_unioned_dataset(dataset_id):
            unioned.append(dataset_id)
        elif is_combined_dataset(dataset_id):
            combined.append(dataset_id)
        elif is_deid_dataset(dataset_id):
            deid.append(dataset_id)

    LatestDatasets = collections.namedtuple(
        'LatestDatasets', 'vocabulary rdr unioned combined deid')
    latest = LatestDatasets(vocabulary=vocabulary[0],
                            rdr=rdr[0],
                            unioned=unioned[0],
                            combined=combined[0],
                            deid=deid[0])

    TrendDatasets = collections.namedtuple('TrendDatasets',
                                           'rdr unioned combined deid')
    trend = TrendDatasets(rdr=rdr[0:TREND_N],
                          unioned=unioned[0:TREND_N],
                          combined=combined[0:TREND_N],
                          deid=deid[0:TREND_N])

    return DefaultDatasets(latest=latest, trend=trend)


DEFAULT_DATASETS = _datasets()
