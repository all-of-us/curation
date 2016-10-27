#!/usr/bin/env python

import cchardet
import codecs
import datetime
from heapq import nlargest
from operator import itemgetter
import math
import six
from csvkit import table
import re
from StringIO import StringIO

NoneType = type(None)

MAX_UNIQUE = 5
MAX_FREQ = 5
OPERATIONS = ('min', 'max', 'sum', 'mean', 'median', 'stdev', 'nulls', 'unique', 'freq', 'len')
RESULT_SUCCESS = 'success'
SPRINT_RE = re.compile('.*?(\w+)_(\w+)_DataSprint_(\d+).*')
PERSON_COLUMNS = []


class CsvInfo:
    def __init__(self, input_file, sprint_num, hpo_id, table_name):

        self.sprint_num = sprint_num
        self.hpo_id = hpo_id
        self.table_name = table_name
        self.columns = []

        # strip byte order mark if present
        buf = input_file.read()
        if buf.startswith(codecs.BOM_UTF8):
            buf = buf[len(codecs.BOM_UTF8):]

        # force utf-8 encoding
        detect_result = cchardet.detect(buf)
        encoding = detect_result['encoding']
        if 'utf-8' != encoding.lower():
            buf = buf.decode(encoding, buf).encode('utf-8')
        input_file = StringIO(buf)

        tab = table.Table.from_csv(input_file)

        if not input_file.closed:
            input_file.close()

        for c in tab:
            column = dict()
            values = sorted(filter(lambda i: i is not None, c))

            stats = {}

            for op in OPERATIONS:
                stats[op] = getattr(self, 'get_%s' % op)(c, values, stats)

            column['name'] = c.name
            column['type'] = c.type.__name__
            column['stats'] = stats
            self.columns.append(column)

    def get_min(self, c, values, stats):
        if c.type == NoneType:
            return None

        v = min(values)

        if v in [datetime.datetime, datetime.date, datetime.time]:
            return v.isoformat()

        return v

    def get_max(self, c, values, stats):
        if c.type == NoneType:
            return None

        v = max(values)

        if v in [datetime.datetime, datetime.date, datetime.time]:
            return v.isoformat()

        return v

    def get_sum(self, c, values, stats):
        if c.type not in [int, float]:
            return None

        return sum(values)

    def get_mean(self, c, values, stats):
        if c.type not in [int, float]:
            return None

        if 'sum' not in stats:
            stats['sum'] = self.get_sum(c, values, stats)

        return float(stats['sum']) / len(values)

    def get_median(self, c, values, stats):
        if c.type not in [int, float]:
            return None

        return median(values)

    def get_stdev(self, c, values, stats):
        if c.type not in [int, float]:
            return None

        if 'mean' not in stats:
            stats['mean'] = self.get_mean(c, values, stats)

        return math.sqrt(sum(math.pow(v - stats['mean'], 2) for v in values) / len(values))

    def get_nulls(self, c, values, stats):
        return c.has_nulls()

    def get_unique(self, c, values, stats):
        return set(values)

    def get_freq(self, c, values, stats):
        return freq(values)

    def get_len(self, c, values, stats):
        if c.type != six.text_type:
            return None

        return c.max_length()


def median(l):
    """
    Compute the median of a list.
    """
    length = len(l)

    if length % 2 == 1:
        return l[(length + 1) // 2 - 1]
    else:
        a = l[(length // 2) - 1]
        b = l[length // 2]
    return (float(a + b)) / 2


def freq(l, n=MAX_FREQ):
    """
    Count the number of times each value occurs in a column.
    """
    count = {}

    for x in l:
        s = six.text_type(x)

        if s in count:
            count[s] += 1
        else:
            count[s] = 1

    # This will iterate through dictionary, return N highest
    # values as (key, value) tuples.
    top = nlargest(n, six.iteritems(count), itemgetter(1))

    return top
