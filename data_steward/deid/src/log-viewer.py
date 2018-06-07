"""
    This function is designed to mine the logs of a given run and determine the following for a given (dataset,table)
        - init determines what the target is
            - what the fields to be shifted are
            - what the fields to be dropped are
        - what the job-id/status is 
"""

from google.cloud import bigquery as bq
import json
import sys
import os

SYS_ARGS = {}
if len(sys.argv) > 1:
	
	N = len(sys.argv)
	for i in range(1,N):
		value = None
		if sys.argv[i].startswith('--'):
			key = sys.argv[i].replace('-','')
			SYS_ARGS[key] = 1
			if i + 1 < N and sys.argv[i+1].startswith('--') is False:
				value = sys.argv[i + 1] = sys.argv[i+1].strip()
			if key and value:
				SYS_ARGS[key] = value
		
		i += 2


class ViewLogs :
    def __init__(self):
        self.path = SYS_ARGS['log']
        f = open(self.path)
        for line in f :
            print line
            break



ViewLogs()