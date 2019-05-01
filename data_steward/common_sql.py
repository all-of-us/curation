# This File consists of all the SQL Queries across the modules

# Used in validation/main/get_heel_result()
HEEL_ERROR_QUERY_VALIDATION = '''
     SELECT analysis_id AS Analysis_ID,
      achilles_heel_warning AS Heel_Error,
      rule_id AS Rule_ID,
      record_count AS Record_Count
     FROM `{application}.{dataset}.{table_id}`
     WHERE achilles_heel_warning LIKE 'ERROR:%'
     ORDER BY record_count DESC, analysis_id
    '''

