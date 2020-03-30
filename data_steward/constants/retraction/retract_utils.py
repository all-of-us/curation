TABLE_ID = 'table_id'
PERSON_ID = 'person_id'
RESEARCH_ID = 'research_id'

PID_QUERY = """
(SELECT {pid}
FROM `{pid_source}`)
"""
