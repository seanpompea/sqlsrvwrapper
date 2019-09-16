from __future__ import division
from __future__ import print_function
import time
import pymssql

__all__ = [
  'db_qy',
  'db_test_conn',
  'db_curr_rowcount',
  'db_stmt',
  'db_trunc_table',
  'db_insert_many',
  'db_start_job',
  'db_is_job_idle',
  'db_last_run_succeeded',
  'db_run_agent_job',
  'db_columns_for',
  ]

def db_qy(db_spec, qy, tuple=None):
  '''Runs a SQL query. If tuple is not None, then for each
  format specifier included in qy, there should be a
  corresponding value in the tuple. Returns list of maps.'''
  try:
    with pymssql.connect(**db_spec) as conn:
      cursor = conn.cursor(as_dict=True)
      if tuple != None:
        cursor.execute(qy, tuple)
      else:
        cursor.execute(qy)
      return cursor.fetchall()
  except Exception, ex:
    raise ex

def db_test_conn(db_spec):
  qy = 'select 1 as test'
  return db_qy(db_spec, qy)

def db_curr_rowcount(db_spec, table_name):
  '''Returns int.'''
  qy = 'select count(*) as count from ' + table_name 
  rslt = db_qy(db_spec, qy)
  return rslt[0]['count']

def db_stmt(db_spec, stmt):
  '''Execute a SQL DDL/DML statement. Doesn't return anything. Throws.'''
  try:
    with pymssql.connect(**db_spec) as conn:
      cursor = conn.cursor()
      cursor.execute(stmt)
      conn.commit()
  except Exception, e:
    raise e

def db_trunc_table(db_spec, table_name):
  stmt = 'truncate table ' + table_name
  db_stmt(db_spec, stmt) 

def db_executemany(db_spec, stmt, tuples):
  '''Execute a parameterized statement for a collection of rows.
  tuples should be a sequence of tuples (not maps). Throws.'''
  try:
    with pymssql.connect(**db_spec) as conn:
      cursor = conn.cursor()
      cursor.executemany(stmt, tuples)
      conn.commit()
  except Exception, e:
    raise e

def parameterized_insert_stmt(table_name, data):
  '''data should be a sequence of maps.
  See: http://pymssql.org/en/stable/pymssql_examples.html'''
  stmt = u''
  try:
    stmt = ( u''
           + 'insert into ' + table_name
           + ' ([' 
           + '],['.join(data[0]) # All rows should have same keys.
           + ']) values ('
           + ','.join('%s' for _ in data[0])
           + ')')
  except Exception, ex:
    raise ex
  return stmt

def db_insert_many(db_spec, table_name, data):
  '''Takes a seq of maps. Doesn't return anything. Throws.'''
  try:
    stmt = parameterized_insert_stmt(table_name, data) 
    tuples = map(lambda mp: tuple([mp[k] for k in mp]), data)
    db_executemany(db_spec, stmt, tuples)
  except Exception, ex:
    raise ex

def db_start_job(db_spec, job_name):
  '''Start a SQL Server Agent job. Returns immediately.'''
  try:
    with pymssql.connect(**db_spec) as conn:
      cursor = conn.cursor()
      cursor.callproc('msdb.dbo.sp_start_job', (job_name,))
      conn.commit()
  except Exception, e:
    raise e

def db_is_job_idle(db_spec, job_name):
  '''job_name should be a SQL Server Agent job name. Returns boolean.'''
  result = []
  try:
    with pymssql.connect(**db_spec) as conn:
      cursor = conn.cursor(as_dict=True)
      stmt = "exec msdb.dbo.sp_help_job @job_name=N'" + job_name + "'"
      cursor.execute(stmt) 
      result = cursor.fetchall()
  except Exception, e:
    raise e
  return result[0]['current_execution_status'] == 4 # 4 means idle.

def db_last_run_succeeded(db_spec, job_name):
  '''job_name should be a SQL Server Agent job name. Returns boolean.'''
  result = []
  try:
    with pymssql.connect(**db_spec) as conn:
      cursor = conn.cursor(as_dict=True)
      stmt = "exec msdb.dbo.sp_help_job @job_name=N'" + job_name + "'"
      cursor.execute(stmt) 
      result = cursor.fetchall()
  except Exception, e:
    raise e
  return result[0]['last_run_outcome'] == 1 # 1 means succeeded.

def db_run_agent_job(db_spec, job_name, timeout_threshold=60):
  '''Run an agent job in a synchronous fashion -- that is, this
  function will not return until the job has completed/failed OR the
  timeout_threshold amount of seconds has passed (will raise Exception
  in the latter case). 
  Note: the agent job might/will continue running even if 
  timeout_threshold is passed.'''
  db_start_job(db_spec, job_name)
  # Below we poll to see if the job has finished; when finished we check if
  # it was successful. If the job is still running past the designated
  # threshold, then we raise an exception, with the assumption that something
  # is wrong.
  interval = 10 # Check every 10 seconds.
  havewaited = 0 # How long have we been waiting so far?
  while True:
    time.sleep(interval)
    if not db_is_job_idle(db_spec, job_name):
      havewaited += interval
      if havewaited > timeout_threshold:
        raise Exception('Runtime for job [{}] has exceeded '\
                        'specified threshold of {} seconds.'\
                        ''.format(job_name, timeout_threshold))
      else:
        continue
    else:
      break
  if not db_last_run_succeeded(db_spec, job_name):
    raise Exception('The Agent job [{}] did not run successfully.'\
                    ''.format(job_name))
  else:
    return True 

def db_columns_for(db_spec, table_name):
  '''Expects table_name to be fully-qualified table name with brackets.
  Example: [foo].[dbo].[bar].'''
  # Get specific db and table names by themselves; needed 
  # for subsequent query.
  just_db = table_name[1:table_name.find(']')]
  just_table = table_name[table_name.rfind('[')+1:-1]
  result = db_qy("select column_name from " + just_db 
                + ".information_schema.columns" 
                + " where table_name = N'" + just_table +"'")
  cols = [x['column_name'] for x in result]
  return cols

