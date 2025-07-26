# Databricks notebook source
# MAGIC %run ./config_to_read_outside_DLT

# COMMAND ----------

host=workspace_url 
spnvalue = spnvalue

if workflow_name == 'MARS-SP':
  job_name= mars_sp_load_dlt_workflow_name
  dlt_workflow_name = mars_sp_load_dlt_workflow_name
elif workflow_name == 'MARS':
  job_name= mars_load_dlt_workflow_name
  dlt_workflow_name = mars_load_dlt_workflow_name
else:
  job_name= dlt_workflow_name
  dlt_workflow_name = dlt_workflow_name

# COMMAND ----------

process_table_name='process_status'
table_name='`'+uc_catalog_name+'`.`'+uc_raw_schema+'`'+'.'+process_table_name

http_header = get_dbx_http_header(spnvalue)
dlt_workflow_job_id=get_job_id(job_name, host, http_header)

# COMMAND ----------

from datta_pipeline_library.core.process_status import update_table
update_table(table_name,dlt_workflow_name,dlt_workflow_job_id)
