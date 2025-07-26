# Databricks notebook source
# MAGIC %run ./config_to_read_outside_DLT

# COMMAND ----------

process_table_name='process_status'
if env=="dev":
  raw_folder_path = raw_folder_path.replace("DS_HANA_CDD", "Ingestion_new/process_status")
elif env=="tst":
  raw_folder_path = raw_folder_path.replace("DS_HANA_CDD", "Ingestion_new/process_status")
elif env=="pre":
  raw_folder_path = raw_folder_path.replace("DS_HANA_ZDD", "Ingestion_new/process_status")
elif env=="prd":
  raw_folder_path = raw_folder_path.replace("DS_HANA_PDD", "Ingestion_new/process_status")
else:
  print("please provide the environment variable") 

print("raw_folder_path",raw_folder_path)

tbl_owner_grp=tbl_owner_grp
tbl_read_grp =tbl_read_grp 

process_config_project_name = process_config_project_name
process_order = process_order
process_name = process_name
spnvalue = spnvalue
host=workspace_url 

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

df_schema_table = spark.sql(f"SHOW TABLES in `{uc_catalog_name}`.`{uc_raw_schema}`").select("tableName")
table_list = [row[0] for row in df_schema_table.select('tableName').collect()]
if process_table_name in table_list:
    print("process_status table already present in the schema")
    assignPermission(uc_catalog_name,uc_raw_schema,process_table_name,tbl_owner_grp,tbl_read_grp )
else: 
    spark.sql(f"CREATE TABLE IF NOT EXISTS `{uc_catalog_name}`.`{uc_raw_schema}`.{process_table_name}( run_id BIGINT,  project_name STRING,  process_order INT,  process_name STRING,  dlt_workflow_name STRING, dlt_workflow_job_id BIGINT, run_start_date TIMESTAMP,  run_update_date TIMESTAMP, status STRING) USING delta PARTITIONED BY (dlt_workflow_name) LOCATION '{raw_folder_path}'")    
    assignPermission(uc_catalog_name,uc_raw_schema,process_table_name,tbl_owner_grp,tbl_read_grp )


# COMMAND ----------

table_name='`'+uc_catalog_name+'`.`'+uc_raw_schema+'`'+'.'+process_table_name

http_header = get_dbx_http_header(spnvalue)
dlt_workflow_job_id=get_job_id(job_name, host, http_header)

# COMMAND ----------

from datta_pipeline_library.core.process_status import insert_into_table
insert_into_table(table_name, process_config_project_name, process_order, process_name,dlt_workflow_name,dlt_workflow_job_id)
