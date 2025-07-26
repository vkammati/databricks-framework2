# Databricks notebook source
# MAGIC %run ./config_to_read_outside_DLT

# COMMAND ----------

def get_latest_euh_workflow_status(dlt_workflow_name, catalog, schema):
    df_max_run_id_status = spark.sql(f"""select status from `{catalog}`.`{schema}`.process_status where run_id in (select max(run_id) as max_run_id from `{catalog}`.`{schema}`.process_status where dlt_workflow_name = '{dlt_workflow_name}') and dlt_workflow_name = '{dlt_workflow_name}'""")
                            
    return df_max_run_id_status

# COMMAND ----------

df_md_eh = get_latest_euh_workflow_status("sede-x-DATTA-MD-EH-workflow", uc_catalog_name, uc_raw_schema)
df_fi_eh = get_latest_euh_workflow_status("sede-x-DATTA-FI-EH-workflow", uc_catalog_name, uc_raw_schema)

if df_md_eh.isEmpty():
    df_md_eh_filter = df_md_eh
else:
    df_md_eh_filter = df_md_eh.filter(df_md_eh.status == "completed")

if df_fi_eh.isEmpty():
    df_fi_eh_filter = df_fi_eh
else:
    df_fi_eh_filter = df_fi_eh.filter(df_fi_eh.status == "completed")

df_total = df_md_eh_filter.union(df_fi_eh_filter)

# COMMAND ----------

if df_total.count() == int(2):
    print("EH layer workflows completed successfully")
else:
    raise ValueError("EH workflows did not complete yet. This workflow is dependent on EH workflow.")
