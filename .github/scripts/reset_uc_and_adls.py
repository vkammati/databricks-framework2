# Databricks notebook source
# MAGIC %pip install --upgrade pip

# COMMAND ----------

# MAGIC %pip install -r ../../requirements.txt

# COMMAND ----------

import os
os.environ["pipeline"] = "databricks"

# COMMAND ----------

from datta_pipeline_library.core.base_config import (
    BaseConfig,
    CommonConfig,
    EnvConfig,
)

from datta_pipeline_library.helpers.adls import configure_spark_to_use_spn_to_write_to_adls_gen2
from datta_pipeline_library.helpers.spn import AzureSPN

# COMMAND ----------

# dbutils.widgets.text("env", "dev")
# dbutils.widgets.text("dlt_pipeline_id", "aaaaaaaaaaaa12345")
# dbutils.widgets.text("repos_path", "/Repos/sede-x-EDP-Newton-CatalystMI-ADB/feature-separateRegisterUCTask-manual")
# dbutils.widgets.text("unique_repo_branch_id", "sede_x_EDP_Newton_CatalystMI_ADB_feature_separateRegisterUCTask_manual")

# COMMAND ----------

# DBTITLE 1,Parameters
unique_repo_branch_id = dbutils.widgets.get(name="unique_repo_branch_id")
unique_repo_branch_id_schema = dbutils.widgets.get(name="unique_repo_branch_id")
repos_path = dbutils.widgets.get(name="repos_path")
dlt_pipeline_id = dbutils.widgets.get(name="dlt_pipeline_id")
env = dbutils.widgets.get(name="env")
print("unique_repo_branch_id : ", unique_repo_branch_id)
print("unique_repo_branch_id_schema : ", unique_repo_branch_id_schema)
print("repos_path : ", repos_path)
print("dlt_pipeline_id : ", dlt_pipeline_id)
print("env : ", env)


common_conf = CommonConfig.from_file("../../conf/common/common_conf.json")
env_conf = EnvConfig.from_file(f"../../conf/{env}/conf.json")

kv = env_conf.kv_key

# values from key vault
tenant_id = dbutils.secrets.get(scope=kv, key="AZ-AS-SPN-DATTA-TENANT-ID")
spn_client_id = dbutils.secrets.get(scope=kv, key=env_conf.spn_client_id_key)
spn_client_secret = dbutils.secrets.get(scope=kv, key=env_conf.spn_client_secret_key)

spn = AzureSPN(tenant_id, spn_client_id, spn_client_secret)


base_config = BaseConfig.from_confs(env_conf, common_conf)
if unique_repo_branch_id:
    base_config.set_unique_id(unique_repo_branch_id)
if unique_repo_branch_id_schema:
    base_config.set_unique_id_schema(unique_repo_branch_id_schema)

# COMMAND ----------

configure_spark_to_use_spn_to_write_to_adls_gen2(env_conf.storage_account, spn)

# COMMAND ----------

# DBTITLE 1,Configuration
uc_catalog = base_config.get_uc_catalog_name()
print("uc_catalog : ", uc_catalog)

eh_schema = base_config.get_uc_eh_schema()
print("eh_schema : ", eh_schema)
curated_schema = base_config.get_uc_curated_schema()
print("curated_schema : ", curated_schema)

eh_folder_path = base_config.get_eh_folder_path()
print("eh_folder_path : ", eh_folder_path)
curated_folder_path = base_config.get_curated_folder_path()
print("curated_folder_path : ", curated_folder_path)

eh_fi_schema = eh_schema
eh_md_schema = eh_schema.replace("-finance-", "-masterdata-")

eh_fi_folder_path=eh_folder_path
eh_md_folder_path=eh_folder_path.replace("/finance_dev", "/masterdata_dev")

print("eh fi schema: ", eh_fi_schema)
print("eh fi folder path: ", eh_fi_folder_path)
print("eh schema: ", eh_md_schema)
print("eh folder path: ", eh_md_folder_path)

# COMMAND ----------

# MAGIC %md ## Drop schemas and tables

# COMMAND ----------

def drop_all_tables_in_schema(uc_catalog_name,uc_schema_name):
    print("")
    print("uc_catalog_name : ",uc_catalog_name)
    print("uc_schema_name : ",uc_schema_name)
    df_schema_table = spark.sql(f"SHOW TABLES in `{uc_catalog_name}`.`{uc_schema_name}`").select("tableName")
    table_list = [row[0] for row in df_schema_table.select('tableName').collect()]
    if not table_list  :
        print(uc_schema_name , " || The schema is empty , NO Tables to delete ")
    else :
        for row in table_list:
            table_name = row
            print(f"Dropping table `{uc_catalog_name}`.`{uc_schema_name}`.{table_name}")
            spark.sql(f"DROP TABLE IF EXISTS `{uc_catalog_name}`.`{uc_schema_name}`.{table_name}")

# COMMAND ----------

if env == "dev":
     print(f"Dropping all tables :  `{uc_catalog}`.`{eh_fi_schema}`")
     drop_all_tables_in_schema(uc_catalog, eh_fi_schema)

# COMMAND ----------

if env == "dev":
    print(f"Dropping schema AND all tables :  `{uc_catalog}`.`{eh_fi_schema}`")
    spark.sql(f"DROP SCHEMA IF EXISTS `{uc_catalog}`.`{eh_fi_schema}` CASCADE")

# COMMAND ----------

if env == "dev":
     print(f"Dropping all tables :  `{uc_catalog}`.`{eh_md_schema}`")
     drop_all_tables_in_schema(uc_catalog, eh_md_schema)

# COMMAND ----------

if env == "dev":
    print(f"Dropping schema AND all tables :  `{uc_catalog}`.`{eh_md_schema}`")
    spark.sql(f"DROP SCHEMA IF EXISTS `{uc_catalog}`.`{eh_md_schema}` CASCADE")

# COMMAND ----------

if env == "dev":
     print(f"Dropping all tables :  `{uc_catalog}`.`{curated_schema}`")
     drop_all_tables_in_schema(uc_catalog, curated_schema)

# COMMAND ----------

if env == "dev":
    print(f"Dropping schema AND all tables :  `{uc_catalog}`.`{curated_schema}`")
    spark.sql(f"DROP SCHEMA IF EXISTS `{uc_catalog}`.`{curated_schema}` CASCADE")

# COMMAND ----------

# MAGIC %md ## Delete ADLS folders

# COMMAND ----------

# DBTITLE 1,Delete EH folder
if env == "dev":
    dbutils.fs.rm(eh_fi_folder_path, recurse=True)

# COMMAND ----------

if env == "dev":
    dbutils.fs.rm(eh_md_folder_path, recurse=True)

# COMMAND ----------

# DBTITLE 1,Delete curated folder
if env == "dev":
    dbutils.fs.rm(curated_folder_path, recurse=True)
