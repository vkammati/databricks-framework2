# Databricks notebook source
pip install msal

# COMMAND ----------

pip install adal

# COMMAND ----------

dbutils.library.restartPython() 

# COMMAND ----------

from datta_pipeline_library.core.base_config import (
    BaseConfig,
    CollibraConfig,
    CommonConfig,
    EnvConfig
)
from datta_pipeline_library.helpers.adls import configure_spark_to_use_spn_to_write_to_adls_gen2
from datta_pipeline_library.helpers.spn import AzureSPN
# from datta_pipeline_library.edc.collibra import fetch_business_metadata
from datta_pipeline_library.helpers.uc import (
    get_catalog_name,
    get_raw_schema_name
)

from datetime import datetime
import requests
from requests.exceptions import HTTPError
import time
from typing import Optional
import requests, json, datetime
from ci_cd_helpers.auth import get_dbx_http_header

# COMMAND ----------

env = dbutils.widgets.get(name="env")
repos_path = dbutils.widgets.get(name="repos_path")
unique_repo_branch_id = dbutils.widgets.get(name="unique_repo_branch_id")
unique_repo_branch_id_schema = dbutils.widgets.get(name="unique_repo_branch_id_schema")
workflow_name = dbutils.widgets.get(name="workflow_name")

# env = "dev"
# repos_path = "/Repos/DATTA-MVP2/DATTA-FCB-CURATED"
# unique_repo_branch_id = ""
# unique_repo_branch_id_schema = "gsap"

common_conf = CommonConfig.from_file(f"/Workspace/{repos_path.strip('/')}/conf/common/common_conf.json")
print("common_conf !!!")
env_conf = EnvConfig.from_file(f"/Workspace/{repos_path.strip('/')}/conf/{env}/conf.json")

base_config = BaseConfig.from_confs(env_conf, common_conf)
if unique_repo_branch_id:
    base_config.set_unique_id(unique_repo_branch_id)
if unique_repo_branch_id_schema:
    base_config.set_unique_id_schema(unique_repo_branch_id_schema)

# COMMAND ----------

kv = env_conf.kv_key

# values from key vault
tenant_id = dbutils.secrets.get(scope=kv, key="AZ-AS-SPN-DATTA-TENANT-ID")
spn_client_id = dbutils.secrets.get(scope=kv, key=env_conf.spn_client_id_key)
spn_client_secret = dbutils.secrets.get(scope=kv, key=env_conf.spn_client_secret_key)
edc_user_id = dbutils.secrets.get(scope=kv, key=env_conf.edc_user_id_key)
edc_user_pwd = dbutils.secrets.get(scope=kv, key=env_conf.edc_user_pwd_key)

spn = AzureSPN(tenant_id, spn_client_id, spn_client_secret)
configure_spark_to_use_spn_to_write_to_adls_gen2(env_conf.storage_account, spn)

# COMMAND ----------

spnvalue = spn.generate_ad_token()
print(spnvalue)

# COMMAND ----------

uc_catalog_name = base_config.get_uc_catalog_name()
print("uc_catalog_name : ",uc_catalog_name)
uc_raw_schema = base_config.get_uc_raw_schema()
print("uc_raw_schema : ",uc_raw_schema)

raw_folder_path = base_config.get_raw_folder_path()
print("raw_folder_path : ",raw_folder_path)

tbl_owner_grp = base_config.get_tbl_owner_grp()
print("tbl_owner_grp : ",tbl_owner_grp)
tbl_read_grp = base_config.get_tbl_read_grp()
print("tbl_read_grp : ",tbl_owner_grp)

process_config_project_name = common_conf.process_config_project_name
print("process_config_project_name: ", process_config_project_name)
process_order = common_conf.process_order
print("process_order: ", process_order)
process_name = common_conf.process_name
print("process_name: ", process_name)
dlt_workflow_name = common_conf.dlt_workflow_name
print("dlt_workflow_name: ", dlt_workflow_name)
mars_sp_load_dlt_workflow_name = common_conf.mars_sp_load_dlt_workflow_name
print("mars_sp_load_dlt_workflow_name: ", mars_sp_load_dlt_workflow_name)
mars_load_dlt_workflow_name = common_conf.mars_load_dlt_workflow_name
print("mars_load_dlt_workflow_name: ", mars_load_dlt_workflow_name)

workspace_url = env_conf.workspace_url
print("workspace_url: ", workspace_url)

# COMMAND ----------

''' assignPermission This function assigns Permission to all the tables created '''
def assignPermission(catalog,schema,table_name,tbl_owner,tbl_read):
    spark.sql(f"ALTER table `{catalog}`.`{schema}`.{table_name} owner to `{tbl_owner}`")
    print("Table Owner is assigned")
    spark.sql(f"GRANT ALL PRIVILEGES ON TABLE `{catalog}`.`{schema}`.{table_name} TO `{tbl_owner}`")
    print("All privileges access given to tbl owner")
    spark.sql(f"GRANT SELECT ON TABLE `{catalog}`.`{schema}`.{table_name} TO `{tbl_read}`")
    print("Reader access granted")

# COMMAND ----------

def get_job_id(job_name: str, host: str, http_header: dict) -> Optional[str]:
    """Get job id if it exists, None otherwise.
    
    :param job_name: Databricks job name
    :param host: Databricks host
    :param http_header: HTTP header used for the Databricks REST API
    :return: job id if it exists, None otherwise
    """
    job_id = None
    limit = 1
    expand_tasks = "false"

    query_string = f"name={job_name}&limit={limit}&expand_tasks={expand_tasks}"

    req = requests.get(
        f"{host}/api/2.1/jobs/list?{query_string}",
        headers=http_header,
    )

    try:
        req.raise_for_status()
    except HTTPError as e:
        print(e.response.text)
        raise e

    jobs_key = "jobs"

    if jobs_key in req.json():
        job_id = req.json()[jobs_key][0]["job_id"]
        print("job_id",job_id)

    return job_id
